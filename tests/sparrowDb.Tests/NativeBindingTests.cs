using System;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using SparrowDb;
using SparrowDb.Native;
using Xunit;

namespace SparrowDb.Tests;

public class NativeBindingTests
{
    [Fact]
    public void ExecuteQuery_SimpleSelect_ReturnsCorrectValue()
    {
        using var db = new Database();
        using var result = db.ExecuteQuery("SELECT 42 AS val, 'hello' AS str;");

        Assert.Equal(1, result.RowCount);
        Assert.Equal(2, result.ColumnCount);
        Assert.Equal("val", result.GetColumnName(0));
        Assert.Equal("str", result.GetColumnName(1));
        Assert.Equal(42, result.GetValue<int>(0, 0));
        Assert.Equal("hello", result.GetString(0, 1));
    }

    [Fact]
    public unsafe void TestDuckDbArrowScanEndToEnd()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        // 1. Create a schema and batch
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default))
            .Field(f => f.Name("name").DataType(Apache.Arrow.Types.StringType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < 5; i++)
        {
            idBuilder.Append(i * 10);
            nameBuilder.Append($"item_{i}");
        }

        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), nameBuilder.Build() }, 5);

        // Serialize to IPC stream bytes
        using var ms = new System.IO.MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, schema))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        byte[] ipcBytes = ms.ToArray();

        // 2. Read back with ArrowStreamReader via zero-copy stream
        using var readMs = new System.IO.MemoryStream(ipcBytes, 0, ipcBytes.Length, writable: false);
        using var reader = new ArrowStreamReader(readMs);

        // 3. Export stream
        var streamHolder = SparrowDb.Arrow.NativeArrowStreamHolder.Create(reader);
        try
        {
            // Register arrow scan in DuckDB
            var state = DuckDBNative.duckdb_arrow_scan(conn.Handle, "test_arrow_view", streamHolder.StreamPtr);
            Assert.Equal(DuckDBState.Success, state);

            // Test auto-create if not exists
            conn.ExecuteQuery("CREATE TABLE IF NOT EXISTS auto_items AS SELECT * FROM test_arrow_view LIMIT 0;");
            conn.ExecuteQuery("INSERT INTO auto_items SELECT * FROM test_arrow_view;");

            using var result = conn.ExecuteQuery("SELECT id, name FROM auto_items ORDER BY id;");
            Assert.Equal(5L, result.RowCount);
            Assert.Equal(0, result.GetValue<int>(0, 0));
            Assert.Equal("item_0", result.GetString(0, 1));
            Assert.Equal(40, result.GetValue<int>(4, 0));
            Assert.Equal("item_4", result.GetString(4, 1));
        }
        finally
        {
            streamHolder.Dispose();
        }
    }
}
