using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using SparrowDb;
using SparrowDb.Arrow;
using Xunit;

namespace SparrowDb.Tests;

public class NativeArrowIpcTests
{
    [Fact]
    public void Test_NativeArrowIpc_BasicIngestion_Success()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("score").DataType(DoubleType.Default))
            .Field(f => f.Name("name").DataType(StringType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var scoreBuilder = new DoubleArray.Builder();
        var nameBuilder = new StringArray.Builder();

        for (int i = 0; i < 50; i++)
        {
            idBuilder.Append(i);
            scoreBuilder.Append(i * 1.5);
            nameBuilder.Append($"user_{i}");
        }

        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            idBuilder.Build(),
            scoreBuilder.Build(),
            nameBuilder.Build()
        }, 50);

        byte[] ipcBytes = SerializeBatch(schema, batch);

        conn.IngestArrowIpcNative("users", ipcBytes);

        using var result = conn.ExecuteQuery("SELECT count(*), sum(id), max(score) FROM users;");
        Assert.Equal(1L, result.RowCount);
        Assert.Equal(50L, result.GetValue<long>(0, 0));
        Assert.Equal(1225L, result.GetValue<long>(0, 1));
        Assert.Equal(49 * 1.5, result.GetValue<double>(0, 2), 4);
    }

    [Fact]
    public void Test_NativeArrowIpc_ExistingTable_Success()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        // Pre-create table
        conn.ExecuteQuery("CREATE TABLE pre_created_table (id INTEGER, val DOUBLE);");

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("val").DataType(DoubleType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        for (int i = 0; i < 20; i++)
        {
            idBuilder.Append(i);
            valBuilder.Append(i * 10.0);
        }

        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), valBuilder.Build() }, 20);
        byte[] ipcBytes = SerializeBatch(schema, batch);

        conn.IngestArrowIpcNative("pre_created_table", ipcBytes, autoCreateTable: false);

        using var result = conn.ExecuteQuery("SELECT count(*) FROM pre_created_table;");
        Assert.Equal(20L, result.GetValue<long>(0, 0));
    }

    [Fact]
    public void Test_NativeArrowIpc_MultipleBatchesInSingleIpcStream()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("batch_id").DataType(Int32Type.Default))
            .Field(f => f.Name("item").DataType(StringType.Default))
            .Build();

        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, schema))
        {
            for (int b = 0; b < 3; b++)
            {
                var idBuilder = new Int32Array.Builder();
                var itemBuilder = new StringArray.Builder();
                for (int i = 0; i < 100; i++)
                {
                    idBuilder.Append(b);
                    itemBuilder.Append($"batch_{b}_item_{i}");
                }
                var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), itemBuilder.Build() }, 100);
                writer.WriteRecordBatch(batch);
            }
            writer.WriteEnd();
        }

        byte[] ipcBytes = ms.ToArray();

        conn.IngestArrowIpcNative("multi_batch_table", ipcBytes);

        using var result = conn.ExecuteQuery("SELECT count(*), count(DISTINCT batch_id) FROM multi_batch_table;");
        Assert.Equal(300L, result.GetValue<long>(0, 0));
        Assert.Equal(3L, result.GetValue<long>(0, 1));
    }

    [Fact]
    public void Test_NativeArrowIpc_SequentialChunksToSameTable()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("seq").DataType(Int32Type.Default))
            .Field(f => f.Name("payload").DataType(StringType.Default))
            .Build();

        for (int chunk = 0; chunk < 4; chunk++)
        {
            var seqBuilder = new Int32Array.Builder();
            var payloadBuilder = new StringArray.Builder();
            for (int i = 0; i < 25; i++)
            {
                seqBuilder.Append(chunk * 25 + i);
                payloadBuilder.Append($"chunk_{chunk}_{i}");
            }
            var batch = new RecordBatch(schema, new IArrowArray[] { seqBuilder.Build(), payloadBuilder.Build() }, 25);
            byte[] ipcBytes = SerializeBatch(schema, batch);

            conn.IngestArrowIpcNative("sequential_table", ipcBytes);
        }

        using var result = conn.ExecuteQuery("SELECT count(*), min(seq), max(seq) FROM sequential_table;");
        Assert.Equal(100L, result.GetValue<long>(0, 0));
        Assert.Equal(0, result.GetValue<int>(0, 1));
        Assert.Equal(99, result.GetValue<int>(0, 2));
    }

    [Fact]
    public void Test_NativeArrowIpc_Strings_Normal_Empty_Null_Unicode()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("txt").DataType(StringType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var txtBuilder = new StringArray.Builder();

        idBuilder.Append(1); txtBuilder.Append("Regular ASCII string");
        idBuilder.Append(2); txtBuilder.Append(""); // Empty string
        idBuilder.Append(3); txtBuilder.AppendNull(); // Null string
        idBuilder.Append(4); txtBuilder.Append("🚀 SparrowDB Unicode: こんにちは世界, Übergröße, £500");

        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), txtBuilder.Build() }, 4);
        byte[] ipcBytes = SerializeBatch(schema, batch);

        conn.IngestArrowIpcNative("string_test", ipcBytes);

        using var result = conn.ExecuteQuery("SELECT id, txt FROM string_test ORDER BY id;");
        Assert.Equal(4L, result.RowCount);

        Assert.Equal("Regular ASCII string", result.GetString(0, 1));
        Assert.Equal("", result.GetString(1, 1));
        Assert.True(result.IsNull(2, 1));
        Assert.Equal("🚀 SparrowDB Unicode: こんにちは世界, Übergröße, £500", result.GetString(3, 1));
    }

    [Fact]
    public void Test_NativeArrowIpc_Numerics_AllTypes_Nulls_Negatives()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("int_val").DataType(Int32Type.Default))
            .Field(f => f.Name("long_val").DataType(Int64Type.Default))
            .Field(f => f.Name("float_val").DataType(FloatType.Default))
            .Field(f => f.Name("double_val").DataType(DoubleType.Default))
            .Build();

        var i32Builder = new Int32Array.Builder();
        var i64Builder = new Int64Array.Builder();
        var f32Builder = new FloatArray.Builder();
        var f64Builder = new DoubleArray.Builder();

        // Row 1: normal positives
        i32Builder.Append(100);
        i64Builder.Append(1_000_000_000_000L);
        f32Builder.Append(3.14f);
        f64Builder.Append(2.718281828);

        // Row 2: negatives and extremes
        i32Builder.Append(-2147483648);
        i64Builder.Append(-9223372036854775807L);
        f32Builder.Append(-0.001f);
        f64Builder.Append(-1e15);

        // Row 3: nulls
        i32Builder.AppendNull();
        i64Builder.AppendNull();
        f32Builder.AppendNull();
        f64Builder.AppendNull();

        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            i32Builder.Build(),
            i64Builder.Build(),
            f32Builder.Build(),
            f64Builder.Build()
        }, 3);

        byte[] ipcBytes = SerializeBatch(schema, batch);

        conn.IngestArrowIpcNative("numerics_test", ipcBytes);

        using var result = conn.ExecuteQuery("SELECT int_val, long_val, float_val, double_val FROM numerics_test ORDER BY int_val NULLS LAST;");
        Assert.Equal(3L, result.RowCount);

        // Row 1 (negative)
        Assert.Equal(-2147483648, result.GetValue<int>(0, 0));
        Assert.Equal(-9223372036854775807L, result.GetValue<long>(0, 1));

        // Row 2 (positive)
        Assert.Equal(100, result.GetValue<int>(1, 0));
        Assert.Equal(1_000_000_000_000L, result.GetValue<long>(1, 1));

        // Row 3 (nulls)
        Assert.True(result.IsNull(2, 0));
        Assert.True(result.IsNull(2, 1));
        Assert.True(result.IsNull(2, 2));
        Assert.True(result.IsNull(2, 3));
    }

    [Fact]
    public void Test_NativeArrowIpc_Booleans_True_False_Null()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("flag").DataType(BooleanType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var boolBuilder = new BooleanArray.Builder();

        idBuilder.Append(1); boolBuilder.Append(true);
        idBuilder.Append(2); boolBuilder.Append(false);
        idBuilder.Append(3); boolBuilder.AppendNull();

        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), boolBuilder.Build() }, 3);
        byte[] ipcBytes = SerializeBatch(schema, batch);

        conn.IngestArrowIpcNative("bool_test", ipcBytes);

        using var result = conn.ExecuteQuery("SELECT id, flag FROM bool_test ORDER BY id;");
        Assert.Equal(3L, result.RowCount);

        Assert.True(result.GetValue<bool>(0, 1));
        Assert.False(result.GetValue<bool>(1, 1));
        Assert.True(result.IsNull(2, 1));
    }

    [Fact]
    public void Test_NativeArrowIpc_DatesAndTimestamps()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("date_val").DataType(Date32Type.Default))
            .Field(f => f.Name("ts_val").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var dateBuilder = new Date32Array.Builder();
        var tsBuilder = new TimestampArray.Builder();

        var now = DateTimeOffset.UtcNow;
        idBuilder.Append(1);
        dateBuilder.Append(new DateTime(2026, 10, 5));
        tsBuilder.Append(now);

        idBuilder.Append(2);
        dateBuilder.AppendNull();
        tsBuilder.AppendNull();

        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            idBuilder.Build(),
            dateBuilder.Build(),
            tsBuilder.Build()
        }, 2);

        byte[] ipcBytes = SerializeBatch(schema, batch);

        conn.IngestArrowIpcNative("temporal_test", ipcBytes);

        using var nullCheck = conn.ExecuteQuery("SELECT count(*) FROM temporal_test WHERE date_val IS NULL;");
        Assert.Equal(1L, nullCheck.GetValue<long>(0, 0));

        using var tsNullCheck = conn.ExecuteQuery("SELECT count(*) FROM temporal_test WHERE ts_val IS NULL;");
        Assert.Equal(1L, tsNullCheck.GetValue<long>(0, 0));

        using var notNullCheck = conn.ExecuteQuery("SELECT count(*) FROM temporal_test WHERE date_val IS NOT NULL AND ts_val IS NOT NULL;");
        Assert.Equal(1L, notNullCheck.GetValue<long>(0, 0));
    }

    [Fact]
    public void Test_NativeArrowIpc_ErrorHandling_EmptyPayload()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        Assert.Throws<ArgumentException>(() =>
        {
            conn.IngestArrowIpcNative("test", System.Array.Empty<byte>());
        });
    }

    [Fact]
    public void Test_NativeArrowIpc_ErrorHandling_CorruptPayload()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        byte[] garbage = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        Assert.Throws<InvalidOperationException>(() =>
        {
            conn.IngestArrowIpcNative("test", garbage);
        });
    }

    [Fact]
    public void Test_NativeArrowIpc_ErrorHandling_SchemaMismatch()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        conn.ExecuteQuery("CREATE TABLE mismatch_test (id INTEGER, num_col INTEGER);");

        // Payload has VARCHAR for num_col with non-numeric string
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("num_col").DataType(StringType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var strBuilder = new StringArray.Builder();
        idBuilder.Append(1);
        strBuilder.Append("non_numeric_string_value");

        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), strBuilder.Build() }, 1);
        byte[] ipcBytes = SerializeBatch(schema, batch);

        Assert.Throws<InvalidOperationException>(() =>
        {
            conn.IngestArrowIpcNative("mismatch_test", ipcBytes, autoCreateTable: false);
        });
    }

    [Fact]
    public void Test_NativeArrowIpc_LegacyVsNative_Parity()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("val").DataType(DoubleType.Default))
            .Field(f => f.Name("label").DataType(StringType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        var labelBuilder = new StringArray.Builder();

        for (int i = 0; i < 200; i++)
        {
            idBuilder.Append(i);
            valBuilder.Append(i * 3.14);
            labelBuilder.Append($"item_{i}");
        }

        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            idBuilder.Build(),
            valBuilder.Build(),
            labelBuilder.Build()
        }, 200);

        byte[] ipcBytes = SerializeBatch(schema, batch);

        // Ingest via legacy scalar path
        db.IngestArrowStream("table_legacy", schema, ipcBytes);

        // Ingest via native path
        db.IngestArrowIpcNative("table_native", ipcBytes);

        using var resLegacy = conn.ExecuteQuery("SELECT count(*), sum(id), sum(val) FROM table_legacy;");
        using var resNative = conn.ExecuteQuery("SELECT count(*), sum(id), sum(val) FROM table_native;");

        Assert.Equal(resLegacy.GetValue<long>(0, 0), resNative.GetValue<long>(0, 0));
        Assert.Equal(resLegacy.GetValue<long>(0, 1), resNative.GetValue<long>(0, 1));
        Assert.Equal(resLegacy.GetValue<double>(0, 2), resNative.GetValue<double>(0, 2), 4);
    }

    [Fact]
    public async Task Test_NativeArrowIpc_MultiConnection_Concurrency()
    {
        // One Database (:memory:), multiple concurrent worker connections
        using var db = new Database(":memory:");

        int workerCount = 8;
        int rowsPerWorker = 500;

        var schema = new Schema.Builder()
            .Field(f => f.Name("worker_id").DataType(Int32Type.Default))
            .Field(f => f.Name("seq").DataType(Int32Type.Default))
            .Field(f => f.Name("amount").DataType(DoubleType.Default))
            .Build();

        var tasks = new List<Task>();

        for (int w = 0; w < workerCount; w++)
        {
            int workerId = w;
            string tableName = $"worker_table_{workerId}";

            tasks.Add(Task.Run(() =>
            {
                // Each worker creates and owns its own DuckDB connection
                using var workerConn = db.CreateConnection();

                var idBuilder = new Int32Array.Builder();
                var seqBuilder = new Int32Array.Builder();
                var amtBuilder = new DoubleArray.Builder();

                for (int i = 0; i < rowsPerWorker; i++)
                {
                    idBuilder.Append(workerId);
                    seqBuilder.Append(i);
                    amtBuilder.Append(workerId * 1000.0 + i);
                }

                var batch = new RecordBatch(schema, new IArrowArray[]
                {
                    idBuilder.Build(),
                    seqBuilder.Build(),
                    amtBuilder.Build()
                }, rowsPerWorker);

                byte[] ipcBytes = SerializeBatch(schema, batch);

                workerConn.IngestArrowIpcNative(tableName, ipcBytes);
            }));
        }

        await Task.WhenAll(tasks);

        // Verify across all worker tables on the same in-memory database
        var mainConn = db.Connection;
        for (int w = 0; w < workerCount; w++)
        {
            string tableName = $"worker_table_{w}";
            using var res = mainConn.ExecuteQuery($"SELECT count(*), min(worker_id), max(worker_id) FROM {tableName};");
            Assert.Equal((long)rowsPerWorker, res.GetValue<long>(0, 0));
            Assert.Equal(w, res.GetValue<int>(0, 1));
            Assert.Equal(w, res.GetValue<int>(0, 2));
        }
    }

    [Fact]
    public void Test_NativeArrowIpc_PythonCompatibility_PyArrowGenerated()
    {
        // Test Python PyArrow producer compatibility if Python 3 and PyArrow are available
        string pyScript = @"
import sys
try:
    import pyarrow as pa
except ImportError:
    sys.exit(42)

schema = pa.schema([
    ('id', pa.int32()),
    ('price', pa.float64()),
    ('symbol', pa.string())
])

data = [
    pa.array([101, 102, 103], type=pa.int32()),
    pa.array([150.25, 2800.50, 420.00], type=pa.float64()),
    pa.array(['AAPL', 'GOOGL', 'MSFT'], type=pa.string())
]

batch = pa.record_batch(data, schema=schema)
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, schema) as writer:
    writer.write_batch(batch)

buf = sink.getvalue()
sys.stdout.buffer.write(buf.to_pybytes())
";

        string tempPyFile = Path.Combine(Path.GetTempPath(), $"pyarrow_test_{Guid.NewGuid():N}.py");
        File.WriteAllText(tempPyFile, pyScript);

        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "python3",
                Arguments = tempPyFile,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            using var proc = Process.Start(psi);
            if (proc == null) return;

            using var ms = new MemoryStream();
            proc.StandardOutput.BaseStream.CopyTo(ms);
            proc.WaitForExit();

            if (proc.ExitCode == 42)
            {
                // pyarrow is not installed in the python environment, skip gracefully
                return;
            }

            Assert.Equal(0, proc.ExitCode);
            byte[] pyArrowIpcBytes = ms.ToArray();
            Assert.True(pyArrowIpcBytes.Length > 0);

            using var db = new Database(":memory:");
            var conn = db.Connection;

            conn.IngestArrowIpcNative("pyarrow_stocks", pyArrowIpcBytes);

            using var result = conn.ExecuteQuery("SELECT id, price, symbol FROM pyarrow_stocks ORDER BY id;");
            Assert.Equal(3L, result.RowCount);
            Assert.Equal(101, result.GetValue<int>(0, 0));
            Assert.Equal(150.25, result.GetValue<double>(0, 1));
            Assert.Equal("AAPL", result.GetString(0, 2));
            Assert.Equal("MSFT", result.GetString(2, 2));
        }
        finally
        {
            if (File.Exists(tempPyFile)) File.Delete(tempPyFile);
        }
    }

    [Fact]
    public void Test_NativeArrowIpc_StressTest_1MM_Rows_100_Columns_60_Strings()
    {
        // 1MM rows, 100 columns (60 strings, 20 int32, 10 int64, 5 double, 5 timestamp) in 1 file
        int totalRows = 1_000_000;
        int batchSize = 100_000;
        int batchCount = totalRows / batchSize;

        // Build 100-column schema
        var schemaBuilder = new Schema.Builder();

        // 60 string columns
        for (int i = 0; i < 60; i++)
            schemaBuilder.Field(f => f.Name($"str_{i}").DataType(StringType.Default));

        // 20 int32 columns
        for (int i = 0; i < 20; i++)
            schemaBuilder.Field(f => f.Name($"int_{i}").DataType(Int32Type.Default));

        // 10 int64 columns
        for (int i = 0; i < 10; i++)
            schemaBuilder.Field(f => f.Name($"long_{i}").DataType(Int64Type.Default));

        // 5 double columns
        for (int i = 0; i < 5; i++)
            schemaBuilder.Field(f => f.Name($"dbl_{i}").DataType(DoubleType.Default));

        // 5 timestamp columns
        for (int i = 0; i < 5; i++)
            schemaBuilder.Field(f => f.Name($"ts_{i}").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")));

        var schema = schemaBuilder.Build();
        Assert.Equal(100, schema.FieldsList.Count);

        string tempIpcFile = Path.Combine(Path.GetTempPath(), $"wide_1mm_test_{Guid.NewGuid():N}.arrow");

        try
        {
            // 1. Generate 1MM rows x 100 columns into 1 Arrow IPC file
            var swGen = Stopwatch.StartNew();
            using (var fs = new FileStream(tempIpcFile, FileMode.Create, FileAccess.Write, FileShare.None, 4 * 1024 * 1024))
            using (var writer = new ArrowStreamWriter(fs, schema))
            {
                DateTimeOffset baseTime = DateTimeOffset.UtcNow;
                string[] sampleTexts = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"];

                for (int b = 0; b < batchCount; b++)
                {
                    int startOffset = b * batchSize;

                    var strBuilders = new StringArray.Builder[60];
                    for (int s = 0; s < 60; s++) strBuilders[s] = new StringArray.Builder();

                    var intBuilders = new Int32Array.Builder[20];
                    for (int n = 0; n < 20; n++) intBuilders[n] = new Int32Array.Builder();

                    var longBuilders = new Int64Array.Builder[10];
                    for (int l = 0; l < 10; l++) longBuilders[l] = new Int64Array.Builder();

                    var dblBuilders = new DoubleArray.Builder[5];
                    for (int d = 0; d < 5; d++) dblBuilders[d] = new DoubleArray.Builder();

                    var tsBuilders = new TimestampArray.Builder[5];
                    for (int t = 0; t < 5; t++) tsBuilders[t] = new TimestampArray.Builder();

                    for (int r = 0; r < batchSize; r++)
                    {
                        int rowId = startOffset + r;

                        for (int s = 0; s < 60; s++)
                            strBuilders[s].Append(sampleTexts[(rowId + s) % 8]);

                        for (int n = 0; n < 20; n++)
                            intBuilders[n].Append(rowId * 10 + n);

                        for (int l = 0; l < 10; l++)
                            longBuilders[l].Append((long)rowId * 1000 + l);

                        for (int d = 0; d < 5; d++)
                            dblBuilders[d].Append(rowId * 1.25 + d);

                        for (int t = 0; t < 5; t++)
                            tsBuilders[t].Append(baseTime.AddSeconds(rowId));
                    }

                    var columns = new IArrowArray[100];
                    int colIdx = 0;
                    for (int s = 0; s < 60; s++) columns[colIdx++] = strBuilders[s].Build();
                    for (int n = 0; n < 20; n++) columns[colIdx++] = intBuilders[n].Build();
                    for (int l = 0; l < 10; l++) columns[colIdx++] = longBuilders[l].Build();
                    for (int d = 0; d < 5; d++) columns[colIdx++] = dblBuilders[d].Build();
                    for (int t = 0; t < 5; t++) columns[colIdx++] = tsBuilders[t].Build();

                    var batch = new RecordBatch(schema, columns, batchSize);
                    writer.WriteRecordBatch(batch);
                }
                writer.WriteEnd();
            }
            swGen.Stop();

            long fileSizeBytes = new FileInfo(tempIpcFile).Length;
            double fileSizeMB = fileSizeBytes / (1024.0 * 1024.0);

            // 2. Measure Native Arrow IPC Ingestion Time into DuckDB
            using var db = new Database(":memory:");
            var conn = db.Connection;

            var swIngest = Stopwatch.StartNew();
            conn.IngestArrowIpcFileNative("wide_1mm_table", tempIpcFile);
            swIngest.Stop();

            long ingestMs = swIngest.ElapsedMilliseconds;
            double rowsPerSec = (totalRows / (double)ingestMs) * 1000.0;
            double mbPerSec = (fileSizeMB / (ingestMs / 1000.0));

            Console.WriteLine($"=================================================================================");
            Console.WriteLine($" 1MM Rows x 100 Columns (60 Strings) Stress Test Results");
            Console.WriteLine($" Total Rows:           {totalRows:N0}");
            Console.WriteLine($" Columns:              100 (60 String, 20 Int32, 10 Int64, 5 Double, 5 Timestamp)");
            Console.WriteLine($" File Size:            {fileSizeMB:F2} MB");
            Console.WriteLine($" Generation Time:      {swGen.ElapsedMilliseconds:N0} ms");
            Console.WriteLine($" Ingestion Time:       {ingestMs:N0} ms");
            Console.WriteLine($" Ingestion Throughput: {rowsPerSec:N0} rows/s ({mbPerSec:F1} MB/s)");
            Console.WriteLine($"=================================================================================");

            // 3. Verify correctness
            using var countRes = conn.ExecuteQuery("SELECT count(*) FROM wide_1mm_table;");
            Assert.Equal(1_000_000L, countRes.GetValue<long>(0, 0));

            using var sampleRes = conn.ExecuteQuery("SELECT str_0, str_59, int_0, long_0, dbl_0 FROM wide_1mm_table LIMIT 5;");
            Assert.Equal(5L, sampleRes.RowCount);
            Assert.Equal("Alpha", sampleRes.GetString(0, 0));
        }
        finally
        {
            if (File.Exists(tempIpcFile))
            {
                try { File.Delete(tempIpcFile); } catch { }
            }
        }
    }

    [Fact]
    public void Test_NativeArrowIpc_StressTest_1MM_Rows_100_Columns_DictionaryEncoded()
    {
        // 1MM rows, 100 columns (60 dictionary-encoded strings, 20 int32, 10 int64, 5 double, 5 timestamp) in 1 file
        int totalRows = 1_000_000;
        int batchSize = 100_000;
        int batchCount = totalRows / batchSize;

        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, ordered: false);

        // Build 100-column schema
        var schemaBuilder = new Schema.Builder();

        // 60 dictionary-encoded string columns
        for (int i = 0; i < 60; i++)
            schemaBuilder.Field(f => f.Name($"dict_str_{i}").DataType(dictType));

        // 20 int32 columns
        for (int i = 0; i < 20; i++)
            schemaBuilder.Field(f => f.Name($"int_{i}").DataType(Int32Type.Default));

        // 10 int64 columns
        for (int i = 0; i < 10; i++)
            schemaBuilder.Field(f => f.Name($"long_{i}").DataType(Int64Type.Default));

        // 5 double columns
        for (int i = 0; i < 5; i++)
            schemaBuilder.Field(f => f.Name($"dbl_{i}").DataType(DoubleType.Default));

        // 5 timestamp columns
        for (int i = 0; i < 5; i++)
            schemaBuilder.Field(f => f.Name($"ts_{i}").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")));

        var schema = schemaBuilder.Build();
        Assert.Equal(100, schema.FieldsList.Count);

        string tempIpcFile = Path.Combine(Path.GetTempPath(), $"wide_1mm_dict_test_{Guid.NewGuid():N}.arrow");

        try
        {
            var dictValuesBuilder = new StringArray.Builder();
            string[] sampleCategories = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"];
            foreach (var cat in sampleCategories) dictValuesBuilder.Append(cat);
            var dictValues = dictValuesBuilder.Build();

            // 1. Generate 1MM rows x 100 columns dictionary-encoded into 1 Arrow IPC file
            var swGen = Stopwatch.StartNew();
            using (var fs = new FileStream(tempIpcFile, FileMode.Create, FileAccess.Write, FileShare.None, 4 * 1024 * 1024))
            using (var writer = new ArrowStreamWriter(fs, schema))
            {
                DateTimeOffset baseTime = DateTimeOffset.UtcNow;

                for (int b = 0; b < batchCount; b++)
                {
                    int startOffset = b * batchSize;

                    var idxBuilders = new Int32Array.Builder[60];
                    for (int s = 0; s < 60; s++) idxBuilders[s] = new Int32Array.Builder();

                    var intBuilders = new Int32Array.Builder[20];
                    for (int n = 0; n < 20; n++) intBuilders[n] = new Int32Array.Builder();

                    var longBuilders = new Int64Array.Builder[10];
                    for (int l = 0; l < 10; l++) longBuilders[l] = new Int64Array.Builder();

                    var dblBuilders = new DoubleArray.Builder[5];
                    for (int d = 0; d < 5; d++) dblBuilders[d] = new DoubleArray.Builder();

                    var tsBuilders = new TimestampArray.Builder[5];
                    for (int t = 0; t < 5; t++) tsBuilders[t] = new TimestampArray.Builder();

                    for (int r = 0; r < batchSize; r++)
                    {
                        int rowId = startOffset + r;

                        for (int s = 0; s < 60; s++)
                            idxBuilders[s].Append((rowId + s) % 8);

                        for (int n = 0; n < 20; n++)
                            intBuilders[n].Append(rowId * 10 + n);

                        for (int l = 0; l < 10; l++)
                            longBuilders[l].Append((long)rowId * 1000 + l);

                        for (int d = 0; d < 5; d++)
                            dblBuilders[d].Append(rowId * 1.25 + d);

                        for (int t = 0; t < 5; t++)
                            tsBuilders[t].Append(baseTime.AddSeconds(rowId));
                    }

                    var columns = new IArrowArray[100];
                    int colIdx = 0;
                    for (int s = 0; s < 60; s++)
                        columns[colIdx++] = new DictionaryArray(dictType, idxBuilders[s].Build(), dictValues);

                    for (int n = 0; n < 20; n++) columns[colIdx++] = intBuilders[n].Build();
                    for (int l = 0; l < 10; l++) columns[colIdx++] = longBuilders[l].Build();
                    for (int d = 0; d < 5; d++) columns[colIdx++] = dblBuilders[d].Build();
                    for (int t = 0; t < 5; t++) columns[colIdx++] = tsBuilders[t].Build();

                    var batch = new RecordBatch(schema, columns, batchSize);
                    writer.WriteRecordBatch(batch);
                }
                writer.WriteEnd();
            }
            swGen.Stop();

            long fileSizeBytes = new FileInfo(tempIpcFile).Length;
            double fileSizeMB = fileSizeBytes / (1024.0 * 1024.0);

            // 2. Measure Native Arrow IPC Ingestion Time into DuckDB
            using var db = new Database(":memory:");
            var conn = db.Connection;

            var swIngest = Stopwatch.StartNew();
            conn.IngestArrowIpcFileNative("wide_1mm_dict_table", tempIpcFile);
            swIngest.Stop();

            long ingestMs = swIngest.ElapsedMilliseconds;
            double rowsPerSec = (totalRows / (double)ingestMs) * 1000.0;
            double mbPerSec = (fileSizeMB / (ingestMs / 1000.0));

            Console.WriteLine($"=================================================================================");
            Console.WriteLine($" 1MM Rows x 100 Columns (60 Dictionary-Encoded Strings) Stress Test Results");
            Console.WriteLine($" Total Rows:           {totalRows:N0}");
            Console.WriteLine($" Columns:              100 (60 Dict-String, 20 Int32, 10 Int64, 5 Double, 5 Timestamp)");
            Console.WriteLine($" File Size:            {fileSizeMB:F2} MB");
            Console.WriteLine($" Generation Time:      {swGen.ElapsedMilliseconds:N0} ms");
            Console.WriteLine($" Ingestion Time:       {ingestMs:N0} ms");
            Console.WriteLine($" Ingestion Throughput: {rowsPerSec:N0} rows/s ({mbPerSec:F1} MB/s)");
            Console.WriteLine($"=================================================================================");

            // 3. Verify correctness
            using var countRes = conn.ExecuteQuery("SELECT count(*) FROM wide_1mm_dict_table;");
            Assert.Equal(1_000_000L, countRes.GetValue<long>(0, 0));

            using var sampleRes = conn.ExecuteQuery("SELECT dict_str_0, dict_str_59, int_0, long_0, dbl_0 FROM wide_1mm_dict_table LIMIT 5;");
            Assert.Equal(5L, sampleRes.RowCount);
            Assert.Equal("Alpha", sampleRes.GetString(0, 0));

            using var distinctRes = conn.ExecuteQuery("SELECT count(DISTINCT dict_str_0) FROM wide_1mm_dict_table;");
            Assert.Equal(8L, distinctRes.GetValue<long>(0, 0));
        }
        finally
        {
            if (File.Exists(tempIpcFile))
            {
                try { File.Delete(tempIpcFile); } catch { }
            }
        }
    }

    [Fact]
    public void Test_NativeArrowIpc_DictionaryEncoding_Success()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var statusDictType = new DictionaryType(Int32Type.Default, StringType.Default, ordered: false);
        var countryDictType = new DictionaryType(Int16Type.Default, StringType.Default, ordered: false);

        var schema = new Schema.Builder()
            .Field(f => f.Name("user_id").DataType(Int32Type.Default))
            .Field(f => f.Name("status").DataType(statusDictType))
            .Field(f => f.Name("country").DataType(countryDictType))
            .Build();

        int rowCount = 100;
        var idBuilder = new Int32Array.Builder();
        var statusIdxBuilder = new Int32Array.Builder();
        var countryIdxBuilder = new Int16Array.Builder();

        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            if (i % 10 == 0)
            {
                statusIdxBuilder.AppendNull();
            }
            else
            {
                statusIdxBuilder.Append(i % 3); // 0 = PENDING, 1 = ACTIVE, 2 = SUSPENDED
            }

            countryIdxBuilder.Append((short)(i % 4)); // 0 = US, 1 = UK, 2 = DE, 3 = JP
        }

        var statusValuesBuilder = new StringArray.Builder();
        statusValuesBuilder.Append("PENDING");
        statusValuesBuilder.Append("ACTIVE");
        statusValuesBuilder.Append("SUSPENDED");
        var statusValues = statusValuesBuilder.Build();

        var countryValuesBuilder = new StringArray.Builder();
        countryValuesBuilder.Append("US");
        countryValuesBuilder.Append("UK");
        countryValuesBuilder.Append("DE");
        countryValuesBuilder.Append("JP");
        var countryValues = countryValuesBuilder.Build();

        var statusArr = new DictionaryArray(statusDictType, statusIdxBuilder.Build(), statusValues);
        var countryArr = new DictionaryArray(countryDictType, countryIdxBuilder.Build(), countryValues);

        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            idBuilder.Build(),
            statusArr,
            countryArr
        }, rowCount);

        byte[] ipcBytes = SerializeBatch(schema, batch);

        // Ingest into DuckDB
        conn.IngestArrowIpcNative("dict_users", ipcBytes);

        // Verify total rows
        using var countRes = conn.ExecuteQuery("SELECT count(*), count(status), count(country) FROM dict_users;");
        Assert.Equal(100L, countRes.GetValue<long>(0, 0));
        Assert.Equal(90L, countRes.GetValue<long>(0, 1)); // 10 nulls
        Assert.Equal(100L, countRes.GetValue<long>(0, 2));

        // Verify distinct values
        using var distinctRes = conn.ExecuteQuery("SELECT count(DISTINCT status), count(DISTINCT country) FROM dict_users;");
        Assert.Equal(3L, distinctRes.GetValue<long>(0, 0));
        Assert.Equal(4L, distinctRes.GetValue<long>(0, 1));

        // Verify decoded text values
        using var filterRes = conn.ExecuteQuery("SELECT count(*) FROM dict_users WHERE status = 'ACTIVE';");
        Assert.True(filterRes.GetValue<long>(0, 0) > 0);

        using var countryFilterRes = conn.ExecuteQuery("SELECT count(*) FROM dict_users WHERE country = 'US';");
        Assert.Equal(25L, countryFilterRes.GetValue<long>(0, 0));

        // Verify null preservation
        using var nullRes = conn.ExecuteQuery("SELECT count(*) FROM dict_users WHERE status IS NULL;");
        Assert.Equal(10L, nullRes.GetValue<long>(0, 0));
    }

    [Fact]
    public void Test_NativeArrowIpc_DictionaryEncoding_ParquetExport_Verification()
    {
        using var db = new Database(":memory:");
        var conn = db.Connection;

        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, ordered: false);
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("tier").DataType(dictType))
            .Build();

        var idB = new Int32Array.Builder();
        var idxB = new Int32Array.Builder();
        for (int i = 0; i < 50; i++)
        {
            idB.Append(i);
            idxB.Append(i % 3);
        }

        var valB = new StringArray.Builder();
        valB.Append("BRONZE");
        valB.Append("SILVER");
        valB.Append("GOLD");

        var dictArr = new DictionaryArray(dictType, idxB.Build(), valB.Build());
        var batch = new RecordBatch(schema, new IArrowArray[] { idB.Build(), dictArr }, 50);
        byte[] ipcBytes = SerializeBatch(schema, batch);

        // 1. Ingest into DuckDB
        conn.IngestArrowIpcNative("tier_source", ipcBytes);

        // 2. Export table to Parquet bytes using ParquetBuffer
        byte[] parquetBytes = Parquet.ParquetBuffer.ExportTableToParquetBytes(conn, "tier_source", "ZSTD");
        Assert.True(parquetBytes.Length > 0);

        // 3. Inspect Parquet metadata in DuckDB to confirm dictionary encoding (RLE_DICTIONARY)
        string tempPq = Path.Combine(Path.GetTempPath(), $"pq_verify_{Guid.NewGuid():N}.parquet");
        File.WriteAllBytes(tempPq, parquetBytes);
        try
        {
            using var metaRes = conn.ExecuteQuery($"SELECT column_id, encodings::VARCHAR FROM parquet_metadata('{tempPq}');");
            Assert.Equal(2L, metaRes.RowCount);

            // Column 1 (tier) should contain RLE_DICTIONARY encoding
            string tierEncodings = metaRes.GetString(1, 1);
            Assert.Contains("RLE_DICTIONARY", tierEncodings);

            // 4. Ingest exported Parquet bytes back into DuckDB and verify round-trip parity
            Parquet.ParquetBuffer.IngestParquetBytes(conn, "tier_roundtrip", parquetBytes);

            using var verifyRes = conn.ExecuteQuery("SELECT tier, count(*) FROM tier_roundtrip GROUP BY tier ORDER BY tier;");
            Assert.Equal(3L, verifyRes.RowCount);
            Assert.Equal("BRONZE", verifyRes.GetString(0, 0));
            Assert.Equal("GOLD", verifyRes.GetString(1, 0));
            Assert.Equal("SILVER", verifyRes.GetString(2, 0));
        }
        finally
        {
            if (File.Exists(tempPq)) File.Delete(tempPq);
        }
    }

    [Fact]
    public void Test_NativeArrowIpc_PythonCompatibility_DictionaryEncoding()
    {
        string pyScript = @"
import sys
try:
    import pyarrow as pa
except ImportError:
    sys.exit(42)

categories = pa.array(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'])
indices = pa.array([0, 1, 2, 3, 1, 0, 2, None], type=pa.int32())
dict_array = pa.DictionaryArray.from_arrays(indices, categories)

schema = pa.schema([
    ('event_id', pa.int32()),
    ('severity', dict_array.type)
])

batch = pa.record_batch([
    pa.array([1, 2, 3, 4, 5, 6, 7, 8], type=pa.int32()),
    dict_array
], schema=schema)

sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, schema) as writer:
    writer.write_batch(batch)

buf = sink.getvalue()
sys.stdout.buffer.write(buf.to_pybytes())
";

        string tempPyFile = Path.Combine(Path.GetTempPath(), $"pyarrow_dict_{Guid.NewGuid():N}.py");
        File.WriteAllText(tempPyFile, pyScript);

        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "python3",
                Arguments = tempPyFile,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            using var proc = Process.Start(psi);
            if (proc == null) return;

            using var ms = new MemoryStream();
            proc.StandardOutput.BaseStream.CopyTo(ms);
            proc.WaitForExit();

            if (proc.ExitCode == 42)
            {
                // pyarrow not installed, skip gracefully
                return;
            }

            Assert.Equal(0, proc.ExitCode);
            byte[] pyIpcBytes = ms.ToArray();
            Assert.True(pyIpcBytes.Length > 0);

            using var db = new Database(":memory:");
            var conn = db.Connection;

            conn.IngestArrowIpcNative("py_events", pyIpcBytes);

            using var res = conn.ExecuteQuery("SELECT event_id, severity FROM py_events ORDER BY event_id;");
            Assert.Equal(8L, res.RowCount);
            Assert.Equal("LOW", res.GetString(0, 1));
            Assert.Equal("MEDIUM", res.GetString(1, 1));
            Assert.Equal("HIGH", res.GetString(2, 1));
            Assert.Equal("CRITICAL", res.GetString(3, 1));
            Assert.True(res.IsNull(7, 1));
        }
        finally
        {
            if (File.Exists(tempPyFile)) File.Delete(tempPyFile);
        }
    }

    private static byte[] SerializeBatch(Schema schema, RecordBatch batch)
    {
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, schema))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        return ms.ToArray();
    }
}
