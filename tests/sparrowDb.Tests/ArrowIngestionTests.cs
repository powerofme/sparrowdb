using System;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using SparrowDb;
using Xunit;

namespace SparrowDb.Tests;

public class ArrowIngestionTests
{
    private const int RowCount = 1000;
    private const int ColCount = 30;

    [Fact]
    public async Task Benchmark_1000Rows_30Columns_IngestionAndQueries()
    {
        // 1. Generate synthetic Arrow IPC byte stream (1,000 rows x 30 columns)
        var schemaBuilder = new Schema.Builder();

        // Column 0: Category string column for PIVOT
        schemaBuilder.Field(f => f.Name("category").DataType(StringType.Default));

        // Column 1..29: Numeric columns for SUM and WHERE queries
        for (int c = 1; c < ColCount; c++)
        {
            schemaBuilder.Field(f => f.Name($"col_{c}").DataType(Int32Type.Default));
        }

        var schema = schemaBuilder.Build();

        var categoryBuilder = new StringArray.Builder();
        var numBuilders = new Int32Array.Builder[ColCount - 1];
        for (int i = 0; i < ColCount - 1; i++)
        {
            numBuilders[i] = new Int32Array.Builder();
        }

        string[] categories = ["CAT_A", "CAT_B", "CAT_C", "CAT_D"];

        for (int r = 0; r < RowCount; r++)
        {
            categoryBuilder.Append(categories[r % categories.Length]);

            for (int c = 1; c < ColCount; c++)
            {
                numBuilders[c - 1].Append((r + 1) * c);
            }
        }

        var recordBatchColumns = new IArrowArray[ColCount];
        recordBatchColumns[0] = categoryBuilder.Build();
        for (int c = 1; c < ColCount; c++)
        {
            recordBatchColumns[c] = numBuilders[c - 1].Build();
        }

        var batch = new RecordBatch(schema, recordBatchColumns, RowCount);

        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, schema))
        {
            await writer.WriteRecordBatchAsync(batch);
            await writer.WriteEndAsync();
        }

        byte[] arrowBytes = ms.ToArray();
        Assert.True(arrowBytes.Length > 0);

        // 2. Initialize Database and Ingest Arrow Stream
        using var db = new Database();
        db.IngestArrowStream("bench_table", schema, arrowBytes);

        // 3. Test SUM Aggregation
        var sumSql = "SELECT SUM(col_1) AS sum_col1, SUM(col_2) AS sum_col2 FROM bench_table";
        using (var sumResult = db.ExecuteQuery(sumSql))
        {
            Assert.Equal(1, sumResult.RowCount);
            Assert.Equal(2, sumResult.ColumnCount);
        }

        // 4. Test WHERE Clause Filtering
        var whereSql = "SELECT COUNT(*) AS filter_count FROM bench_table WHERE col_1 > 500";
        using (var whereResult = db.ExecuteQuery(whereSql))
        {
            Assert.Equal(1, whereResult.RowCount);
            long filteredCount = whereResult.GetValue<long>(0, 0);
            Assert.Equal(500, filteredCount);
        }

        // 5. Test PIVOT Query
        var pivotSql = "PIVOT bench_table ON category USING SUM(col_1)";
        using (var pivotResult = db.ExecuteQuery(pivotSql))
        {
            Assert.True(pivotResult.RowCount > 0);
            Assert.True(pivotResult.ColumnCount > 1);
        }

        // 6. Test Arrow IPC Export
        using (var fullQuery = db.ExecuteQuery("SELECT * FROM bench_table"))
        {
            byte[] exportedBytes = db.ExportToArrow(fullQuery);
            Assert.NotNull(exportedBytes);
            Assert.True(exportedBytes.Length >= 6);
            Assert.Equal(0x41, exportedBytes[0]); // 'A'
            Assert.Equal(0x52, exportedBytes[1]); // 'R'
        }
    }
}
