using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using SparrowDb;
using Xunit;
using Xunit.Abstractions;

namespace SparrowDb.Tests;

public class ScaleBenchmarkTests
{
    private readonly ITestOutputHelper _output;

    public ScaleBenchmarkTests(ITestOutputHelper output)
    {
        _output = output;
    }

    private static Schema Build50ColumnSchema()
    {
        var builder = new Schema.Builder();

        // 1 Decimal (Double) column
        builder.Field(f => f.Name("dec_0").DataType(DoubleType.Default));

        // 2 Date (Date32) columns
        builder.Field(f => f.Name("date_0").DataType(Date32Type.Default));
        builder.Field(f => f.Name("date_1").DataType(Date32Type.Default));

        // 3 Int (Int32) columns
        builder.Field(f => f.Name("int_0").DataType(Int32Type.Default));
        builder.Field(f => f.Name("int_1").DataType(Int32Type.Default));
        builder.Field(f => f.Name("int_2").DataType(Int32Type.Default));

        // 44 String columns
        for (int i = 0; i < 44; i++)
        {
            builder.Field(f => f.Name($"str_{i}").DataType(StringType.Default));
        }

        return builder.Build();
    }

    private static byte[] GenerateArrowChunk(Schema schema, int rowCount, int startOffset)
    {
        using var ms = new MemoryStream();
        using var writer = new ArrowStreamWriter(ms, schema);

        var decBuilder = new DoubleArray.Builder();
        var date0Builder = new Date32Array.Builder();
        var date1Builder = new Date32Array.Builder();
        var int0Builder = new Int32Array.Builder();
        var int1Builder = new Int32Array.Builder();
        var int2Builder = new Int32Array.Builder();

        var strBuilders = new StringArray.Builder[44];
        for (int i = 0; i < 44; i++)
        {
            strBuilders[i] = new StringArray.Builder();
        }

        DateTime baseDate = new DateTime(2026, 1, 1);
        string[] cats = ["CAT_0", "CAT_1", "CAT_2", "CAT_3", "CAT_4"];

        for (int r = 0; r < rowCount; r++)
        {
            int rowId = startOffset + r;
            decBuilder.Append(rowId * 10.5);

            int days = (rowId % 365);
            date0Builder.Append(baseDate.AddDays(days));
            date1Builder.Append(baseDate.AddDays(days + 1));

            int0Builder.Append(rowId);
            int1Builder.Append(rowId * 2);
            int2Builder.Append(rowId * 3);

            string catVal = cats[rowId % 5];
            for (int s = 0; s < 44; s++)
            {
                strBuilders[s].Append(catVal);
            }
        }

        var columns = new IArrowArray[50];
        columns[0] = decBuilder.Build();
        columns[1] = date0Builder.Build();
        columns[2] = date1Builder.Build();
        columns[3] = int0Builder.Build();
        columns[4] = int1Builder.Build();
        columns[5] = int2Builder.Build();

        for (int s = 0; s < 44; s++)
        {
            columns[6 + s] = strBuilders[s].Build();
        }

        var recordBatch = new RecordBatch(schema, columns, rowCount);
        writer.WriteRecordBatch(recordBatch);
        writer.WriteEnd();

        return ms.ToArray();
    }

    [Theory]
    [InlineData(1_000)]
    [InlineData(10_000)]
    [InlineData(100_000)]
    [InlineData(1_000_000)] // 1MM
    public void Benchmark_50Columns_ScaleTier(int totalRows)
    {
        var schema = Build50ColumnSchema();
        using var db = new Database();

        int chunkSize = Math.Min(totalRows, 50_000);
        int chunks = (int)Math.Ceiling((double)totalRows / chunkSize);

        var sw = Stopwatch.StartNew();

        for (int i = 0; i < chunks; i++)
        {
            int currentChunkRows = Math.Min(chunkSize, totalRows - (i * chunkSize));
            byte[] chunkBytes = GenerateArrowChunk(schema, currentChunkRows, i * chunkSize);
            db.IngestArrowStream("bench_50col", schema, chunkBytes);
        }

        sw.Stop();
        long ingestMs = sw.ElapsedMilliseconds;

        // 2. Query to Filter (WHERE)
        int midVal = totalRows / 2;
        sw.Restart();
        using (var filterResult = db.ExecuteQuery($"SELECT COUNT(*) AS cnt FROM bench_50col WHERE int_0 > {midVal} AND str_0 = 'CAT_0';"))
        {
            sw.Stop();
            long filterMs = sw.ElapsedMilliseconds;
            long cnt = filterResult.GetValue<long>(0, 0);

            // 3. SUM() Query
            sw.Restart();
            using (var sumResult = db.ExecuteQuery("SELECT SUM(dec_0) AS sum_dec, SUM(int_0) AS sum_int0, SUM(int_1) AS sum_int1, SUM(int_2) AS sum_int2 FROM bench_50col;"))
            {
                sw.Stop();
                long sumMs = sw.ElapsedMilliseconds;

                // 4. PIVOT Query
                sw.Restart();
                using (var pivotResult = db.ExecuteQuery("PIVOT bench_50col ON str_0 USING SUM(dec_0);"))
                {
                    sw.Stop();
                    long pivotMs = sw.ElapsedMilliseconds;

                    _output.WriteLine($"| {totalRows,12:N0} | {ingestMs,14:N0} ms | {filterMs,12:N0} ms | {sumMs,10:N0} ms | {pivotMs,12:N0} ms |");
                    Assert.True(cnt >= 0);
                    Assert.Equal(1, sumResult.RowCount);
                    Assert.True(pivotResult.RowCount > 0);
                }
            }
        }
    }
}
