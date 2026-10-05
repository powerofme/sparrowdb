using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using SparrowDb;

namespace SparrowDb.BenchmarkRunner;

internal class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("==========================================================================================================");
        Console.WriteLine(" sparrowDb Benchmark Suite — Multi-Tenant Threading (1,000,000 Rows per Tenant Instance)");
        Console.WriteLine(" Dataset per Tenant: 1,000,000 Rows x 50 Columns (~404.5 MB Stream per Tenant)");
        Console.WriteLine("==========================================================================================================");
        Console.WriteLine();

        var schema = Build50ColumnSchema();

        int[] threadTiers = [1, 2, 5, 10, 20, 30, 40, 50];
        int recordCountPerTenant = 1_000_000; // 1,000,000 records per tenant thread

        Console.WriteLine($"--- Multi-Tenant Scale Tiers ({recordCountPerTenant:N0} records x 50 columns per tenant instance) ---");
        Console.WriteLine("| Active Tenant Threads | Total Combined Records | Total Data Stream | Avg Ingest Time | Avg Query Time | Managed Heap | WorkingSet / Unmanaged | Status |");
        Console.WriteLine("|-----------------------|------------------------|-------------------|-----------------|----------------|--------------|------------------------|--------|");

        foreach (int threadCount in threadTiers)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            long managedStart = GC.GetTotalMemory(true);
            long processStart = Process.GetCurrentProcess().WorkingSet64;

            var ingestTimes = new ConcurrentBag<long>();
            var queryTimes = new ConcurrentBag<long>();
            int successCount = 0;
            int errorCount = 0;

            var swTotal = Stopwatch.StartNew();

            try
            {
                Parallel.For(0, threadCount, new ParallelOptions { MaxDegreeOfParallelism = threadCount }, t =>
                {
                    string uniqueTempDir = Path.Combine(Path.GetTempPath(), $"sparrow_tenant_{Guid.NewGuid():N}");
                    try
                    {
                        Directory.CreateDirectory(uniqueTempDir);

                        using var tenantDb = new Database();
                        tenantDb.ExecuteQuery("SET max_memory = '8GB'; SET threads = 4;");

                        // 1. Ingestion per tenant (1,000,000 rows in 50,000-row chunks)
                        var swIngest = Stopwatch.StartNew();
                        int chunkSize = 50_000;
                        int chunks = recordCountPerTenant / chunkSize;
                        for (int c = 0; c < chunks; c++)
                        {
                            byte[] chunkBytes = GenerateArrowChunk(schema, chunkSize, c * chunkSize);
                            tenantDb.IngestArrowStream("tenant_data", schema, chunkBytes);
                        }
                        swIngest.Stop();
                        ingestTimes.Add(swIngest.ElapsedMilliseconds);

                        // 2. Analytical Queries per tenant
                        var swQuery = Stopwatch.StartNew();
                        using (var res1 = tenantDb.ExecuteQuery("SELECT COUNT(*) FROM tenant_data WHERE int_0 > 500000 AND str_0 = 'CAT_0';")) { }
                        using (var res2 = tenantDb.ExecuteQuery("SELECT SUM(dec_0), SUM(int_0), SUM(int_1) FROM tenant_data;")) { }
                        using (var res3 = tenantDb.ExecuteQuery("PIVOT tenant_data ON str_0 USING SUM(dec_0);")) { }
                        swQuery.Stop();
                        queryTimes.Add(swQuery.ElapsedMilliseconds);

                        Interlocked.Increment(ref successCount);
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref errorCount);
                    }
                    finally
                    {
                        if (Directory.Exists(uniqueTempDir))
                        {
                            try { Directory.Delete(uniqueTempDir, recursive: true); } catch { }
                        }
                    }
                });

                swTotal.Stop();

                GC.Collect();
                GC.WaitForPendingFinalizers();

                long managedPost = GC.GetTotalMemory(false);
                long processPost = Process.GetCurrentProcess().WorkingSet64;

                double avgIngestMs = ingestTimes.Count > 0 ? ingestTimes.Average() : 0;
                double avgQueryMs = queryTimes.Count > 0 ? queryTimes.Average() : 0;

                long totalRecordsAllTenants = (long)successCount * recordCountPerTenant;
                long totalStreamBytes = totalRecordsAllTenants * 404; // ~404 bytes per row across 50 cols

                string status = errorCount == 0 ? "PASSED" : $"ERR ({errorCount})";

                Console.WriteLine($"| {threadCount,-21} | {totalRecordsAllTenants,-22:N0} | {FormatSize(totalStreamBytes),-17} | {FormatTime((long)avgIngestMs),-15} | {FormatTime((long)avgQueryMs),-14} | {FormatBytesMB(managedPost),-12} | {FormatBytesMB(processPost),-22} | {status,-6} |");

                if (errorCount > 0)
                {
                    Console.WriteLine($"[!] System capacity limit hit at {threadCount} concurrent 1MM-record tenant instances.");
                    break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"| {threadCount,-21} | BREAK POINT            | -                 | -               | -              | -            | -                      | FAILED |");
                Console.WriteLine($"[!] System breakpoint reached: {ex.Message}");
                break;
            }
        }

        Console.WriteLine();
        Console.WriteLine("==========================================================================================================");
        Console.WriteLine(" 1MM-Record Multi-Tenant Benchmark Execution Completed.");
        Console.WriteLine("==========================================================================================================");
    }

    private static string FormatSize(long bytes)
    {
        if (bytes < 1024 * 1024)
            return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024)
            return $"{bytes / (1024.0 * 1024.0):F2} MB";
        return $"{bytes / (1024.0 * 1024.0 * 1024.0):F2} GB";
    }

    private static string FormatBytesMB(long bytes)
    {
        double mb = bytes / (1024.0 * 1024.0);
        return $"{mb:F2} MB";
    }

    private static string FormatTime(long ms)
    {
        if (ms < 1000)
            return $"{ms} ms";
        double sec = ms / 1000.0;
        return $"{sec:F2} s";
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
}
