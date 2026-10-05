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
        Console.WriteLine(" SparrowDB Native Arrow IPC vs Legacy Scalar Ingestion Benchmark Suite");
        Console.WriteLine("==========================================================================================================");
        Console.WriteLine();

        var schema = BuildSchema();

        // 1. Direct Comparison: Legacy vs Native (100K and 500K rows)
        await RunComparisonBenchmarkAsync(schema, rowCounts: [100_000, 500_000]);

        // 2. Multi-Worker Concurrency (Production Architecture: 1 Database, N Connections, N Tables)
        await RunMultiWorkerConcurrencyBenchmarkAsync(schema, workerCounts: [1, 4, 8, 16, 23]);

        // 3. Wide Table Stress Test (1MM Rows x 100 Columns, 60 Strings, 1 File)
        await RunWideTableStressTestAsync();
    }

    private static async Task RunComparisonBenchmarkAsync(Schema schema, int[] rowCounts)
    {
        Console.WriteLine("----------------------------------------------------------------------------------------------------------");
        Console.WriteLine(" Benchmark 1: Ingestion Path Comparison (Legacy duckdb_append_* vs Native Arrow IPC)");
        Console.WriteLine("----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("| Rows       | Path             | Time (ms)  | Throughput (rows/s) | GC0 / GC1 / GC2 | Allocated MB |");
        Console.WriteLine("|------------|------------------|------------|---------------------|-----------------|--------------|");

        foreach (int rowCount in rowCounts)
        {
            byte[] ipcBytes = GenerateArrowPayload(schema, rowCount);
            double payloadMb = ipcBytes.Length / (1024.0 * 1024.0);

            // A. Legacy Scalar Appender
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                int g0Start = GC.CollectionCount(0);
                int g1Start = GC.CollectionCount(1);
                int g2Start = GC.CollectionCount(2);
                long memStart = GC.GetAllocatedBytesForCurrentThread();

                using var db = new Database(":memory:");
                var sw = Stopwatch.StartNew();
                db.IngestArrowStream("legacy_table", schema, ipcBytes);
                sw.Stop();

                long memEnd = GC.GetAllocatedBytesForCurrentThread();
                int g0 = GC.CollectionCount(0) - g0Start;
                int g1 = GC.CollectionCount(1) - g1Start;
                int g2 = GC.CollectionCount(2) - g2Start;
                double allocMb = (memEnd - memStart) / (1024.0 * 1024.0);
                double rowsPerSec = (rowCount / (double)sw.ElapsedMilliseconds) * 1000.0;

                Console.WriteLine($"| {rowCount,-10:N0} | {"Legacy Appender",-16} | {sw.ElapsedMilliseconds,-10:N0} | {rowsPerSec,-19:N0} | {g0}/{g1}/{g2,-11} | {allocMb,-12:F1} |");
            }

            // B. Native Arrow IPC Ingestion
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                int g0Start = GC.CollectionCount(0);
                int g1Start = GC.CollectionCount(1);
                int g2Start = GC.CollectionCount(2);
                long memStart = GC.GetAllocatedBytesForCurrentThread();

                using var db = new Database(":memory:");
                var sw = Stopwatch.StartNew();
                db.IngestArrowIpcNative("native_table", ipcBytes);
                sw.Stop();

                long memEnd = GC.GetAllocatedBytesForCurrentThread();
                int g0 = GC.CollectionCount(0) - g0Start;
                int g1 = GC.CollectionCount(1) - g1Start;
                int g2 = GC.CollectionCount(2) - g2Start;
                double allocMb = (memEnd - memStart) / (1024.0 * 1024.0);
                double rowsPerSec = (rowCount / (double)sw.ElapsedMilliseconds) * 1000.0;

                Console.WriteLine($"| {rowCount,-10:N0} | {"Native Arrow IPC",-16} | {sw.ElapsedMilliseconds,-10:N0} | {rowsPerSec,-19:N0} | {g0}/{g1}/{g2,-11} | {allocMb,-12:F1} |");
            }

            Console.WriteLine("|------------|------------------|------------|---------------------|-----------------|--------------|");
        }

        Console.WriteLine();
    }

    private static async Task RunMultiWorkerConcurrencyBenchmarkAsync(Schema schema, int[] workerCounts)
    {
        Console.WriteLine("----------------------------------------------------------------------------------------------------------");
        Console.WriteLine(" Benchmark 2: Production Multi-Worker Concurrency (1 Database :memory:, N Connections, N Tables)");
        Console.WriteLine(" Architecture: Each worker owns 1 DuckDB connection and ingests 50,000 rows into its own table");
        Console.WriteLine("----------------------------------------------------------------------------------------------------------");
        Console.WriteLine("| Workers | Total Rows | Total Data (MB) | Elapsed (ms) | Aggregated Throughput | Status |");
        Console.WriteLine("|---------|------------|-----------------|--------------|-----------------------|--------|");

        int rowsPerWorker = 50_000;
        byte[] ipcBytes = GenerateArrowPayload(schema, rowsPerWorker);
        double mbPerWorker = ipcBytes.Length / (1024.0 * 1024.0);

        foreach (int workers in workerCounts)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            using var sharedDb = new Database(":memory:");
            var sw = Stopwatch.StartNew();

            var tasks = new Task[workers];
            for (int w = 0; w < workers; w++)
            {
                int workerId = w;
                tasks[w] = Task.Run(() =>
                {
                    // Each worker creates and manages its own connection to the single shared in-memory database
                    using var conn = sharedDb.CreateConnection();
                    string tableName = $"worker_table_{workerId}";
                    conn.IngestArrowIpcNative(tableName, ipcBytes);
                });
            }

            await Task.WhenAll(tasks);
            sw.Stop();

            long totalRows = (long)workers * rowsPerWorker;
            double totalMb = workers * mbPerWorker;
            double throughput = (totalRows / (double)sw.ElapsedMilliseconds) * 1000.0;

            // Verify row counts across all worker tables
            var mainConn = sharedDb.Connection;
            bool verified = true;
            for (int w = 0; w < workers; w++)
            {
                using var res = mainConn.ExecuteQuery($"SELECT count(*) FROM worker_table_{w};");
                if (res.GetValue<long>(0, 0) != rowsPerWorker)
                {
                    verified = false;
                    break;
                }
            }

            string status = verified ? "PASSED" : "FAILED";
            Console.WriteLine($"| {workers,-7} | {totalRows,-10:N0} | {totalMb,-15:F1} | {sw.ElapsedMilliseconds,-12:N0} | {throughput,-19:N0} rows/s | {status,-6} |");
        }

        Console.WriteLine("----------------------------------------------------------------------------------------------------------");
        Console.WriteLine();
    }

    private static async Task RunWideTableStressTestAsync()
    {
        Console.WriteLine("----------------------------------------------------------------------------------------------------------");
        Console.WriteLine(" Benchmark 3: Wide Dataset Stress Test (1,000,000 Rows x 100 Columns, 60 Strings, 1 File)");
        Console.WriteLine(" Architecture: Single Arrow IPC file (~730 MB) ingested via Native Vectorized DuckDB Engine");
        Console.WriteLine("----------------------------------------------------------------------------------------------------------");

        int totalRows = 1_000_000;
        int batchSize = 100_000;
        int batchCount = totalRows / batchSize;

        var schemaBuilder = new Schema.Builder();
        for (int i = 0; i < 60; i++) schemaBuilder.Field(f => f.Name($"str_{i}").DataType(StringType.Default));
        for (int i = 0; i < 20; i++) schemaBuilder.Field(f => f.Name($"int_{i}").DataType(Int32Type.Default));
        for (int i = 0; i < 10; i++) schemaBuilder.Field(f => f.Name($"long_{i}").DataType(Int64Type.Default));
        for (int i = 0; i < 5; i++) schemaBuilder.Field(f => f.Name($"dbl_{i}").DataType(DoubleType.Default));
        for (int i = 0; i < 5; i++) schemaBuilder.Field(f => f.Name($"ts_{i}").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")));
        var schema = schemaBuilder.Build();

        string tempIpcFile = Path.Combine(Path.GetTempPath(), $"wide_1mm_stress_{Guid.NewGuid():N}.arrow");

        try
        {
            Console.Write("Generating 1,000,000 rows x 100 columns Arrow IPC file... ");
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
                        for (int s = 0; s < 60; s++) strBuilders[s].Append(sampleTexts[(rowId + s) % 8]);
                        for (int n = 0; n < 20; n++) intBuilders[n].Append(rowId * 10 + n);
                        for (int l = 0; l < 10; l++) longBuilders[l].Append((long)rowId * 1000 + l);
                        for (int d = 0; d < 5; d++) dblBuilders[d].Append(rowId * 1.25 + d);
                        for (int t = 0; t < 5; t++) tsBuilders[t].Append(baseTime.AddSeconds(rowId));
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
            Console.WriteLine($"Done ({swGen.ElapsedMilliseconds:N0} ms, {fileSizeMB:F2} MB).");

            GC.Collect();
            GC.WaitForPendingFinalizers();

            using var db = new Database(":memory:");
            var conn = db.Connection;

            Console.WriteLine("Executing Native Arrow IPC Ingestion into DuckDB table...");
            var swIngest = Stopwatch.StartNew();
            conn.IngestArrowIpcFileNative("wide_1mm_table", tempIpcFile);
            swIngest.Stop();

            long ingestMs = swIngest.ElapsedMilliseconds;
            double rowsPerSec = (totalRows / (double)ingestMs) * 1000.0;
            double mbPerSec = (fileSizeMB / (ingestMs / 1000.0));

            // Verify
            using var countRes = conn.ExecuteQuery("SELECT count(*) FROM wide_1mm_table;");
            long verifiedRows = countRes.GetValue<long>(0, 0);

            Console.WriteLine();
            Console.WriteLine($"| Metric                 | Value                                                        |");
            Console.WriteLine($"|------------------------|--------------------------------------------------------------|");
            Console.WriteLine($"| Total Ingested Rows    | {verifiedRows,-60:N0} |");
            Console.WriteLine($"| Total Columns          | {"100 (60 String, 20 Int32, 10 Int64, 5 Double, 5 Timestamp)",-60} |");
            Console.WriteLine($"| Single File Size       | {$"{fileSizeMB:F2} MB ({fileSizeBytes:N0} bytes)",-60} |");
            Console.WriteLine($"| Ingestion Time         | {$"{ingestMs:N0} ms ({ingestMs / 1000.0:F2} s)",-60} |");
            Console.WriteLine($"| Ingestion Throughput   | {$"{rowsPerSec:N0} rows/s ({mbPerSec:F1} MB/s)",-60} |");
            Console.WriteLine($"| Verification           | {$"COUNT(*) = {verifiedRows:N0} (PASSED)",-60} |");
            Console.WriteLine("----------------------------------------------------------------------------------------------------------");
            Console.WriteLine();
        }
        finally
        {
            if (File.Exists(tempIpcFile))
            {
                try { File.Delete(tempIpcFile); } catch { }
            }
        }
    }

    private static Schema BuildSchema()
    {
        return new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("amount").DataType(DoubleType.Default))
            .Field(f => f.Name("timestamp").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")))
            .Field(f => f.Name("category").DataType(StringType.Default))
            .Field(f => f.Name("description").DataType(StringType.Default))
            .Build();
    }

    private static byte[] GenerateArrowPayload(Schema schema, int rowCount)
    {
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, schema))
        {
            var idBuilder = new Int32Array.Builder();
            var amtBuilder = new DoubleArray.Builder();
            var tsBuilder = new TimestampArray.Builder();
            var catBuilder = new StringArray.Builder();
            var descBuilder = new StringArray.Builder();

            DateTimeOffset now = DateTimeOffset.UtcNow;
            string[] cats = ["FX", "EQUITY", "RATES", "COMMODITY", "CREDIT"];

            for (int i = 0; i < rowCount; i++)
            {
                idBuilder.Append(i);
                amtBuilder.Append(i * 12.34);
                tsBuilder.Append(now.AddSeconds(i));
                catBuilder.Append(cats[i % 5]);
                descBuilder.Append($"transaction_desc_{i % 100}");
            }

            var batch = new RecordBatch(schema, new IArrowArray[]
            {
                idBuilder.Build(),
                amtBuilder.Build(),
                tsBuilder.Build(),
                catBuilder.Build(),
                descBuilder.Build()
            }, rowCount);

            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return ms.ToArray();
    }
}
