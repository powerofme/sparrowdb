using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using SparrowDb;
using Xunit;

namespace SparrowDb.Tests;

public class ParquetBufferTests
{
    private static void PopulateSampleTable(Database db, string tableName, int rowCount)
    {
        db.ExecuteQuery($"CREATE TABLE \"{tableName}\" (id INTEGER, name VARCHAR, score DOUBLE, is_active BOOLEAN);");
        
        // Insert in batches of 1,000 rows using SQL
        int batchSize = 1000;
        int batches = (int)Math.Ceiling((double)rowCount / batchSize);

        for (int b = 0; b < batches; b++)
        {
            int currentBatch = Math.Min(batchSize, rowCount - (b * batchSize));
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO \"{tableName}\" VALUES ");

            for (int r = 0; r < currentBatch; r++)
            {
                int id = (b * batchSize) + r;
                string name = $"user_{id % 100}";
                double score = id * 1.5;
                bool active = id % 2 == 0;

                sb.Append($"({id}, '{name}', {score}, {active})");
                if (r < currentBatch - 1) sb.Append(", ");
            }
            sb.Append(";");
            db.ExecuteQuery(sb.ToString());
        }
    }

    [Fact]
    public void ExportToParquet_And_IngestParquetBytes_RoundTrip_Success()
    {
        using var dbSource = new Database();
        PopulateSampleTable(dbSource, "source_table", 5_000);

        // Export table directly to in-memory byte[]
        byte[] parquetBytes = dbSource.ExportToParquet("source_table", compression: "ZSTD");

        Assert.NotNull(parquetBytes);
        Assert.True(parquetBytes.Length > 0);

        // Verify Parquet magic bytes ("PAR1" header and footer)
        string header = Encoding.ASCII.GetString(parquetBytes, 0, 4);
        string footer = Encoding.ASCII.GetString(parquetBytes, parquetBytes.Length - 4, 4);
        Assert.Equal("PAR1", header);
        Assert.Equal("PAR1", footer);

        // Ingest into target database from memory bytes
        using var dbTarget = new Database();
        dbTarget.IngestParquetBytes("restored_table", parquetBytes);

        using var countResult = dbTarget.ExecuteQuery("SELECT COUNT(*) AS cnt, SUM(score) AS total_score FROM restored_table;");
        long restoredCount = countResult.GetValue<long>(0, 0);
        double totalScore = countResult.GetValue<double>(0, 1);

        Assert.Equal(5_000, restoredCount);
        Assert.True(totalScore > 0);
    }

    [Fact]
    public void ExportQueryToParquet_FilteredQuery_RoundTrip_Success()
    {
        using var dbSource = new Database();
        PopulateSampleTable(dbSource, "source_table", 10_000);

        // Export filtered query to Parquet bytes
        string query = "SELECT name, COUNT(*) AS cnt, SUM(score) AS sum_score FROM source_table WHERE is_active = true GROUP BY name";
        byte[] parquetBytes = dbSource.ExportQueryToParquet(query, compression: "SNAPPY");

        Assert.NotNull(parquetBytes);
        Assert.True(parquetBytes.Length > 0);

        // Ingest query result into target database
        using var dbTarget = new Database();
        dbTarget.IngestParquetBytes("query_summary", parquetBytes);

        using var result = dbTarget.ExecuteQuery("SELECT COUNT(*) AS user_count, SUM(cnt) AS active_records FROM query_summary;");
        long userCount = result.GetValue<long>(0, 0);
        long activeRecords = result.GetValue<long>(0, 1);

        Assert.Equal(50, userCount); // exactly 50 even-numbered users are active
        Assert.Equal(5_000, activeRecords); // exactly half of 10,000 rows were active
    }

    [Fact]
    public void ExportToParquetChunks_And_IngestParquetChunks_RoundTrip_Success()
    {
        using var dbSource = new Database();
        int totalRows = 25_000;
        int chunkSize = 5_000;
        PopulateSampleTable(dbSource, "chunk_source", totalRows);

        // Export table in chunks of 5,000 rows
        List<byte[]> chunks = dbSource.ExportToParquetChunks("chunk_source", rowsPerChunk: chunkSize, compression: "ZSTD").ToList();

        Assert.Equal(5, chunks.Count);

        foreach (var chunk in chunks)
        {
            Assert.True(chunk.Length > 0);
            string header = Encoding.ASCII.GetString(chunk, 0, 4);
            string footer = Encoding.ASCII.GetString(chunk, chunk.Length - 4, 4);
            Assert.Equal("PAR1", header);
            Assert.Equal("PAR1", footer);
        }

        // Ingest chunks into target database using IngestParquetChunks
        using var dbTarget = new Database();
        dbTarget.IngestParquetChunks("reconstructed_table", chunks);

        using var countResult = dbTarget.ExecuteQuery("SELECT COUNT(*) AS cnt, MIN(id) AS min_id, MAX(id) AS max_id FROM reconstructed_table;");
        long restoredCount = countResult.GetValue<long>(0, 0);
        int minId = countResult.GetValue<int>(0, 1);
        int maxId = countResult.GetValue<int>(0, 2);

        Assert.Equal(totalRows, restoredCount);
        Assert.Equal(0, minId);
        Assert.Equal(totalRows - 1, maxId);
    }

    [Fact]
    public void ExportAndIngest_LeavesNoTemporaryFileResidue()
    {
        string tempDir = OperatingSystem.IsLinux() && Directory.Exists("/dev/shm") ? "/dev/shm" : Path.GetTempPath();
        
        using var db = new Database();
        PopulateSampleTable(db, "temp_check", 2_000);

        int filesBefore = Directory.GetFiles(tempDir, "sparrow_parquet_*.parquet").Length;

        // Perform export
        byte[] bytes = db.ExportToParquet("temp_check");

        // Perform chunked export
        var chunks = db.ExportToParquetChunks("temp_check", rowsPerChunk: 500).ToList();

        // Perform ingestion
        db.IngestParquetBytes("temp_check_copy", bytes);
        db.IngestParquetChunks("temp_check_chunks", chunks);

        int filesAfter = Directory.GetFiles(tempDir, "sparrow_parquet_*.parquet").Length;

        // All ephemeral files must be cleaned up immediately
        Assert.Equal(filesBefore, filesAfter);
    }
}
