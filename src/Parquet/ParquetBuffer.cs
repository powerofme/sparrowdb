using System;
using System.Collections.Generic;
using System.IO;
using SparrowDb.Native;

namespace SparrowDb.Parquet;

public static class ParquetBuffer
{
    private static string GetEphemeralDirectory()
    {
        // On Linux (Docker/Kubernetes), /dev/shm is a tmpfs (100% in-memory RAM disk)
        if (OperatingSystem.IsLinux() && Directory.Exists("/dev/shm"))
            return "/dev/shm";

        return Path.GetTempPath();
    }

    public static byte[] ExportTableToParquetBytes(Connection connection, string tableName, string compression = "ZSTD")
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));

        string tempPath = Path.Combine(GetEphemeralDirectory(), $"sparrow_parquet_{Guid.NewGuid():N}.parquet");
        try
        {
            string sql = $"COPY \"{tableName}\" TO '{tempPath}' (FORMAT PARQUET, COMPRESSION {compression});";
            connection.ExecuteQuery(sql);
            return File.ReadAllBytes(tempPath);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { }
            }
        }
    }

    public static byte[] ExportQueryToParquetBytes(Connection connection, string querySql, string compression = "ZSTD")
    {
        if (string.IsNullOrWhiteSpace(querySql))
            throw new ArgumentException("Query SQL cannot be null or empty", nameof(querySql));

        string tempPath = Path.Combine(GetEphemeralDirectory(), $"sparrow_query_parquet_{Guid.NewGuid():N}.parquet");
        try
        {
            string sql = $"COPY ({querySql.TrimEnd(';')}) TO '{tempPath}' (FORMAT PARQUET, COMPRESSION {compression});";
            connection.ExecuteQuery(sql);
            return File.ReadAllBytes(tempPath);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { }
            }
        }
    }

    public static IEnumerable<byte[]> ExportTableToParquetChunks(
        Connection connection,
        string tableName,
        int rowsPerChunk = 50_000,
        string compression = "ZSTD")
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        if (rowsPerChunk <= 0)
            throw new ArgumentOutOfRangeException(nameof(rowsPerChunk), "Rows per chunk must be greater than zero.");

        long totalRows;
        using (var countResult = connection.ExecuteQuery($"SELECT COUNT(*) AS cnt FROM \"{tableName}\";"))
        {
            totalRows = countResult.GetValue<long>(0, 0);
        }

        if (totalRows == 0)
            yield break;

        long offset = 0;
        string ephemeralDir = GetEphemeralDirectory();

        while (offset < totalRows)
        {
            long currentChunkSize = Math.Min(rowsPerChunk, totalRows - offset);
            string tempPath = Path.Combine(ephemeralDir, $"sparrow_parquet_chunk_{Guid.NewGuid():N}.parquet");
            byte[] bytes;
            try
            {
                string sql = $"COPY (SELECT * FROM \"{tableName}\" LIMIT {currentChunkSize} OFFSET {offset}) TO '{tempPath}' (FORMAT PARQUET, COMPRESSION {compression});";
                connection.ExecuteQuery(sql);
                bytes = File.ReadAllBytes(tempPath);
            }
            finally
            {
                if (File.Exists(tempPath))
                {
                    try { File.Delete(tempPath); } catch { }
                }
            }

            yield return bytes;
            offset += currentChunkSize;
        }
    }

    public static void IngestParquetBytes(Connection connection, string tableName, byte[] parquetBytes)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        if (parquetBytes == null || parquetBytes.Length == 0)
            throw new ArgumentException("Parquet bytes cannot be null or empty", nameof(parquetBytes));

        string tempPath = Path.Combine(GetEphemeralDirectory(), $"sparrow_parquet_in_{Guid.NewGuid():N}.parquet");
        try
        {
            File.WriteAllBytes(tempPath, parquetBytes);

            bool tableExists = connection.CreatedTables.ContainsKey(tableName);
            if (!tableExists)
            {
                string checkSql = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tableName}';";
                using var checkResult = connection.ExecuteQuery(checkSql);
                tableExists = checkResult.GetValue<long>(0, 0) > 0;
            }

            if (tableExists)
            {
                string insertSql = $"INSERT INTO \"{tableName}\" SELECT * FROM read_parquet('{tempPath}');";
                connection.ExecuteQuery(insertSql);
            }
            else
            {
                string createSql = $"CREATE TABLE \"{tableName}\" AS SELECT * FROM read_parquet('{tempPath}');";
                connection.ExecuteQuery(createSql);
                connection.CreatedTables.TryAdd(tableName, true);
            }
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { }
            }
        }
    }

    public static void IngestParquetChunks(Connection connection, string tableName, IEnumerable<byte[]> parquetChunks)
    {
        if (parquetChunks == null)
            throw new ArgumentNullException(nameof(parquetChunks));

        foreach (var chunk in parquetChunks)
        {
            if (chunk != null && chunk.Length > 0)
            {
                IngestParquetBytes(connection, tableName, chunk);
            }
        }
    }
}
