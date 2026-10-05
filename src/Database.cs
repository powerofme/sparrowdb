using System;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using SparrowDb.Arrow;
using SparrowDb.Native;
using SparrowDb.S3;

namespace SparrowDb;

public sealed class Database : IDisposable
{
    private readonly DatabaseSafeHandle _dbHandle;
    private readonly Connection _connection;
    private bool _disposed;

    public Database(string? databasePath = ":memory:")
    {
        DuckDBState state = DuckDBNative.duckdb_open(databasePath, out _dbHandle);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to open DuckDB database at path '{databasePath}'");

        state = DuckDBNative.duckdb_connect(_dbHandle, out var connHandle);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException("Failed to establish DuckDB connection");

        _connection = new Connection(connHandle);
    }

    public Connection Connection => _connection;

    public Connection CreateConnection()
    {
        DuckDBState state = DuckDBNative.duckdb_connect(_dbHandle, out var connHandle);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException("Failed to establish DuckDB connection");
        return new Connection(connHandle);
    }

    public void IngestArrowStream(string tableName, Schema schema, byte[] arrowBytes)
    {
        ArrowBatchReader.IngestIpcStreamSync(_connection, tableName, schema, arrowBytes);
    }

    public void IngestArrowIpcNative(
        string tableName,
        ReadOnlyMemory<byte> arrowIpc,
        bool autoCreateTable = true)
    {
        _connection.IngestArrowIpcNative(tableName, arrowIpc, autoCreateTable);
    }

    public void IngestArrowIpcNative(
        string tableName,
        byte[] arrowIpc,
        bool autoCreateTable = true)
    {
        _connection.IngestArrowIpcNative(tableName, arrowIpc, autoCreateTable);
    }

    public void IngestArrowIpcNative(
        string tableName,
        ReadOnlySpan<byte> arrowIpc,
        bool autoCreateTable = true)
    {
        _connection.IngestArrowIpcNative(tableName, arrowIpc, autoCreateTable);
    }

    public void IngestArrowIpcFileNative(
        string tableName,
        string filePath,
        bool autoCreateTable = true)
    {
        _connection.IngestArrowIpcFileNative(tableName, filePath, autoCreateTable);
    }

    public void IngestArrowIpcStreamNative(
        string tableName,
        System.IO.Stream stream,
        bool autoCreateTable = true)
    {
        _connection.IngestArrowIpcStreamNative(tableName, stream, autoCreateTable);
    }

    public async Task IngestArrowStreamAsync(string tableName, Schema schema, byte[] arrowBytes)
    {
        await ArrowBatchReader.IngestIpcStreamAsync(_connection, tableName, schema, arrowBytes);
    }

    public QueryResult ExecuteQuery(string sql)
    {
        return _connection.ExecuteQuery(sql);
    }

    public Statement Prepare(string sql)
    {
        return _connection.Prepare(sql);
    }

    public byte[] ExportToArrow(QueryResult result)
    {
        if (result == null)
            throw new ArgumentNullException(nameof(result));

        using var memoryStream = new MemoryStream();
        memoryStream.Write(new byte[] { 0x41, 0x52, 0x38, 0x30, 0x30, 0x31 }, 0, 6);
        return memoryStream.ToArray();
    }

    public void ExportToS3(string tableName, string s3Path, string format = "CSV")
    {
        S3Storage.ExportToS3(_connection, tableName, s3Path, format);
    }

    public byte[] ExportToParquet(string tableName, string compression = "ZSTD")
    {
        return Parquet.ParquetBuffer.ExportTableToParquetBytes(_connection, tableName, compression);
    }

    public byte[] ExportQueryToParquet(string querySql, string compression = "ZSTD")
    {
        return Parquet.ParquetBuffer.ExportQueryToParquetBytes(_connection, querySql, compression);
    }

    public System.Collections.Generic.IEnumerable<byte[]> ExportToParquetChunks(string tableName, int rowsPerChunk = 50_000, string compression = "ZSTD")
    {
        return Parquet.ParquetBuffer.ExportTableToParquetChunks(_connection, tableName, rowsPerChunk, compression);
    }

    public void IngestParquetBytes(string tableName, byte[] parquetBytes)
    {
        Parquet.ParquetBuffer.IngestParquetBytes(_connection, tableName, parquetBytes);
    }

    public void IngestParquetChunks(string tableName, System.Collections.Generic.IEnumerable<byte[]> parquetChunks)
    {
        Parquet.ParquetBuffer.IngestParquetChunks(_connection, tableName, parquetChunks);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _connection.Dispose();
            _dbHandle.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
