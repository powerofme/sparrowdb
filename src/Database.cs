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
