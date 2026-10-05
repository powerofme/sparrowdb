using System;
using System.Runtime.InteropServices;
using SparrowDb.Native;

namespace SparrowDb;

public unsafe sealed class Connection : IDisposable
{
    private readonly ConnectionSafeHandle _handle;
    private bool _disposed;

    internal Connection(ConnectionSafeHandle handle)
    {
        _handle = handle;
    }

    internal ConnectionSafeHandle Handle => _handle;
    internal System.Collections.Concurrent.ConcurrentDictionary<string, bool> CreatedTables { get; } = new();

    public QueryResult ExecuteQuery(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL query string cannot be null or empty", nameof(sql));

        DuckDBState state = DuckDBNative.duckdb_query(_handle, sql, out var nativeResult);
        if (state != DuckDBState.Success)
        {
            string err = Marshal.PtrToStringAnsi((IntPtr)nativeResult.ErrorMessage) ?? "Unknown DuckDB error";
            DuckDBNative.duckdb_destroy_result(ref nativeResult);
            throw new InvalidOperationException($"DuckDB Query Execution Error: {err}");
        }

        long colCount = (long)DuckDBNative.duckdb_column_count(ref nativeResult).Value;
        string[] names = new string[colCount];
        DuckDBType[] types = new DuckDBType[colCount];

        for (long i = 0; i < colCount; i++)
        {
            names[i] = Marshal.PtrToStringAnsi(DuckDBNative.duckdb_column_name(ref nativeResult, (ulong)i)) ?? string.Empty;
            types[i] = DuckDBNative.duckdb_column_type(ref nativeResult, (ulong)i);
        }

        return new QueryResult(nativeResult, names, types);
    }

    public Statement Prepare(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL statement cannot be null or empty", nameof(sql));

        DuckDBState state = DuckDBNative.duckdb_prepare(_handle, sql, out var stmtHandle);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to prepare SQL statement: {sql}");

        return new Statement(stmtHandle, this);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _handle.Dispose();
            _disposed = true;
        }
    }
}
