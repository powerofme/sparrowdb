using System;
using System.Collections.Generic;
using SparrowDb.Native;

namespace SparrowDb;

public class Parameters
{
    private readonly Dictionary<string, object?> _map = new(StringComparer.OrdinalIgnoreCase);

    public Parameters Set(string name, object? value)
    {
        _map[name] = value;
        return this;
    }

    public bool TryGet(string name, out object? value) => _map.TryGetValue(name, out value);

    public IReadOnlyDictionary<string, object?> Values => _map;
}

public sealed class Statement : IDisposable
{
    private readonly PreparedStatementSafeHandle _handle;
    private readonly Connection _connection;
    private bool _disposed;

    internal Statement(PreparedStatementSafeHandle handle, Connection connection)
    {
        _handle = handle;
        _connection = connection;
    }

    public Statement Bind(ulong index, int val)
    {
        DuckDBState state = DuckDBNative.duckdb_bind_int32(_handle, index, val);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to bind int32 at index {index}");
        return this;
    }

    public Statement Bind(ulong index, long val)
    {
        DuckDBState state = DuckDBNative.duckdb_bind_int64(_handle, index, val);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to bind int64 at index {index}");
        return this;
    }

    public Statement Bind(ulong index, double val)
    {
        DuckDBState state = DuckDBNative.duckdb_bind_double(_handle, index, val);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to bind double at index {index}");
        return this;
    }

    public Statement Bind(ulong index, string val)
    {
        DuckDBState state = DuckDBNative.duckdb_bind_varchar(_handle, index, val);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to bind string at index {index}");
        return this;
    }

    public Statement BindNull(ulong index)
    {
        DuckDBState state = DuckDBNative.duckdb_bind_null(_handle, index);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to bind null at index {index}");
        return this;
    }

    public QueryResult Execute()
    {
        DuckDBState state = DuckDBNative.duckdb_execute_prepared(_handle, out var nativeResult);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException("Failed to execute prepared statement");

        long colCount = (long)DuckDBNative.duckdb_column_count(ref nativeResult).Value;
        string[] names = new string[colCount];
        DuckDBType[] types = new DuckDBType[colCount];

        for (long i = 0; i < colCount; i++)
        {
            names[i] = System.Runtime.InteropServices.Marshal.PtrToStringAnsi(DuckDBNative.duckdb_column_name(ref nativeResult, (ulong)i)) ?? string.Empty;
            types[i] = DuckDBNative.duckdb_column_type(ref nativeResult, (ulong)i);
        }

        return new QueryResult(nativeResult, names, types);
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
