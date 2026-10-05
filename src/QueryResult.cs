using System;
using System.Runtime.InteropServices;
using SparrowDb.Native;

namespace SparrowDb;

public unsafe class QueryResult : IDisposable
{
    private DuckDBResult _result;
    private bool _disposed;

    internal QueryResult(DuckDBResult result)
    {
        _result = result;
    }

    public long RowCount => (long)DuckDBNative.duckdb_row_count(ref _result).Value;
    public long ColumnCount => (long)DuckDBNative.duckdb_column_count(ref _result).Value;

    public string[] ColumnNames
    {
        get;
    } = [];

    public DuckDBType[] ColumnTypes
    {
        get;
    } = [];

    internal QueryResult(DuckDBResult result, string[] columnNames, DuckDBType[] columnTypes) : this(result)
    {
        ColumnNames = columnNames;
        ColumnTypes = columnTypes;
    }

    public string GetColumnName(long col)
    {
        if (col < 0 || col >= ColumnCount)
            throw new ArgumentOutOfRangeException(nameof(col));

        IntPtr ptr = DuckDBNative.duckdb_column_name(ref _result, (ulong)col);
        return Marshal.PtrToStringAnsi(ptr) ?? string.Empty;
    }

    public DuckDBType GetColumnType(long col)
    {
        if (col < 0 || col >= ColumnCount)
            throw new ArgumentOutOfRangeException(nameof(col));

        return DuckDBNative.duckdb_column_type(ref _result, (ulong)col);
    }

    public bool IsNull(long row, long col)
    {
        if (row < 0 || row >= RowCount)
            throw new ArgumentOutOfRangeException(nameof(row));
        if (col < 0 || col >= ColumnCount)
            throw new ArgumentOutOfRangeException(nameof(col));

        bool* nullMask = DuckDBNative.duckdb_nullmask_data(ref _result, (ulong)col);
        return nullMask != null && nullMask[row];
    }

    public T GetValue<T>(long row, long col) where T : unmanaged
    {
        if (IsNull(row, col))
            throw new InvalidOperationException($"Value at row {row}, col {col} is NULL");

        void* dataPtr = DuckDBNative.duckdb_column_data(ref _result, (ulong)col);
        if (dataPtr == null)
            throw new InvalidOperationException("Column data pointer is null");

        T* typedPtr = (T*)dataPtr;
        return typedPtr[row];
    }

    public string GetString(long row, long col)
    {
        if (IsNull(row, col))
            return string.Empty;

        DuckDBType type = GetColumnType(col);
        if (type == DuckDBType.Varchar)
        {
            void* dataPtr = DuckDBNative.duckdb_column_data(ref _result, (ulong)col);
            IntPtr* ptrArray = (IntPtr*)dataPtr;
            return Marshal.PtrToStringAnsi(ptrArray[row]) ?? string.Empty;
        }

        return GetValue<int>(row, col).ToString();
    }

    public object? GetObject(long row, long col)
    {
        if (IsNull(row, col))
            return null;

        DuckDBType type = GetColumnType(col);
        return type switch
        {
            DuckDBType.Boolean => GetValue<bool>(row, col),
            DuckDBType.TinyInt => GetValue<sbyte>(row, col),
            DuckDBType.SmallInt => GetValue<short>(row, col),
            DuckDBType.Integer => GetValue<int>(row, col),
            DuckDBType.BigInt => GetValue<long>(row, col),
            DuckDBType.UnsignedTinyInt => GetValue<byte>(row, col),
            DuckDBType.UnsignedSmallInt => GetValue<ushort>(row, col),
            DuckDBType.UnsignedInteger => GetValue<uint>(row, col),
            DuckDBType.UnsignedBigInt => GetValue<ulong>(row, col),
            DuckDBType.Float => GetValue<float>(row, col),
            DuckDBType.Double => GetValue<double>(row, col),
            DuckDBType.Varchar => GetString(row, col),
            _ => GetString(row, col)
        };
    }

    public void* GetColumnDataPointer(long col)
    {
        return DuckDBNative.duckdb_column_data(ref _result, (ulong)col);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            DuckDBNative.duckdb_destroy_result(ref _result);
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    ~QueryResult()
    {
        Dispose();
    }
}
