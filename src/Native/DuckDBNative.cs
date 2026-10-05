using System;
using System.Runtime.InteropServices;

namespace SparrowDb.Native;

public static unsafe class DuckDBNative
{
    private const string NativeLib = "duckdb";

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_open(string? path, out DatabaseSafeHandle database);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void duckdb_close(ref IntPtr database);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_connect(DatabaseSafeHandle database, out ConnectionSafeHandle connection);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void duckdb_disconnect(ref IntPtr connection);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_query(ConnectionSafeHandle connection, string query, out DuckDBResult result);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void duckdb_destroy_result(ref DuckDBResult result);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern idx_t duckdb_column_count(ref DuckDBResult result);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern idx_t duckdb_row_count(ref DuckDBResult result);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr duckdb_column_name(ref DuckDBResult result, idx_t col);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBType duckdb_column_type(ref DuckDBResult result, idx_t col);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void* duckdb_column_data(ref DuckDBResult result, idx_t col);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern bool* duckdb_nullmask_data(ref DuckDBResult result, idx_t col);

    // Prepared Statements
    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_prepare(ConnectionSafeHandle connection, string query, out PreparedStatementSafeHandle statement);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void duckdb_destroy_prepare(ref IntPtr statement);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_bind_varchar(PreparedStatementSafeHandle statement, idx_t index, string val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_bind_int32(PreparedStatementSafeHandle statement, idx_t index, int val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_bind_int64(PreparedStatementSafeHandle statement, idx_t index, long val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_bind_double(PreparedStatementSafeHandle statement, idx_t index, double val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_bind_null(PreparedStatementSafeHandle statement, idx_t index);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_execute_prepared(PreparedStatementSafeHandle statement, out DuckDBResult result);

    // Appender API
    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_appender_create(ConnectionSafeHandle connection, string? schema, string table, out AppenderSafeHandle appender);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_appender_begin_row(AppenderSafeHandle appender);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_appender_end_row(AppenderSafeHandle appender);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_append_varchar(AppenderSafeHandle appender, string val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_append_int32(AppenderSafeHandle appender, int val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_append_int64(AppenderSafeHandle appender, long val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_append_double(AppenderSafeHandle appender, double val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_append_bool(AppenderSafeHandle appender, bool val);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_append_null(AppenderSafeHandle appender);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_appender_flush(AppenderSafeHandle appender);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_appender_destroy(ref IntPtr appender);

    // Arrow Integration
    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern DuckDBState duckdb_query_arrow(ConnectionSafeHandle connection, string query, out void* result_arrow);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern DuckDBState duckdb_arrow_scan(ConnectionSafeHandle connection, string table_name, ArrowArrayStream* arrow_stream);
}
