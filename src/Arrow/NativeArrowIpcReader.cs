using System;
using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.Ipc;
using SparrowDb.Native;

namespace SparrowDb.Arrow;

/// <summary>
/// High-performance native ingestion of Apache Arrow IPC streams into DuckDB.
/// Vectors Arrow buffers directly into DuckDB's columnar execution engine
/// without managed row-by-row iteration or duckdb_append_* calls.
/// </summary>
public static unsafe class NativeArrowIpcReader
{
    public static void IngestArrowIpcNative(
        Connection connection,
        string tableName,
        ReadOnlyMemory<byte> arrowIpc,
        bool autoCreateTable = true)
    {
        if (arrowIpc.IsEmpty)
            throw new ArgumentException("Arrow IPC payload cannot be empty", nameof(arrowIpc));

        using Stream stream = CreateStreamFromMemory(arrowIpc);
        IngestArrowIpcStreamNative(connection, tableName, stream, autoCreateTable);
    }

    public static void IngestArrowIpcStreamNative(
        Connection connection,
        string tableName,
        Stream stream,
        bool autoCreateTable = true)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));

        ArrowStreamReader reader;
        try
        {
            reader = new ArrowStreamReader(stream);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to initialize ArrowStreamReader from stream: {ex.Message}", ex);
        }

        using (reader)
        {
            if (reader.Schema == null)
                throw new InvalidOperationException("Arrow IPC stream contains no schema.");

            using var streamHolder = NativeArrowStreamHolder.Create(reader);
            string viewName = $"__sparrow_arrow_{Guid.NewGuid():N}";
            string quotedTable = QuoteIdentifier(tableName);
            string quotedView = QuoteIdentifier(viewName);

            var state = DuckDBNative.duckdb_arrow_scan(connection.Handle, viewName, streamHolder.StreamPtr);
            if (state != DuckDBState.Success)
                throw new InvalidOperationException($"duckdb_arrow_scan failed with status code {state}");

            try
            {
                if (autoCreateTable)
                {
                    connection.ExecuteQuery($"CREATE TABLE IF NOT EXISTS {quotedTable} AS SELECT * FROM {quotedView} LIMIT 0;");
                }

                connection.ExecuteQuery($"INSERT INTO {quotedTable} SELECT * FROM {quotedView};");
            }
            finally
            {
                try
                {
                    connection.ExecuteQuery($"DROP VIEW IF EXISTS {quotedView};");
                }
                catch
                {
                    // Best effort cleanup of virtual view
                }
            }
        }
    }

    public static void IngestArrowIpcFileNative(
        Connection connection,
        string tableName,
        string filePath,
        bool autoCreateTable = true)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        if (!File.Exists(filePath))
            throw new FileNotFoundException("Arrow IPC file not found", filePath);

        using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024);
        IngestArrowIpcStreamNative(connection, tableName, fileStream, autoCreateTable);
    }

    private static Stream CreateStreamFromMemory(ReadOnlyMemory<byte> memory)
    {
        if (MemoryMarshal.TryGetArray(memory, out var segment))
        {
            return new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false);
        }

        // For non-array backed memory (e.g. native buffers), copy or pin safely
        byte[] copy = memory.ToArray();
        return new MemoryStream(copy, 0, copy.Length, writable: false);
    }

    public static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
            throw new ArgumentException("Identifier cannot be null or empty", nameof(identifier));
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
