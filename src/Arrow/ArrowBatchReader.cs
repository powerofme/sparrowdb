using System;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using SparrowDb.Native;

namespace SparrowDb.Arrow;

public class ArrowBatchReader
{
    public static void IngestIpcStreamSync(Connection connection, string tableName, Schema schema, byte[] arrowBytes)
    {
        if (arrowBytes == null || arrowBytes.Length == 0)
            throw new ArgumentException("Arrow byte stream cannot be empty", nameof(arrowBytes));

        using var memoryStream = new MemoryStream(arrowBytes);
        using var reader = new ArrowStreamReader(memoryStream);

        RecordBatch? batch;
        bool tableCreated = false;

        while ((batch = reader.ReadNextRecordBatch()) != null)
        {
            if (!tableCreated)
            {
                CreateTableFromSchema(connection, tableName, batch.Schema);
                tableCreated = true;
            }

            AppendRecordBatchHighPerformance(connection, tableName, batch);
        }
    }

    public static async Task IngestIpcStreamAsync(Connection connection, string tableName, Schema schema, byte[] arrowBytes)
    {
        if (arrowBytes == null || arrowBytes.Length == 0)
            throw new ArgumentException("Arrow byte stream cannot be empty", nameof(arrowBytes));

        using var memoryStream = new MemoryStream(arrowBytes);
        using var reader = new ArrowStreamReader(memoryStream);

        RecordBatch? batch;
        bool tableCreated = false;

        while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
        {
            if (!tableCreated)
            {
                CreateTableFromSchema(connection, tableName, batch.Schema);
                tableCreated = true;
            }

            AppendRecordBatchHighPerformance(connection, tableName, batch);
        }
    }

    private static void CreateTableFromSchema(Connection connection, string tableName, Schema schema)
    {
        if (connection.CreatedTables.ContainsKey(tableName)) return;

        var colDefs = new System.Collections.Generic.List<string>();

        foreach (var field in schema.FieldsList)
        {
            string duckDbType = MapArrowTypeToDuckDb(field.DataType);
            colDefs.Add($"\"{field.Name}\" {duckDbType}");
        }

        string sql = $"CREATE TABLE IF NOT EXISTS \"{tableName}\" ({string.Join(", ", colDefs)});";
        connection.ExecuteQuery(sql);
        connection.CreatedTables.TryAdd(tableName, true);
    }

    private static void AppendRecordBatchHighPerformance(Connection connection, string tableName, RecordBatch batch)
    {
        DuckDBState state = DuckDBNative.duckdb_appender_create(connection.Handle, null, tableName, out var appender);
        if (state != DuckDBState.Success)
            throw new InvalidOperationException($"Failed to create DuckDB appender for table '{tableName}'");

        try
        {
            int colCount = batch.Schema.FieldsList.Count;
            int batchLength = batch.Length;

            var colArrays = new IArrowArray[colCount];
            for (int col = 0; col < colCount; col++)
            {
                colArrays[col] = batch.Column(col);
            }

            for (int row = 0; row < batchLength; row++)
            {
                DuckDBNative.duckdb_appender_begin_row(appender);

                for (int col = 0; col < colCount; col++)
                {
                    var columnArray = colArrays[col];

                    if (columnArray.IsNull(row))
                    {
                        DuckDBNative.duckdb_append_null(appender);
                        continue;
                    }

                    switch (columnArray)
                    {
                        case Int32Array intArray:
                            DuckDBNative.duckdb_append_int32(appender, intArray.GetValue(row)!.Value);
                            break;
                        case Int64Array longArray:
                            DuckDBNative.duckdb_append_int64(appender, longArray.GetValue(row)!.Value);
                            break;
                        case Date32Array dateArray:
                            DuckDBNative.duckdb_append_int32(appender, dateArray.GetValue(row)!.Value);
                            break;
                        case DoubleArray doubleArray:
                            DuckDBNative.duckdb_append_double(appender, doubleArray.GetValue(row)!.Value);
                            break;
                        case FloatArray floatArray:
                            DuckDBNative.duckdb_append_double(appender, floatArray.GetValue(row)!.Value);
                            break;
                        case StringArray stringArray:
                            DuckDBNative.duckdb_append_varchar(appender, stringArray.GetString(row)!);
                            break;
                        case BooleanArray boolArray:
                            DuckDBNative.duckdb_append_bool(appender, boolArray.GetValue(row)!.Value);
                            break;
                        default:
                            DuckDBNative.duckdb_append_null(appender);
                            break;
                    }
                }

                DuckDBNative.duckdb_appender_end_row(appender);
            }

            DuckDBNative.duckdb_appender_flush(appender);
        }
        finally
        {
            appender.Dispose();
        }
    }

    private static string MapArrowTypeToDuckDb(IArrowType type)
    {
        return type.TypeId switch
        {
            ArrowTypeId.Int32 => "INTEGER",
            ArrowTypeId.Int64 => "BIGINT",
            ArrowTypeId.Date32 => "INTEGER",
            ArrowTypeId.Float => "FLOAT",
            ArrowTypeId.Double => "DOUBLE",
            ArrowTypeId.String => "VARCHAR",
            ArrowTypeId.Boolean => "BOOLEAN",
            ArrowTypeId.Timestamp => "TIMESTAMP",
            _ => "VARCHAR"
        };
    }
}
