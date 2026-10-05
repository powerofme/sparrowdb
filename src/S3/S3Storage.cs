using System;

namespace SparrowDb.S3;

public static class S3Storage
{
    public static void ConfigureS3Secret(Connection connection, string keyId, string secretKey, string region, string? endpoint = null)
    {
        // Load httpfs native extension
        connection.ExecuteQuery("INSTALL httpfs; LOAD httpfs;");

        var sqlBuilder = new System.Text.StringBuilder();
        sqlBuilder.Append($"CREATE SECRET s3_secret (TYPE S3, KEY_ID '{keyId}', SECRET '{secretKey}', REGION '{region}'");

        if (!string.IsNullOrWhiteSpace(endpoint))
        {
            sqlBuilder.Append($", ENDPOINT '{endpoint}'");
        }

        sqlBuilder.Append(");");

        connection.ExecuteQuery(sqlBuilder.ToString());
    }

    public static void ExportToS3(Connection connection, string tableName, string s3Path, string format = "CSV")
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        if (string.IsNullOrWhiteSpace(s3Path))
            throw new ArgumentException("S3 path cannot be null or empty", nameof(s3Path));

        string copySql = $"COPY \"{tableName}\" TO '{s3Path}' (FORMAT {format});";
        connection.ExecuteQuery(copySql);
    }
}
