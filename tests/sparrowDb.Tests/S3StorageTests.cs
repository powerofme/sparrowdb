using System;
using SparrowDb.S3;
using Xunit;

namespace SparrowDb.Tests;

public class S3StorageTests
{
    [Fact]
    public void ConfigureS3Secret_GeneratesCorrectNativeSql()
    {
        using var db = new Database();
        
        // Verifies S3 secret syntax configuration without throwing
        S3Storage.ConfigureS3Secret(db.Connection, "dummy_key", "dummy_secret", "us-east-1");
        
        var result = db.ExecuteQuery("SELECT current_setting('s3_region') AS s3_region;");
        Assert.Equal(1, result.RowCount);
    }
}
