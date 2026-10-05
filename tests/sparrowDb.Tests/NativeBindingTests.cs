using System;
using SparrowDb;
using Xunit;

namespace SparrowDb.Tests;

public class NativeBindingTests
{
    [Fact]
    public void ExecuteQuery_SimpleSelect_ReturnsCorrectValue()
    {
        using var db = new Database();
        using var result = db.ExecuteQuery("SELECT 42 AS val, 'hello' AS str;");

        Assert.Equal(1, result.RowCount);
        Assert.Equal(2, result.ColumnCount);
        Assert.Equal("val", result.GetColumnName(0));
        Assert.Equal("str", result.GetColumnName(1));
        Assert.Equal(42, result.GetValue<int>(0, 0));
        Assert.Equal("hello", result.GetString(0, 1));
    }
}
