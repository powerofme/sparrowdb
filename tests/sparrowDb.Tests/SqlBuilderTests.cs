using System;
using Xunit;
using SparrowDb;

namespace SparrowDb.Tests;

public class SqlBuilderTests
{
    [Fact]
    public void Build_SimpleSelectFromWhere_GeneratesCorrectSql()
    {
        var sql = new SqlBuilder()
            .From("results")
            .Select("x", "y")
            .Where("z > 10")
            .Build();

        Assert.Equal("SELECT x, y FROM results WHERE z > 10", sql);
    }

    [Fact]
    public void Build_AggregationsAndGroupBy_GeneratesCorrectSql()
    {
        var sql = new SqlBuilder()
            .From("results")
            .Avg("x", "avg_x")
            .Sum("y", "sum_y")
            .Where("z > 10")
            .GroupBy("category")
            .OrderBy("avg_x", "DESC")
            .Build();

        Assert.Equal("SELECT AVG(x) AS avg_x, SUM(y) AS sum_y FROM results WHERE z > 10 GROUP BY category ORDER BY avg_x DESC", sql);
    }

    [Fact]
    public void Build_PivotClause_GeneratesCorrectSql()
    {
        var sql = new SqlBuilder()
            .From("sales")
            .Pivot("quarter", "SUM(amount)")
            .Where("year = 2026")
            .Build();

        Assert.Equal("PIVOT sales ON quarter USING SUM(amount) WHERE year = 2026", sql);
    }
}
