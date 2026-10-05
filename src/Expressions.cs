using System;
using System.Collections.Generic;
using System.Text;

namespace SparrowDb;

public class SqlBuilder
{
    private string? _fromTable;
    private readonly List<string> _projections = new();
    private readonly List<string> _aggregations = new();
    private readonly List<string> _whereConditions = new();
    private readonly List<string> _groupByColumns = new();
    private readonly List<string> _orderByColumns = new();
    private string? _pivotClause;

    public SqlBuilder From(string tableName)
    {
        _fromTable = tableName ?? throw new ArgumentNullException(nameof(tableName));
        return this;
    }

    public SqlBuilder Select(params string[] columns)
    {
        _projections.AddRange(columns);
        return this;
    }

    public SqlBuilder Avg(string column, string alias)
    {
        _aggregations.Add($"AVG({column}) AS {alias}");
        return this;
    }

    public SqlBuilder Sum(string column, string alias)
    {
        _aggregations.Add($"SUM({column}) AS {alias}");
        return this;
    }

    public SqlBuilder Count(string column, string alias)
    {
        _aggregations.Add($"COUNT({column}) AS {alias}");
        return this;
    }

    public SqlBuilder Min(string column, string alias)
    {
        _aggregations.Add($"MIN({column}) AS {alias}");
        return this;
    }

    public SqlBuilder Max(string column, string alias)
    {
        _aggregations.Add($"MAX({column}) AS {alias}");
        return this;
    }

    public SqlBuilder Where(string condition)
    {
        if (!string.IsNullOrWhiteSpace(condition))
            _whereConditions.Add(condition);
        return this;
    }

    public SqlBuilder GroupBy(params string[] columns)
    {
        _groupByColumns.AddRange(columns);
        return this;
    }

    public SqlBuilder OrderBy(string column, string direction = "ASC")
    {
        _orderByColumns.Add($"{column} {direction}");
        return this;
    }

    public SqlBuilder Pivot(string onColumn, string aggregateFunction)
    {
        _pivotClause = $"PIVOT {_fromTable} ON {onColumn} USING {aggregateFunction}";
        return this;
    }

    public string Build()
    {
        if (!string.IsNullOrEmpty(_pivotClause))
        {
            var sbPivot = new StringBuilder(_pivotClause);
            if (_whereConditions.Count > 0)
            {
                sbPivot.Append(" WHERE ").Append(string.Join(" AND ", _whereConditions));
            }
            return sbPivot.ToString();
        }

        if (string.IsNullOrWhiteSpace(_fromTable))
            throw new InvalidOperationException("FROM table must be specified before building query");

        var sb = new StringBuilder("SELECT ");

        var selects = new List<string>(_projections);
        selects.AddRange(_aggregations);

        if (selects.Count == 0)
        {
            sb.Append("*");
        }
        else
        {
            sb.Append(string.Join(", ", selects));
        }

        sb.Append(" FROM ").Append(_fromTable);

        if (_whereConditions.Count > 0)
        {
            sb.Append(" WHERE ").Append(string.Join(" AND ", _whereConditions));
        }

        if (_groupByColumns.Count > 0)
        {
            sb.Append(" GROUP BY ").Append(string.Join(", ", _groupByColumns));
        }

        if (_orderByColumns.Count > 0)
        {
            sb.Append(" ORDER BY ").Append(string.Join(", ", _orderByColumns));
        }

        return sb.ToString();
    }
}
