using System.Linq.Expressions;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDev.Pongo.Filtering;

public static class WhereClause
{
    public static string Get(string tableName, Expression expression)
    {
        var whereExpressions = new FilterCriteriaExtractor().GetCriteriaExpressions(expression);

        if (!whereExpressions.Any())
            return "1=1";

        var visitor = new QueryExpressionVisitor(tableName, new TimeScaleOperatorVisitor());

        var whereClause = string.Join(
            ") AND (",
            whereExpressions.Select(whereExpression =>visitor.Visit(whereExpression).UnwrapSqlExpression())
        );

        return $"({whereClause})";
    }
}
