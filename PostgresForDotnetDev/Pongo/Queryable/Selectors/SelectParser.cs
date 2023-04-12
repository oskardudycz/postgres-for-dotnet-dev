using System.Linq.Expressions;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDev.Pongo.Filtering.Selectors;

public class SelectParser
{
    public static string Parse(string tableName, Expression expression)
    {
        var selectExpressions = new SelectExtractor().GetSelectExpressions(expression);

        if (!selectExpressions.Any())
            return "\"data\"";

        var visitor = new SelectExpressionVisitor(tableName, new TimeScaleOperatorVisitor());

        var whereClause = string.Join(
            ") AND (",
            selectExpressions.Select(whereExpression =>visitor.Visit(whereExpression).UnwrapSqlExpression())
        );

        return $"({whereClause})";
    }
}
