using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;
using PostgresForDotnetDev.TimescaleDB;

namespace PostgresForDotnetDev.Pongo;

public class FilterExpressionVisitor: ExpressionVisitor
{
    private readonly string tableName;

    public FilterExpressionVisitor(string tableName)
    {
        this.tableName = tableName;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        object? left;
        switch (node.Left)
        {
            case MemberExpression:
            case MethodCallExpression:
                left = Visit(node.Left);
                break;
            case ConstantExpression constantLeft:
                left = constantLeft.Value;
                break;
            default:
                left = Expression.Lambda(node.Left).Compile().DynamicInvoke()!;
                break;
        }

        object? right;
        switch (node.Right)
        {
            case MemberExpression:
            case MethodCallExpression:
                right = Visit(node.Right);
                break;
            case ConstantExpression constantRight:
                right = constantRight.Value;
                break;
            default:
                right = Expression.Lambda(node.Right).Compile().DynamicInvoke()!;
                break;
        }

        var op = node.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"The binary operator '{node.NodeType}' is not supported")
        };


        // Build the SQL expression
        string leftSql = left is SqlExpression expression1 ? expression1.Sql : FormatConstant(left);
        string rightSql = right is SqlExpression expression ? expression.Sql : FormatConstant(right);
        string sqlExpression = $"{leftSql} {op} {rightSql}";
        return new SqlExpression(sqlExpression, typeof(bool));
    }

    protected override Expression VisitMember(MemberExpression node) =>
        new SqlExpression($"'{tableName}.{node.Member.Name}'");

    protected override Expression VisitConstant(ConstantExpression node) =>
        new SqlExpression(FormatConstant(node.Value));

    private static string FormatConstant(object? value)
    {
        return value switch
        {
            null => "NULL",
            string => $"'{value}'",
            TimeSpan => $"'{value:G}'",
            DateTime date => $"'{date.ToString("yyyy-MM-ddTHH:mm:ss'Z'")}'",
            DateTimeOffset date => date.Offset == TimeSpan.Zero
                ? $"'{date.ToString("yyyy-MM-ddTHH:mm:ss'Z'")}'"
                : $"'{date.ToString("yyyy-MM-ddTHH:mm:sszzz")}'",
            _ => value.ToString()!
        };
    }

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        var body = Visit(node.Body);
        return Expression.Lambda(body, node.Parameters);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType != typeof(TimescaleDbFunctions)) return base.VisitMethodCall(node);

        return new CompositeTimescaleDbFunction().Visit(node, Visit) ?? base.VisitMethodCall(node);
    }
}
