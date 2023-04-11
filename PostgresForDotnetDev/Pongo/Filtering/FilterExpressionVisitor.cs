using System.Linq.Expressions;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Driver.GeoJsonObjectModel;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDev.Pongo;

public class FilterExpressionVisitor: ExpressionVisitor
{
    private readonly string tableName;
    private readonly CompositeCustomOperatorVisitor compositeCustomOperatorVisitor;

    public FilterExpressionVisitor(string tableName, CompositeCustomOperatorVisitor compositeCustomOperatorVisitor)
    {
        this.tableName = tableName;
        this.compositeCustomOperatorVisitor = compositeCustomOperatorVisitor;
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

    protected override Expression VisitMember(MemberExpression node)
    {
        if ((node.Member.MemberType != MemberTypes.Property && node.Member.MemberType != MemberTypes.Field))
            return base.VisitMember(node);

        // Check if the expression is a closure accessing a local variable
        if (node.Expression is MemberExpression { Expression: ConstantExpression constantExpression } innerMemberExpression)
        {
            object closureInstance = constantExpression.Value!;

            object? documentInstance = innerMemberExpression.Member switch
            {
                PropertyInfo propertyInfo => propertyInfo.GetValue(closureInstance),
                FieldInfo fieldInfo => fieldInfo.GetValue(closureInstance),
                _ => null
            };

            if (documentInstance != null)
            {
                object value;

                switch (node.Member)
                {
                    case PropertyInfo propertyInfo:
                        value = propertyInfo.GetValue(documentInstance)!;
                        break;
                    case FieldInfo fieldInfo:
                        value = fieldInfo.GetValue(documentInstance)!;
                        break;
                    default:
                        return base.VisitMember(node);
                }

                return new SqlExpression(FormatConstant(value), node.Type);
            }
        }

        string jsonbExpression = BuildJsonbExpression(node, "\"data\"");
        return new SqlExpression($"{tableName}.{jsonbExpression}");
    }

    private static string BuildJsonbExpression(MemberExpression node, string currentExpression)
    {
        if (node.Expression is MemberExpression innerMemberExpression)
        {
            currentExpression = BuildJsonbExpression(innerMemberExpression, currentExpression);
        }

        return $"{currentExpression}->>'{node.Member.Name}'";
    }

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
            BsonDocument bsonDoc => $"'{bsonDoc.ToJson().Replace("'", "''")}'::jsonb",
            GeoJson2DGeographicCoordinates coordinates =>
                $"ST_SetSRID(ST_Point({coordinates.Longitude}, {coordinates.Latitude}), 4326)",
            _ => value.ToString()!
        };
    }

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        var body = Visit(node.Body);
        return Expression.Lambda(body, node.Parameters);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node) =>
        compositeCustomOperatorVisitor.Visit(node, Visit) ?? base.VisitMethodCall(node);
}
