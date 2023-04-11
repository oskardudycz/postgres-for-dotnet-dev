using System.Linq.Expressions;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Driver.GeoJsonObjectModel;
using PostgresForDotnetDev.Core.Expressions;

namespace PostgresForDotnetDev.Pongo;

public class QueryExpressionVisitor: ExpressionVisitor
{
    private readonly string tableName;
    private readonly CompositeCustomOperatorVisitor compositeCustomOperatorVisitor;

    public QueryExpressionVisitor(string tableName, CompositeCustomOperatorVisitor compositeCustomOperatorVisitor)
    {
        this.tableName = tableName;
        this.compositeCustomOperatorVisitor = compositeCustomOperatorVisitor;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        var left = Unwrap(node.Left);
        var right = Unwrap(node.Right);

        var op = MapToSqlOperator(node.NodeType);
        string sqlExpression = $"{Format(left)} {op} {Format(right)}";
        return new SqlExpression(sqlExpression, typeof(bool));
    }

    private object? Unwrap(Expression expression) =>
        expression switch
        {
            MemberExpression =>
                Visit(expression),

            MethodCallExpression =>
                Visit(expression),

            ConstantExpression constant =>
                constant.Value,

            _ => Expression.Lambda(expression).Compile().DynamicInvoke()!
        };

    private static string MapToSqlOperator(ExpressionType expressionType) =>
        expressionType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"The binary operator '{expressionType}' is not supported")
        };

    protected override Expression VisitMember(MemberExpression node)
    {
        if ((node.Member.MemberType != MemberTypes.Property && node.Member.MemberType != MemberTypes.Field))
            return base.VisitMember(node);

        if (node.Expression is not MemberExpression
            {
                Expression: ConstantExpression constantExpression
            } innerMemberExpression)
        {
            return new SqlExpression($"{tableName}.{BuildJsonbExpression(node, "\"data\"")}");
        }

        var closureInstance = constantExpression.Value!;

        var documentInstance = innerMemberExpression.Member switch
        {
            PropertyInfo propertyInfo => propertyInfo.GetValue(closureInstance),
            FieldInfo fieldInfo => fieldInfo.GetValue(closureInstance),
            _ => null
        };

        if (documentInstance == null)
            return new SqlExpression($"{tableName}.{BuildJsonbExpression(node, "\"data\"")}");

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

        return new SqlExpression(Format(value), node.Type);
    }

    private static string BuildJsonbExpression(MemberExpression node, string currentExpression)
    {
        if (node.Expression is MemberExpression innerMemberExpression)
            currentExpression = BuildJsonbExpression(innerMemberExpression, currentExpression);

        return $"{currentExpression}->>'{node.Member.Name}'";
    }

    protected override Expression VisitConstant(ConstantExpression node) =>
        new SqlExpression(Format(node.Value));

    private static string Format(object? value) =>
        value switch
        {
            SqlExpression sqlExpression => sqlExpression.Sql,

            string => $"'{value}'",

            TimeSpan => $"'{value:G}'",

            DateTime date => $"'{date.ToString("yyyy-MM-ddTHH:mm:ss'Z'")}'",

            DateTimeOffset date => date.Offset == TimeSpan.Zero
                ? $"'{date.ToString("yyyy-MM-ddTHH:mm:ss'Z'")}'"
                : $"'{date.ToString("yyyy-MM-ddTHH:mm:sszzz")}'",

            BsonDocument bsonDoc => $"'{bsonDoc.ToJson().Replace("'", "''")}'::jsonb",

            GeoJson2DGeographicCoordinates coordinates =>
                $"ST_SetSRID(ST_Point({coordinates.Longitude}, {coordinates.Latitude}), 4326)",

            null => "NULL",

            _ => value.ToString()!
        };

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        var body = Visit(node.Body);
        return Expression.Lambda(body, node.Parameters);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node) =>
        compositeCustomOperatorVisitor.Visit(node, Visit) ?? base.VisitMethodCall(node);
}
