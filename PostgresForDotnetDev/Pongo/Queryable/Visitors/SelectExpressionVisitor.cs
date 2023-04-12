using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver.GeoJsonObjectModel;
using PostgresForDotnetDev.Core.Expressions;
using Remotion.Linq.Clauses.Expressions;

namespace PostgresForDotnetDev.Pongo;

public class SelectExpressionVisitor: ExpressionVisitor
{
    private readonly string tableName;
    private readonly CompositeCustomOperatorVisitor compositeCustomOperatorVisitor;

    public SelectExpressionVisitor(string tableName, CompositeCustomOperatorVisitor compositeCustomOperatorVisitor)
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
        var lambda = Expression.Lambda(body, node.Parameters);

        if (lambda.Body is not NewExpression newExpression)
            return lambda;

        var sb = new StringBuilder();
        for (int i = 0; i < newExpression.Arguments.Count; i++)
        {
            var argument = newExpression.Arguments[i] as SqlExpression;
            var member = newExpression.Members![i];

            if (argument == null)
                Visit(argument);

            if (member != null)
            {
                sb.Append($"${argument!.Sql} AS {member.Name}");
            }

            if (i < newExpression.Arguments.Count - 1)
            {
                sb.Append(", ");
            }
        }

        return new SqlExpression(sb.ToString());
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(Enumerable) && node.Method.Name == "Select")
        {
            return VisitSelectMethodCall(node);
        }

        return compositeCustomOperatorVisitor.Visit(node, Visit) ?? base.VisitMethodCall(node);
    }

    private Expression VisitSelectMethodCall(MethodCallExpression node)
    {
        if (node.Arguments.Count != 2)
        {
            throw new NotSupportedException("Only Enumerable.Select() with a single selector is supported.");
        }

        if (node.Arguments[1] is not UnaryExpression unaryExpression ||
            unaryExpression.Operand is not LambdaExpression lambdaExpression)
        {
            throw new NotSupportedException("The argument for Enumerable.Select() must be a lambda expression.");
        }

        // Traverse the expression inside the Select() method call
        Expression body = Visit(lambdaExpression.Body);

        if (body is not MemberInitExpression memberInitExpression)
        {
            throw new NotSupportedException(
                "The body of the lambda expression inside Enumerable.Select() must be a member initialization expression.");
        }

        var selectedProperties = new List<string>();
        foreach (MemberAssignment binding in memberInitExpression.Bindings)
        {
            if (binding.Expression is not SqlExpression sqlExpression)
            {
                throw new NotSupportedException(
                    "The value of the member assignment inside Enumerable.Select() must be an SQL expression.");
            }

            selectedProperties.Add($"{sqlExpression.Sql} as {binding.Member.Name}");
        }

        string jsonbExpression =
            $"jsonb_array_elements({tableName}.data->'{((MemberExpression)node.Arguments[0]).Member.Name}')";
        string projection = string.Join(", ", selectedProperties);
        return new SqlExpression($"{jsonbExpression} || jsonb_build_object({projection})", typeof(object));
    }

    private static Expression StripQuotes(Expression e)
    {
        while (e.NodeType == ExpressionType.Quote)
        {
            e = ((UnaryExpression)e).Operand;
        }

        return e;
    }
}
