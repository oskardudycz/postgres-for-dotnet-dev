using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Npgsql;
using PostgresForDotnetDev.Core;
using PostgresForDotnetDev.Core.Reflection;
using PostgresForDotnetDev.Pongo.Filtering.Selectors;
using Remotion.Linq.Clauses;

namespace PostgresForDotnetDev.Pongo.Filtering;

public class PongoQueryableExecutor
{
    private readonly NpgsqlConnection connection;
    private readonly Func<Type, string> getTableName;

    public PongoQueryableExecutor(NpgsqlConnection connection, Func<Type, string> getTableName)
    {
        this.connection = connection;
        this.getTableName = getTableName;
    }

    public TResult? Execute<TSource, TResult>(Expression expression)
    {
        var sourceType = typeof(TSource);
        var tableName = getTableName(sourceType);
        var itemType = !sourceType.IsGenericType ? sourceType : sourceType.GetGenericArguments()[0];

        var whereClause = WhereClause.Parse(tableName, expression);

        Expression selectExpression;
        switch (expression)
        {
            case MethodCallExpression
            {
                Method: { Name: nameof(Queryable.Select) }, Arguments: { Count: 2 }
            } selectCall:
                selectExpression = selectCall.Arguments[1];
                break;
            case MethodCallExpression
            {
                Method: { Name: nameof(Queryable.SelectMany) }, Arguments: { Count: 2 }
            } selectManyCall:
                selectExpression = selectManyCall.Arguments[1];
                break;
            default:
                throw new NotSupportedException($"The method '{expression}' is not supported by this provider.");
        }

        var selectClause =
            SelectParser.Parse(tableName, ExtractSelectExpression(expression)!); // ExtractSelectExpression(expression);

        var sql = $"SELECT {selectClause} FROM {tableName} WHERE {whereClause}";

        using var command = new NpgsqlCommand(sql, connection);

        switch (expression)
        {
            case MethodCallExpression { Method: { Name: nameof(Queryable.Any) } }:
                command.CommandText = $"SELECT EXISTS({sql})";
                return (TResult?)command.ExecuteScalar();

            case MethodCallExpression { Method: { Name: nameof(Queryable.All) } }:
                command.CommandText = $"SELECT NOT EXISTS(SELECT 1 FROM {tableName} WHERE NOT ({whereClause}))";
                return (TResult?)command.ExecuteScalar();

            case MethodCallExpression { Method: { Name: nameof(Queryable.SingleOrDefault) } }:
                return GetItemOf<TResult>(command, shouldBeSingle: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.FirstOrDefault) } }:
                return GetItemOf<TResult>(command);

            case MethodCallExpression { Method: { Name: nameof(Queryable.Single) } }:
                return GetItemOf<TResult>(command, shouldFailIfNotExists: true, shouldBeSingle: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.First) } }:
                return GetItemOf<TResult>(command, shouldFailIfNotExists: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.Select) } }:
                var method = FindMaterializationMethod(expression);
                return this.Invoke<PongoQueryableExecutor, TResult>(nameof(GetListOf), itemType, command)!;

            // case ConstantExpression { Type: { IsGenericType: true } }:
            //     return this.Invoke<PongoQueryableExecutor, TResult>(nameof(GetListOf), itemType, command)!;

            default:
                throw new NotSupportedException($"The method '{expression}' is not supported by this provider.");
        }
    }

    private static MethodInfo? FindMaterializationMethod(Expression expression)
    {
        if (expression is MethodCallExpression methodCall)
        {
            var method = methodCall.Method;
            if (method.Name == nameof(Enumerable.ToArray) ||
                method.Name == nameof(Enumerable.ToList) ||
                method.Name == nameof(Enumerable.First) ||
                method.Name == nameof(Enumerable.FirstOrDefault) ||
                method.Name == nameof(Enumerable.Single) ||
                method.Name == nameof(Enumerable.SingleOrDefault))
            {
                return method;
            }
            else
            {
                return FindMaterializationMethod(methodCall.Arguments.Last());
            }
        }
        else if (expression is UnaryExpression unaryExpression)
        {
            return FindMaterializationMethod(unaryExpression.Operand);
        }
        else
        {
            return null;
        }
    }

    private static bool IsMaterializationMethod(MethodInfo methodInfo)
    {
        return methodInfo.Name == nameof(Enumerable.ToList) ||
               methodInfo.Name == nameof(Enumerable.ToArray) ||
               methodInfo.Name == nameof(Queryable.FirstOrDefault) ||
               methodInfo.Name == nameof(Queryable.SingleOrDefault) ||
               methodInfo.Name == nameof(Queryable.Any) ||
               methodInfo.Name == nameof(Queryable.All) ||
               methodInfo.Name == nameof(Queryable.First) ||
               methodInfo.Name == nameof(Queryable.Single) ||
               methodInfo.Name == nameof(Queryable.SingleOrDefault);
    }

    private static MethodCallExpression? ExtractSelectExpression(Expression expression)
    {
        return ExtractMethodCallExpression(expression, "Select");
    }

    private static MethodCallExpression? ExtractWhereExpression(Expression expression)
    {
        return ExtractMethodCallExpression(expression, "Where");
    }

    private static MethodCallExpression? ExtractMethodCallExpression(Expression expression, string methodName)
    {
        while (expression != null)
        {
            if (expression is MethodCallExpression methodCallExpression &&
                methodCallExpression.Method.DeclaringType == typeof(Queryable) &&
                methodCallExpression.Method.Name == methodName)
            {
                return methodCallExpression;
            }

            if (expression is ConstantExpression { Value: IQueryable queryable })
            {
                expression = queryable.Expression;
            }
            else
            {
                break;
            }
        }

        return null;
    }

    private TResult? GetItemOf<TResult>(
        NpgsqlCommand command,
        bool shouldFailIfNotExists = false,
        bool shouldBeSingle = false
    )
    {
        using var reader = command.ExecuteReader();

        if (!reader.Read())
            return !shouldFailIfNotExists ? default : throw new InvalidOperationException("No rows!");

        var json = reader.GetString(0);

        if (shouldBeSingle && !reader.Read())
            throw new InvalidOperationException("More than one item");

        return JsonSerializer.Deserialize<TResult>(json);
    }

    private IEnumerable<T> GetListOf<T>(NpgsqlCommand command) =>
        command.AsEnumerableFromJson<T>().ToList();
}
