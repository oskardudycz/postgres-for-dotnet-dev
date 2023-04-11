using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Npgsql;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;
using Remotion.Linq;

namespace PostgresForDotnetDev.Pongo.Filtering;

public class PongoQueryable<T>:  QueryableBase<T>
{
    public PongoQueryable(PongoQueryableProvider provider): base(provider)
    {
    }
}

public class PongoQueryableProvider: IQueryProvider
{
    private readonly PongoQueryableExecutor executor;

    public PongoQueryableProvider(PongoQueryableExecutor executor) =>
        this.executor = executor;

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.GetQueryableElementType();
        var queryableType = typeof(PongoQueryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression  _) =>
        throw new NotSupportedException();

    public object Execute(Expression expression) =>
        executor.Execute(expression);

    public TResult Execute<TResult>(Expression expression) =>
        executor.Execute<TResult>(expression)!;
}

public class PongoQueryableExecutor
{
    private readonly NpgsqlConnection connection;
    private readonly Func<Type, string> getTableName;

    private static readonly ConcurrentDictionary<Type, MethodInfo> ExecuteMethods = new();

    public PongoQueryableExecutor(NpgsqlConnection connection, Func<Type, string> getTableName)
    {
        this.connection = connection;
        this.getTableName = getTableName;
    }

    public object Execute(Expression _) =>
        throw new NotSupportedException();

    public TResult? Execute<TResult>(Expression expression)
    {
        var tableName = getTableName(typeof(TResult));

        var whereClause = new FilterExpressionVisitor(tableName, new TimeScaleOperatorVisitor());

        var sql = $"SELECT data FROM {tableName} WHERE {whereClause}";
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
                return GetSingleOrDefault<TResult>(command, shouldBeSingle: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.FirstOrDefault) } }:
                return GetSingleOrDefault<TResult>(command);

            case MethodCallExpression { Method: { Name: nameof(Queryable.Single) } }:
                return GetSingleOrDefault<TResult>(command, shouldFailIfNotExists: true, shouldBeSingle: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.First) } }:
                return GetSingleOrDefault<TResult>(command, shouldFailIfNotExists: true);

            // case MethodCallExpression { Method: { Name: nameof(Enumerable.ToList) } }:
            //     return (TResult) GetAll<TResult>(command).ToList();
            //
            // case MethodCallExpression { Method: { Name: nameof(Enumerable.ToArray) } }:
            //     return GetToArrayAsyncResult(command);

            default:
                throw new NotSupportedException($"The method '{expression}' is not supported by this provider.");
        }
    }

    private TResult? GetSingleOrDefault<TResult>(
        NpgsqlCommand command,
        bool shouldFailIfNotExists = false,
        bool shouldBeSingle = false
    )
    {
        var reader = command.ExecuteReader();

        if (!reader.Read())
            return !shouldFailIfNotExists ? default : throw new InvalidOperationException("No rows!");

        var json = reader.GetString(0);

        if (shouldBeSingle && !reader.Read())
            throw new InvalidOperationException("More than one item");

        return JsonSerializer.Deserialize<TResult>(json);
    }

    private IEnumerable<TResult> GetAll<TResult>(
        NpgsqlCommand command
    )
    {
        var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var json = reader.GetString(0);
            var document = JsonSerializer.Deserialize<TResult>(json)!;
            yield return document;
        }
    }


    public NpgsqlDataReader GetReader(Expression expression)
    {
        var elementType = expression.GetQueryableElementType();

        var tableName = getTableName(elementType);

        var whereClause = new FilterExpressionVisitor(tableName, new TimeScaleOperatorVisitor());

        var sql = $"SELECT data FROM {tableName} WHERE {whereClause}";
        using var command = new NpgsqlCommand(sql, connection);

        return command.ExecuteReader();
    }


    private static MethodInfo GetGenericPublishFor(Type elementType) =>
        ExecuteMethods.GetOrAdd(elementType, eventType =>
            typeof(PongoQueryableExecutor)
                .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                .Single(m => m.Name == nameof(Execute) && m.GetGenericArguments().Any())
                .MakeGenericMethod(eventType)
        );
}
