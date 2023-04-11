using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Npgsql;
using PostgresForDotnetDev.Core;
using PostgresForDotnetDev.Core.Reflection;

namespace PostgresForDotnetDev.Pongo.Filtering;

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

    public TResult? Execute<TResult>(Expression expression)
    {
        var type = typeof(TResult);
        var itemType = !type.IsGenericType ? type : type.GetGenericArguments()[0];
        var tableName = getTableName(itemType);

        var whereClause = WhereClause.Get(tableName, expression);

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
                return GetItemOf<TResult>(command, shouldBeSingle: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.FirstOrDefault) } }:
                return GetItemOf<TResult>(command);

            case MethodCallExpression { Method: { Name: nameof(Queryable.Single) } }:
                return GetItemOf<TResult>(command, shouldFailIfNotExists: true, shouldBeSingle: true);
            case MethodCallExpression { Method: { Name: nameof(Queryable.First) } }:
                return GetItemOf<TResult>(command, shouldFailIfNotExists: true);

            case ConstantExpression { Type: { IsGenericType: true } }:
                return this.Invoke<PongoQueryableExecutor, TResult>(nameof(GetListOf), itemType, command)!;

            default:
                throw new NotSupportedException($"The method '{expression}' is not supported by this provider.");
        }
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
