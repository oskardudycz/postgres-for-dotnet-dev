using System.Text.Json;
using MongoDB.Driver;

namespace PostgresForDotnetDev.Pongo;

public static class PongoExpressionExtensions
{
    public static string ToSqlExpression<T>(this FilterDefinition<T> filter)
    {
        // Assuming a basic filter definition with a dictionary of field names and values for equality checks
        var equalityFilter = filter as Dictionary<string, object>;

        if (equalityFilter == null)
        {
            throw new NotSupportedException("Only basic equality filters are supported in this example.");
        }

        var conditions = new List<string>();
        foreach (var (field, o) in equalityFilter)
        {
            var value = JsonSerializer.Serialize(o);

            // Postgres JSONB equality operator
            conditions.Add($"data->>'{field}' = {value}");
        }

        return string.Join(" AND ", conditions);
    }

    public static string ToSqlExpression<T>(this UpdateDefinition<T> update)
    {
        // Assuming a basic update definition with a dictionary of field names and values for set operations
        var setUpdate = update as Dictionary<string, object>;

        if (setUpdate == null)
        {
            throw new NotSupportedException("Only basic set updates are supported in this example.");
        }

        var operations = new List<string>();
        foreach (var (field, o) in setUpdate)
        {
            var value = JsonSerializer.Serialize(o);

            // Postgres JSONB set operator
            operations.Add($"jsonb_set(data, '{{{field}}}', {value})");
        }

        return string.Join(" || ", operations);
    }
}
