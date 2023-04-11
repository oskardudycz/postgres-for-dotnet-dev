using System.Collections.Concurrent;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace PostgresForDotnetDev.Pongo.Filtering;

public static class FilterDefinitionParser
{
    private static ConcurrentDictionary<string, FilterExpressionVisitor> visitors = new();

    public static string ToSqlExpression<T>(this FilterDefinition<T> filter)
    {
        var bsonFilter = filter.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(),
            BsonSerializer.SerializerRegistry);

        return BsonFilterToSqlExpression(bsonFilter);
    }

    private static string BsonFilterToSqlExpression(BsonDocument filter)
    {
        var conditionsList = new List<string>();
        foreach (var field in filter)
        {
            if (!field.Value.IsBsonDocument)
            {
                conditionsList.Add($"data->>'{field.Name}' = {BsonValueToSqlLiteral(field.Value)}");
                continue;
            }

            var subDocument = field.Value.AsBsonDocument;
            conditionsList.AddRange(subDocument.Names.Select(operatorName =>
                operatorName is "$and" or "$or" or "$not"
                    ? ProcessLogicalOperator(operatorName, subDocument[operatorName].AsBsonArray)
                    : ProcessQueryOperator(field.Name, operatorName, subDocument[operatorName])));
        }

        return string.Join(" AND ", conditionsList);
    }

    private static string BsonValueToSqlLiteral(BsonValue value)
    {
        return (value.IsString ? $"'{value.AsString.Replace("'", "''")}'" : value.ToString())!;
    }

    private static string ProcessQueryOperator(string field, string operatorName, BsonValue value)
    {
        return operatorName switch
        {
            "$eq" => $"data->>'{field}' = {BsonValueToSqlLiteral(value)}",
            "$ne" => $"data->>'{field}' <> {BsonValueToSqlLiteral(value)}",
            "$gt" => $"CAST(data->>'{field}' AS DOUBLE PRECISION) > {value}",
            "$gte" => $"CAST(data->>'{field}' AS DOUBLE PRECISION) >= {value}",
            "$lt" => $"CAST(data->>'{field}' AS DOUBLE PRECISION) < {value}",
            "$lte" => $"CAST(data->>'{field}' AS DOUBLE PRECISION) <= {value}",
            "$in" => $"data->>'{field}' IN ({string.Join(", ", value.AsBsonArray.Select(BsonValueToSqlLiteral))})",
            "$nin" => $"data->>'{field}' NOT IN ({string.Join(", ", value.AsBsonArray.Select(BsonValueToSqlLiteral))})",
            _ => throw new NotSupportedException($"Unsupported query operator: {operatorName}")
        };
    }

    private static string ProcessLogicalOperator(string operatorName, BsonArray subfilters)
    {
        var conditions = subfilters.Select(subfilter => BsonFilterToSqlExpression(subfilter.AsBsonDocument)).ToArray();
        var joinedConditions = string.Join(" AND ", conditions);
        return operatorName switch
        {
            "$and" => $"({joinedConditions})",
            "$or" => $"({string.Join(" OR ", conditions)})",
            "$not" => $"NOT ({joinedConditions})",
            _ => throw new NotSupportedException($"Unsupported logical operator: {operatorName}")
        };
    }
}
