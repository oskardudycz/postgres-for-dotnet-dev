using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using NetTopologySuite.Geometries;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDev.Pongo.Filtering;

public static class FilterDefinitionParser
{
    private static ConcurrentDictionary<string, FilterExpressionVisitor> visitors = new();

    public static string ToSqlExpression<T>(this FilterDefinition<T> filter)
    {
        // Use a BsonDocument to access MongoDB filter elements
        var bsonFilter = filter.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(),
            BsonSerializer.SerializerRegistry);

        var propertyToColumnMapping = GetMappedColumns<T>();
        var filterBuilder = new StringBuilder();
        filterBuilder.Append(BsonFilterToSqlExpression(bsonFilter));

        // ... your existing filtering logic ...

        // Check for property names in the filter and replace them with the corresponding generated column names
        foreach (var kvp in propertyToColumnMapping)
        {
            var propertyName = "\"" + kvp.Key + "\"";
            var columnName = kvp.Value;

            filterBuilder.Replace(propertyName, columnName);
        }

        return filterBuilder.ToString();
    }

    public static string FilterConditionToSqlExpression<T>(Expression<Func<T, bool>> predicate, string tableName)
    {
        var expressionVisitor = new FilterExpressionVisitor(tableName, new TimeScaleOperatorVisitor());
        var sqlExpression = expressionVisitor.Visit(predicate);
        return sqlExpression.ToString();
    }

    public static string FilterConditionsToSqlExpression<T>(List<FilterCondition<T>> conditions, string tableName)
    {
        var filterExpressions = conditions
            .Select(condition => FilterConditionToSqlExpression(condition.Predicate, tableName)).ToList();
        var sqlExpression = string.Join(" AND ", filterExpressions);
        return sqlExpression;
    }

    private static Dictionary<string, string> GetMappedColumns<T>()
    {
        var mappedColumns = new Dictionary<string, string>();

        var properties = typeof(T).GetProperties();
        foreach (var property in properties)
        {
            if (property.PropertyType == typeof(Point) || property.PropertyType == typeof(List<Point>))
            {
                mappedColumns.Add(property.Name, property.Name);
            }
            else if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(List<DateTime>))
            {
                mappedColumns.Add(property.Name, property.Name);
            }
            // Add more conditions here for other property types
        }

        return mappedColumns;
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
