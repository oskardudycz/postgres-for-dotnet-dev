using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace PostgresForDotnetDev.Pongo;

public static class PongoExpressionExtensions
{
    public static string ToSqlExpression<T>(this FilterDefinition<T> filter)
    {
        // Use a BsonDocument to access MongoDB filter elements
        var bsonFilter = filter.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(),
            BsonSerializer.SerializerRegistry);
        return BsonFilterToSqlExpression(bsonFilter);
    }

    private static string BsonFilterToSqlExpression(BsonDocument bsonFilter)
    {
        // Implement a recursive function to handle various filter conditions and logical operators
        var conditions = new List<string>();

        foreach (var element in bsonFilter)
        {
            var field = element.Name;
            var value = element.Value;

            switch (field)
            {
                case "$and":
                case "$or":
                    var operatorName = field == "$and" ? "AND" : "OR";
                    var subFilters = value.AsBsonArray;
                    var subConditions = subFilters
                        .Select(subFilter => BsonFilterToSqlExpression(subFilter.AsBsonDocument)).ToList();
                    conditions.Add($"({string.Join($" {operatorName} ", subConditions)})");
                    break;

                case "$eq":
                case "$ne":
                case "$gt":
                case "$gte":
                case "$lt":
                case "$lte":
                    var sqlOperator = field switch
                    {
                        "$eq" => "=",
                        "$ne" => "<>",
                        "$gt" => ">",
                        "$gte" => ">=",
                        "$lt" => "<",
                        "$lte" => "<=",
                        _ => throw new NotSupportedException($"Unsupported filter operator: {field}")
                    };
                    conditions.Add($"data->>'{field}' {sqlOperator} '{value}'");
                    break;

                default:
                    conditions.Add($"data->>'{field}' = '{value}'");
                    break;
            }
        }

        return string.Join(" AND ", conditions);
    }

    public static string ToSqlExpression<T>(this UpdateDefinition<T> update)
    {
        // Use a BsonDocument to access MongoDB update elements
        var bsonUpdate = update.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(),
            BsonSerializer.SerializerRegistry);
        return BsonUpdateToSqlExpression(bsonUpdate.AsBsonDocument);
    }

    private static string BsonUpdateToSqlExpression(BsonDocument bsonUpdate)
    {
        var operations = new List<string>();

        foreach (var element in bsonUpdate)
        {
            var updateOperator = element.Name;
            var updateFields = element.Value.AsBsonDocument;

            foreach (var updateField in updateFields)
            {
                var field = updateField.Name;
                var value = updateField.Value;

                string operation;

                switch (updateOperator)
                {
                    case "$set":
                        operation = $"jsonb_set(data, '{{{field}}}', '{value}', true)";
                        break;

                    case "$unset":
                        operation = $"data - '{field}'";
                        break;

                    case "$inc":
                        operation =
                            $"jsonb_set(data, '{{{field}}}', (COALESCE((data->>'{field}')::numeric, 0) + {value})::text::jsonb, true)";
                        break;

                    case "$mul":
                        operation =
                            $"jsonb_set(data, '{{{field}}}', (COALESCE((data->>'{field}')::numeric, 1) * {value})::text::jsonb, true)";
                        break;

                    case "$min":
                        operation =
                            $"jsonb_set(data, '{{{field}}}', LEAST(COALESCE((data->>'{field}')::numeric, {value}), {value})::text::jsonb, true)";
                        break;

                    case "$max":
                        operation =
                            $"jsonb_set(data, '{{{field}}}', GREATEST(COALESCE((data->>'{field}')::numeric, {value}), {value})::text::jsonb, true)";
                        break;

                    case "$currentDate":
                        if (value.BsonType == BsonType.Document && value.AsBsonDocument.Contains("$type") &&
                            value.AsBsonDocument["$type"].AsString == "timestamp")
                        {
                            operation = $"jsonb_set(data, '{{{field}}}', to_jsonb(CURRENT_TIMESTAMP), true)";
                        }
                        else
                        {
                            operation = $"jsonb_set(data, '{{{field}}}', to_jsonb(CURRENT_DATE), true)";
                        }

                        break;

                    case "$push":
                        if (value.IsBsonDocument && value.AsBsonDocument.Contains("$each"))
                        {
                            var eachArray = value["$each"].AsBsonArray;
                            var elements = eachArray.Select(el => el.ToString()!).ToList();
                            var elementsJson = string.Join(", ", elements);
                            operation =
                                $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{elementsJson}]'::jsonb, true)";
                        }
                        else
                        {
                            operation =
                                $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{value}]'::jsonb, true)";
                        }

                        break;

                    case "$addToSet":
                        if (value.IsBsonDocument && value.AsBsonDocument.Contains("$each"))
                        {
                            var eachArray = value["$each"].AsBsonArray;
                            var elements = eachArray.Select(el => SanitizeStringValue(el.ToString()!)).ToList();

                            var elementsJson = string.Join(", ", elements);
                            operation =
                                $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{elementsJson}]'::jsonb) - 'null', true)";
                        }
                        else
                        {
                            var sanitizedValue = SanitizeStringValue(value.ToString()!);
                            operation =
                                $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{sanitizedValue}]'::jsonb) - 'null', true)";
                        }

                        break;

                    case "$pop":
                        var popValue = value.ToInt32();
                        operation = popValue > 0
                            ? $"jsonb_set(data, '{{{field}}}', data->'{field}' - (jsonb_array_length(data->'{field}') - 1), true)"
                            : $"jsonb_set(data, '{{{field}}}', data->'{field}' - 0, true)";

                        break;

                    case "$pull":
                        var sanitizedPullValue = SanitizeStringValue(value.ToString()!);
                        operation = $"jsonb_set(data, '{{{field}}}', data->'{field}' - '{sanitizedPullValue}', true)";
                        break;

                    case "$pullAll":
                        var pullAllArray = value.AsBsonArray;
                        var pullAllConditions = pullAllArray.Select(el => SanitizeStringValue(el.ToString()!))
                            .Select(sanitizedElement => $"data->'{field}' - '{sanitizedElement}'").ToList();

                        operation = $"jsonb_set(data, '{{{field}}}', {string.Join(" || ", pullAllConditions)}, true)";
                        break;

                    default:
                        throw new NotSupportedException($"Unsupported update operator: {updateOperator}");
                }

                operations.Add(operation);
            }
        }

        return string.Join(" || ", operations);
    }

    private static string SanitizeStringValue(string input)
    {
        return input.Replace("'", "''");
    }
}
