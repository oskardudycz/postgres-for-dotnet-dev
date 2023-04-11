using System.Collections;
using System.Drawing;
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace PostgresForDotnetDev.Pongo;

public static class UpdateDefinitionParser
{
    public static string ToSqlExpression<T>(this UpdateDefinition<T> update)
    {
        // Use a BsonDocument to access MongoDB update elements
        var bsonUpdate = update.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(),
            BsonSerializer.SerializerRegistry);
        return BsonUpdateToSqlExpression(bsonUpdate.AsBsonDocument);
    }

    private static string GenerateJsonValue(object? value)
    {
        while (true)
        {
            switch (value)
            {
                case null:
                    return "null";
                case string:
                    return $"'\"{SanitizeStringValue(value.ToString()!)}\"'";
                case bool boolValue:
                    return boolValue ? "true" : "false";
                case int or long or float or double or decimal:
                    return value.ToString()!;
                case IEnumerable enumerable:
                {
                    var items = enumerable.Cast<object>()
                        .Select(GenerateJsonValue);

                    return $"[{string.Join(",", items)}]";
                }
                case BsonString bsonString:
                    value = bsonString.ToString();
                    continue;
                default:
                    throw new ArgumentException($"Unsupported value type: {value.GetType()}");
            }
        }
    }

    private static string BsonUpdateToSqlExpression(BsonDocument bsonUpdate)
    {
        var operations = bsonUpdate.SelectMany(element =>
        {
            var updateOperator = element.Name;
            var updateFields = element.Value.AsBsonDocument;

            return updateFields.Select(updateField => UpdateField(updateField, updateOperator));
        });

        return string.Join(" || ", operations);
    }

    private static string UpdateField(BsonElement updateField, string updateOperator)
    {
        var field = updateField.Name;
        var value = updateField.Value;

        switch (updateOperator)
        {
            case "$set":
                return $"jsonb_set(data, '{{{field}}}', {GenerateJsonValue(value)}, true)";

            case "$unset":
                return $"data - '{field}'";

            case "$inc":
                return
                    $"jsonb_set(data, '{{{field}}}', (COALESCE((data->>'{field}')::numeric, 0) + {value})::text::jsonb, true)";

            case "$mul":
                return
                    $"jsonb_set(data, '{{{field}}}', (COALESCE((data->>'{field}')::numeric, 1) * {value})::text::jsonb, true)";

            case "$min":
                return
                    $"jsonb_set(data, '{{{field}}}', LEAST(COALESCE((data->>'{field}')::numeric, {value}), {value})::text::jsonb, true)";

            case "$max":
                return
                    $"jsonb_set(data, '{{{field}}}', GREATEST(COALESCE((data->>'{field}')::numeric, {value}), {value})::text::jsonb, true)";

            case "$currentDate":
                return value.BsonType == BsonType.Document && value.AsBsonDocument.Contains("$type") &&
                       value.AsBsonDocument["$type"].AsString == "timestamp"
                    ? $"jsonb_set(data, '{{{field}}}', to_jsonb(CURRENT_TIMESTAMP), true)"
                    : $"jsonb_set(data, '{{{field}}}', to_jsonb(CURRENT_DATE), true)";

            case "$push":
                if (value.IsBsonDocument && value.AsBsonDocument.Contains("$each"))
                {
                    var eachArray = value["$each"].AsBsonArray;
                    var elements = eachArray.Select(el => el.ToString()!).ToList();
                    var elementsJson = string.Join(", ", elements);
                    return
                        $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{elementsJson}]'::jsonb, true)";
                }
                return
                    $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{GenerateJsonValue(value)}]'::jsonb, true)";


            case "$addToSet":
                if (value.IsBsonDocument && value.AsBsonDocument.Contains("$each"))
                {
                    var eachArray = value["$each"].AsBsonArray;
                    var elements = eachArray.Select(el => SanitizeStringValue(el.ToString()!)).ToList();

                    var elementsJson = string.Join(", ", elements);
                    return
                        $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{elementsJson}]'::jsonb) - 'null', true)";
                }

                return
                    $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{GenerateJsonValue(value)}]'::jsonb) - 'null', true)";

            case "$pop":
                var popValue = value.ToInt32();
                return popValue > 0
                    ? $"jsonb_set(data, '{{{field}}}', data->'{field}' - (jsonb_array_length(data->'{field}') - 1), true)"
                    : $"jsonb_set(data, '{{{field}}}', data->'{field}' - 0, true)";

            case "$pull":
                return $"jsonb_set(data, '{{{field}}}', data->'{field}' - '{GenerateJsonValue(value)}', true)";

            case "$pullAll":
                var pullAllArray = value.AsBsonArray;
                var pullAllConditions = pullAllArray.Select(el => GenerateJsonValue(el.ToString()!))
                    .Select(sanitizedElement => $"data->'{field}' - '{sanitizedElement}'").ToList();

                return $"jsonb_set(data, '{{{field}}}', {string.Join(" || ", pullAllConditions)}, true)";

            default:
                throw new NotSupportedException($"Unsupported update operator: {updateOperator}");
        }
    }

    private static string SanitizeStringValue(string input)
    {
        return input.Replace("'", "''");
    }
}
