using System.Collections;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace PostgresForDotnetDev.Pongo;

public static class UpdateDefinitionParser
{
    public static string ToSqlExpression<T>(this UpdateDefinition<T> update)
    {
        var bsonUpdate = update.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(),
            BsonSerializer.SerializerRegistry);

        var operations = bsonUpdate.AsBsonDocument.SelectMany(element =>
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

        return updateOperator switch
        {
            "$set" => $"jsonb_set(data, '{{{field}}}', {GenerateJsonValue(value)}, true)",

            "$unset" => $"data - '{field}'",

            "$inc" =>
                $"jsonb_set(data, '{{{field}}}', (COALESCE((data->>'{field}')::numeric, 0) + {value})::text::jsonb, true)",

            "$mul" =>
                $"jsonb_set(data, '{{{field}}}', (COALESCE((data->>'{field}')::numeric, 1) * {value})::text::jsonb, true)",

            "$min" =>
                $"jsonb_set(data, '{{{field}}}', LEAST(COALESCE((data->>'{field}')::numeric, {value}), {value})::text::jsonb, true)",

            "$max" =>
                $"jsonb_set(data, '{{{field}}}', GREATEST(COALESCE((data->>'{field}')::numeric, {value}), {value})::text::jsonb, true)",

            "$currentDate" => value.BsonType == BsonType.Document && value.AsBsonDocument.Contains("$type") &&
                              value.AsBsonDocument["$type"].AsString == "timestamp"
                ? $"jsonb_set(data, '{{{field}}}', to_jsonb(CURRENT_TIMESTAMP), true)"
                : $"jsonb_set(data, '{{{field}}}', to_jsonb(CURRENT_DATE), true)",

            "$push" => (value.IsBsonDocument && value.AsBsonDocument.Contains("$each"))
                ? $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{EachToElements(value)}]'::jsonb, true)"
                : $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{GenerateJsonValue(value)}]'::jsonb, true)",


            "$addToSet" => (value.IsBsonDocument && value.AsBsonDocument.Contains("$each"))
                ? $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{EachToElements(value)}]'::jsonb) - 'null', true)"
                : $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{GenerateJsonValue(value)}]'::jsonb) - 'null', true)",

            "$pop" => value.ToInt32() > 0
                ? $"jsonb_set(data, '{{{field}}}', data->'{field}' - (jsonb_array_length(data->'{field}') - 1), true)"
                : $"jsonb_set(data, '{{{field}}}', data->'{field}' - 0, true)",

            "$pull" => $"jsonb_set(data, '{{{field}}}', data->'{field}' - '{GenerateJsonValue(value)}', true)",

            "$pullAll" =>
                $"jsonb_set(data, '{{{field}}}', {string.Join(" || ", PullAllConditions(value, field))}, true)",

            _ => throw new NotSupportedException($"Unsupported update operator: {updateOperator}"),
        };
    }

    private static string GenerateJsonValue(object? value) =>
        value switch
        {
            null => "null",

            string => $"'\"{SanitizeStringValue(value.ToString()!)}\"'",
            BsonString bsonString => $"'\"{SanitizeStringValue(bsonString.ToString()!)}\"'",

            bool boolValue => boolValue ? "true" : "false",

            int or long or float or double or decimal => value.ToString()!,

            IEnumerable enumerable =>
                $"[{string.Join(",", enumerable.Cast<object>()
                    .Select(GenerateJsonValue))}]",

            _ => throw new ArgumentException($"Unsupported value type: {value.GetType()}")
        };

    private static IEnumerable<string> PullAllConditions(BsonValue value, string field) =>
        value.AsBsonArray.Select(el => GenerateJsonValue(el.ToString()!))
            .Select(sanitizedElement => $"data->'{field}' - '{sanitizedElement}'");

    private static string EachToElements(BsonValue value)
    {
        var eachArray = value["$each"].AsBsonArray;
        var elements = eachArray.Select(el => el.ToString()!).ToList();
        var elementsJson = string.Join(", ", elements);

        return elementsJson;
    }

    private static string SanitizeStringValue(string input) =>
        input.Replace("'", "''");
}
