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
                        operation = $"jsonb_set(data, '{{{field}}}', {GenerateJsonValue(value)}, true)";
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
                                $"jsonb_set(data, '{{{field}}}', COALESCE(data->'{field}', '[]'::jsonb) || '[{GenerateJsonValue(value)}]'::jsonb, true)";
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
                            operation =
                                $"jsonb_set(data, '{{{field}}}', (COALESCE(data->'{field}', '[]'::jsonb) || '[{GenerateJsonValue(value)}]'::jsonb) - 'null', true)";
                        }

                        break;

                    case "$pop":
                        var popValue = value.ToInt32();
                        operation = popValue > 0
                            ? $"jsonb_set(data, '{{{field}}}', data->'{field}' - (jsonb_array_length(data->'{field}') - 1), true)"
                            : $"jsonb_set(data, '{{{field}}}', data->'{field}' - 0, true)";

                        break;

                    case "$pull":
                        operation = $"jsonb_set(data, '{{{field}}}', data->'{field}' - '{GenerateJsonValue(value)}', true)";
                        break;

                    case "$pullAll":
                        var pullAllArray = value.AsBsonArray;
                        var pullAllConditions = pullAllArray.Select(el => GenerateJsonValue(el.ToString()!))
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

public class SqlExpression: Expression
{
    public string Sql { get; }

    public SqlExpression(string sql, Type? type = null)
    {
        Sql = sql;
        Type = type ?? typeof(string);
    }

    public override Type Type { get; }

    public override ExpressionType NodeType => ExpressionType.Extension;

    public override string ToString() => Sql;
}
