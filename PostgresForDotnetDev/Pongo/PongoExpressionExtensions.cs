using System.Drawing;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;
using ExpressionVisitor = System.Linq.Expressions.ExpressionVisitor;

namespace PostgresForDotnetDev.Pongo;

public static class PongoExpressionExtensions
{
    public static string ToSqlExpression<T>(this FilterDefinition<T> filter)
    {
        if (filter is NearFilterOperator nearFilter)
        {
            var locationColumnName = nearFilter.PropertyName;
            return
                $"ST_DWithin({locationColumnName}, ST_SetSRID(ST_MakePoint(@Longitude, @Latitude), 4326), @Distance)";
        }

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
        var expressionVisitor = new CustomExpressionVisitor(tableName);
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

    public static string ProcessLogicalOperator(string operatorName, BsonArray subfilters)
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

public class CustomExpressionVisitor: ExpressionVisitor
{
    private readonly string tableName;

    public CustomExpressionVisitor(string tableName)
    {
        this.tableName = tableName;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        object? left;
        switch (node.Left)
        {
            case MemberExpression:
            case MethodCallExpression:
                left = Visit(node.Left);
                break;
            case ConstantExpression constantLeft:
                left = constantLeft.Value;
                break;
            default:
                left = Expression.Lambda(node.Left).Compile().DynamicInvoke()!;
                break;
        }

        object? right;
        switch (node.Right)
        {
            case MemberExpression:
            case MethodCallExpression:
                right = Visit(node.Right);
                break;
            case ConstantExpression constantRight:
                right = constantRight.Value;
                break;
            default:
                right = Expression.Lambda(node.Right).Compile().DynamicInvoke()!;
                break;
        }

        var op = node.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"The binary operator '{node.NodeType}' is not supported")
        };


        // Build the SQL expression
        string leftSql = left is SqlExpression expression1 ? expression1.Sql : FormatConstant(left);
        string rightSql = right is SqlExpression expression ? expression.Sql : FormatConstant(right);
        string sqlExpression = $"{leftSql} {op} {rightSql}";
        return new SqlExpression(sqlExpression, typeof(bool));
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        return new SqlExpression($"'{tableName}.{node.Member.Name}'");
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        return new SqlExpression(FormatConstant(node.Value));
    }

    private static string FormatConstant(object? value)
    {
        return value switch
        {
            null => "NULL",
            string => $"'{value}'",
            TimeSpan => $"'{value:G}'",
            DateTime date => $"'{date.ToString("yyyy-MM-ddTHH:mm:ss'Z'")}'",
            DateTimeOffset date => date.Offset == TimeSpan.Zero
                ? $"'{date.ToString("yyyy-MM-ddTHH:mm:ss'Z'")}'"
                : $"'{date.ToString("yyyy-MM-ddTHH:mm:sszzz")}'",
            _ => value.ToString()!
        };
    }

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        var body = Visit(node.Body);
        return Expression.Lambda(body, node.Parameters);
    }

    public static string FormatTimeSpanForTimescaleDb(TimeSpan timeSpan)
    {
        StringBuilder builder = new StringBuilder();

        void AppendIfNonZero(int value, string unit)
        {
            if (value > 0)
            {
                builder.Append($"{value} {unit}{(value > 1 ? "s" : "")} ");
            }
        }

        AppendIfNonZero(timeSpan.Days, "day");
        AppendIfNonZero(timeSpan.Hours, "hour");
        AppendIfNonZero(timeSpan.Minutes, "minute");
        AppendIfNonZero(timeSpan.Seconds, "second");

        return builder.ToString().TrimEnd();
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType != typeof(TimescaleDbExtensions)) return base.VisitMethodCall(node);
        var expression = Visit(node.Arguments[0]);

        TimeSpan interval;
        switch (node.Method.Name)
        {
            case nameof(TimescaleDbExtensions.TimeBucket):
            {
                interval = node.Arguments[1] switch
                {
                    ConstantExpression constantExpression when constantExpression.Type == typeof(TimeSpan) =>
                        (TimeSpan)constantExpression.Value!,
                    MemberExpression { Member: FieldInfo fieldInfo } memberExpression when fieldInfo.FieldType == typeof(TimeSpan) =>
                        (TimeSpan)fieldInfo.GetValue(((ConstantExpression)memberExpression.Expression!).Value)!,
                    _ => throw new InvalidOperationException("Unexpected expression type.")
                };

                return new SqlExpression($"time_bucket(INTERVAL '{FormatTimeSpanForTimescaleDb(interval)}', {expression})");
            }
            case nameof(TimescaleDbExtensions.TimeBucketGapFill):
                var start = Visit(node.Arguments[2]);
                var end = Visit(node.Arguments[3]);
                interval = node.Arguments[1] switch
                {
                    ConstantExpression constantExpression when constantExpression.Type == typeof(TimeSpan) =>
                        (TimeSpan)constantExpression.Value!,
                    MemberExpression { Member: FieldInfo fieldInfo } memberExpression when fieldInfo.FieldType == typeof(TimeSpan) =>
                        (TimeSpan)fieldInfo.GetValue(((ConstantExpression)memberExpression.Expression!).Value)!,
                    _ => throw new InvalidOperationException("Unexpected expression type.")
                };
                return new SqlExpression(
                    $"time_bucket_gapfill(INTERVAL '{FormatTimeSpanForTimescaleDb(interval)}', {expression}, {start}, {end})");
            case nameof(TimescaleDbExtensions.First):
                var firstTimeColumn = Visit(node.Arguments[1]);
                return new SqlExpression($"first({expression}, {firstTimeColumn})");
            case nameof(TimescaleDbExtensions.Last):
                var lastTimeColumn = Visit(node.Arguments[1]);
                return new SqlExpression($"last({expression}, {lastTimeColumn})");
            case nameof(TimescaleDbExtensions.Lag):
                var lagStep = Visit(node.Arguments[1]);
                return new SqlExpression($"lag({expression}, {lagStep})");
            case nameof(TimescaleDbExtensions.Lead):
                var leadStep = Visit(node.Arguments[1]);
                return new SqlExpression($"lead({expression}, {leadStep})");
            case nameof(TimescaleDbExtensions.DateTrunc):
                var truncationUnit = (string)Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke()!;
                var timestamp = Visit(node.Arguments[0]);

                return timestamp is not SqlExpression timestampSqlExpression
                    ? throw new NotSupportedException("The argument for the DateTrunc method must be an SqlExpression")
                    : new SqlExpression($"date_trunc('{truncationUnit}', {timestampSqlExpression.Sql})");
            default:
                return base.VisitMethodCall(node);
        }
    }
}
