using System.Linq.Expressions;
using System.Reflection;
using PostgresForDotnetDev.TimescaleDB;

namespace PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

public static class TimescaleDbFunctionsVisitor
{
    public static Expression? VisitMethodCall(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);

        TimeSpan interval;
        switch (node.Method.Name)
        {
            case nameof(TimescaleDbFunctions.TimeBucket):
            {
                interval = node.Arguments[1] switch
                {
                    ConstantExpression constantExpression when constantExpression.Type == typeof(TimeSpan) =>
                        (TimeSpan)constantExpression.Value!,
                    MemberExpression { Member: FieldInfo fieldInfo } memberExpression when fieldInfo.FieldType ==
                        typeof(TimeSpan) =>
                        (TimeSpan)fieldInfo.GetValue(((ConstantExpression)memberExpression.Expression!).Value)!,
                    _ => throw new InvalidOperationException("Unexpected expression type.")
                };

                return new SqlExpression(
                    $"time_bucket(INTERVAL '{interval.FormatToTimescaleDB()}', {expression})");
            }
            case nameof(TimescaleDbFunctions.TimeBucketGapFill):
                var start = visit(node.Arguments[2]);
                var end = visit(node.Arguments[3]);
                interval = node.Arguments[1] switch
                {
                    ConstantExpression constantExpression when constantExpression.Type == typeof(TimeSpan) =>
                        (TimeSpan)constantExpression.Value!,
                    MemberExpression { Member: FieldInfo fieldInfo } memberExpression when fieldInfo.FieldType ==
                        typeof(TimeSpan) =>
                        (TimeSpan)fieldInfo.GetValue(((ConstantExpression)memberExpression.Expression!).Value)!,
                    _ => throw new InvalidOperationException("Unexpected expression type.")
                };
                return new SqlExpression(
                    $"time_bucket_gapfill(INTERVAL '{interval.FormatToTimescaleDB()}', {expression}, {start}, {end})");
            case nameof(TimescaleDbFunctions.First):
                var firstTimeColumn = visit(node.Arguments[1]);
                return new SqlExpression($"first({expression}, {firstTimeColumn})");
            case nameof(TimescaleDbFunctions.Last):
                var lastTimeColumn = visit(node.Arguments[1]);
                return new SqlExpression($"last({expression}, {lastTimeColumn})");
            case nameof(TimescaleDbFunctions.Lag):
                var lagStep = visit(node.Arguments[1]);
                return new SqlExpression($"lag({expression}, {lagStep})");
            case nameof(TimescaleDbFunctions.Lead):
                var leadStep = visit(node.Arguments[1]);
                return new SqlExpression($"lead({expression}, {leadStep})");
            case nameof(TimescaleDbFunctions.DateTrunc):
                var truncationUnit = (string)Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke()!;
                var timestamp = visit(node.Arguments[0]);

                return timestamp is not SqlExpression timestampSqlExpression
                    ? throw new NotSupportedException("The argument for the DateTrunc method must be an SqlExpression")
                    : new SqlExpression($"date_trunc('{truncationUnit}', {timestampSqlExpression.Sql})");
        }

        return null;
    }

    public static TimeSpan GetIntervalFromArgument(this Expression argumentExpression)
    {
        switch (argumentExpression)
        {
            case ConstantExpression constantExpression when constantExpression.Type == typeof(TimeSpan):
                return (TimeSpan)constantExpression.Value!;
            case MemberExpression { Member: FieldInfo fieldInfo } memberExpression when
                fieldInfo.FieldType == typeof(TimeSpan):
            {
                var constantExpression = (ConstantExpression)memberExpression.Expression!;
                var instance = constantExpression.Value;
                var value = (TimeSpan)fieldInfo.GetValue(instance)!;
                return value;
            }
            default:
                throw new ArgumentException("Invalid argument type for interval.");
        }
    }
}

public interface ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit);
}

public class TimeBucketFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);

        var interval = node.Arguments[1].GetIntervalFromArgument();

        return new SqlExpression(
            $"time_bucket(INTERVAL '{interval.FormatToTimescaleDB()}', {expression})");
    }
}

public class TimeBucketGapFillFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);

        var interval = TimescaleDbFunctionsVisitor.GetIntervalFromArgument(node.Arguments[1]);

        var start = visit(node.Arguments[2]);
        var end = visit(node.Arguments[3]);

        return new SqlExpression(
            $"time_bucket_gapfill(INTERVAL '{interval.FormatToTimescaleDB()}', {expression}, {start}, {end})");
    }
}

public class FirstFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var firstTimeColumn = visit(node.Arguments[1]);

        return new SqlExpression($"first({expression}, {firstTimeColumn})");
    }
}

public class LastFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var lastTimeColumn = visit(node.Arguments[1]);

        return new SqlExpression($"last({expression}, {lastTimeColumn})");
    }
}

public class LagFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var lagStep = visit(node.Arguments[1]);

        return new SqlExpression($"lag({expression}, {lagStep})");
    }
}

public class LeadFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var leadStep = visit(node.Arguments[1]);

        return new SqlExpression($"lead({expression}, {leadStep})");
    }
}

public class DateTruncFunction: ITimescaleDbFunction
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var truncationUnit = (string)Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke()!;
        var timestamp = visit(node.Arguments[0]);

        return timestamp is not SqlExpression timestampSqlExpression
            ? throw new NotSupportedException("The argument for the DateTrunc method must be an SqlExpression")
            : new SqlExpression($"date_trunc('{truncationUnit}', {timestampSqlExpression.Sql})");
    }
}

public class CompositeTimescaleDbFunction: ITimescaleDbFunction
{
    private static readonly Dictionary<string, ITimescaleDbFunction> Functions = new()
    {
        { nameof(TimescaleDbFunctions.TimeBucket), new TimeBucketFunction() },
        { nameof(TimescaleDbFunctions.TimeBucketGapFill), new TimeBucketGapFillFunction() },
        { nameof(TimescaleDbFunctions.First), new FirstFunction() },
        { nameof(TimescaleDbFunctions.Last), new LastFunction() },
        { nameof(TimescaleDbFunctions.Lag), new LagFunction() },
        { nameof(TimescaleDbFunctions.Lead), new LeadFunction() },
        { nameof(TimescaleDbFunctions.DateTrunc), new DateTruncFunction() },
    };

    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit) =>
        Functions.TryGetValue(node.Method.Name, out var value) ? value.Visit(node, visit) : null;
}
