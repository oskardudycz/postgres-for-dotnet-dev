using System.Linq.Expressions;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.TimescaleDB;

namespace PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

public class TimeBucketFunction: ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);

        var interval = node.Arguments[1].GetConstantArgument<TimeSpan>();

        return new SqlExpression(
            $"time_bucket(INTERVAL '{interval.FormatToTimescaleDB()}', {expression})");
    }
}

public class TimeBucketGapFillFunction: ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);

        var interval = node.Arguments[1].GetConstantArgument<TimeSpan>();

        var start = visit(node.Arguments[2]);
        var end = visit(node.Arguments[3]);

        return new SqlExpression(
            $"time_bucket_gapfill(INTERVAL '{interval.FormatToTimescaleDB()}', {expression}, {start}, {end})");
    }
}

public class FirstFunction: ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var firstTimeColumn = visit(node.Arguments[1]);

        return new SqlExpression($"first({expression}, {firstTimeColumn})");
    }
}

public class LastFunction: ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var lastTimeColumn = visit(node.Arguments[1]);

        return new SqlExpression($"last({expression}, {lastTimeColumn})");
    }
}

public class LagFunction: ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var lagStep = visit(node.Arguments[1]);

        return new SqlExpression($"lag({expression}, {lagStep})");
    }
}

public class LeadFunction: ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit)
    {
        var expression = visit(node.Arguments[0]);
        var leadStep = visit(node.Arguments[1]);

        return new SqlExpression($"lead({expression}, {leadStep})");
    }
}

public class DateTruncFunction: ICustomLinqOperatorVisitor
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

public class TimeScaleOperatorVisitor: CompositeCustomOperatorVisitor
{
    public TimeScaleOperatorVisitor():
        base(
            new Dictionary<string, ICustomLinqOperatorVisitor>
            {
                { nameof(TimescaleDbFunctions.TimeBucket), new TimeBucketFunction() },
                { nameof(TimescaleDbFunctions.TimeBucketGapFill), new TimeBucketGapFillFunction() },
                { nameof(TimescaleDbFunctions.First), new FirstFunction() },
                { nameof(TimescaleDbFunctions.Last), new LastFunction() },
                { nameof(TimescaleDbFunctions.Lag), new LagFunction() },
                { nameof(TimescaleDbFunctions.Lead), new LeadFunction() },
                { nameof(TimescaleDbFunctions.DateTrunc), new DateTruncFunction() },
            }
        )
    {
    }
}
