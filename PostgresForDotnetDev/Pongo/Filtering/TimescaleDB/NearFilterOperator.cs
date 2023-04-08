using System.Drawing;
using System.Linq.Expressions;

namespace PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

public class NearFilterOperator
{
    public string PropertyName { get; set; }
    public Point Point { get; set; }
    public double Distance { get; set; }

    public NearFilterOperator(string propertyName, Point point, double distance)
    {
        PropertyName = propertyName;
        Point = point;
        Distance = distance;
    }
}

public enum FilterOperator
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    TimeBucket
}

public class FilterCondition<T>
{
    public Expression<Func<T, bool>> Predicate { get; set; } = default!;
}

public class CustomFilter<T>
{
    public List<FilterCondition<T>> Conditions { get; set; } = default!;
}

public static class TimescaleDbExtensions
{
    public static DateTimeOffset TimeBucket(this DateTimeOffset dateTimeOffset, TimeSpan interval)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    public static DateTimeOffset TimeBucketGapFill(this DateTimeOffset dateTimeOffset, TimeSpan interval, DateTimeOffset start, DateTimeOffset end)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    public static T First<T>(this T value, DateTimeOffset timeColumn)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    public static T Last<T>(this T value, DateTimeOffset timeColumn)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    public static T Lag<T>(this T value, int step)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    public static T Lead<T>(this T value, int step)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    public static DateTimeOffset DateTrunc(this DateTimeOffset dateTimeOffset, string field)
    {
        throw new NotSupportedException("This method should not be called directly. It should only be used in expression trees.");
    }

    // Add other TimescaleDB operators as needed
}

