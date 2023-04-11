namespace PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

public static class TimescaleDbFunctions
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
