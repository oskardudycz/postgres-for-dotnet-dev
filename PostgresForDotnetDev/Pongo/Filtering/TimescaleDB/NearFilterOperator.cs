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
