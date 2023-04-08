using System.Linq.Expressions;
using PostgresForDotnetDev.Pongo;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDevs.Tests.Pongo.Filtering.TimescaleDB;

public class CustomExpressionVisitorTests
{
    public class TestDocument
    {
        public int Id { get; set; }
        public string Name { get; set; } = default!;
        public DateTimeOffset CreatedAt { get; set; }
        public double Value { get; set; }
    }

    [Fact]
    public void TestEqualOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.Name == "value1";
        var visitor = new CustomExpressionVisitor("my_table");
        var modifiedFilter = visitor.Visit(filter) as LambdaExpression;
        var result = modifiedFilter?.Body as SqlExpression;
        Assert.Equal("'my_table.Name' = 'value1'", result?.ToString());
    }

    [Fact]
    public void TestGreaterThanOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.Value > 3.0;
        var visitor = new CustomExpressionVisitor("my_table");
        var result = visitor.Visit(filter);
        Assert.Equal("'my_table.Value' > '3'", result.ToString());
    }

    [Fact]
    public void TestLessThanOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.Id < 10;
        var visitor = new CustomExpressionVisitor("my_table");
        var result = visitor.Visit(filter);
        Assert.Equal("'my_table.Id' < '10'", result.ToString());
    }

    [Fact]
    public void TestDateTruncOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.CreatedAt.DateTrunc("day") == new DateTime(2023, 1, 1);
        var visitor = new CustomExpressionVisitor("my_table");
        var result = visitor.Visit(filter);
        Assert.Equal("date_trunc('day', 'my_table.CreatedAt') = '1/1/2023 12:00:00 AM'", result.ToString());
    }

    [Fact]
    public void TestTimeBucketOperator()
    {
        TimeSpan interval = TimeSpan.FromHours(1);
        Expression<Func<TestDocument, bool>> filter = doc => doc.CreatedAt.TimeBucket(interval) == new DateTime(2023, 1, 1, 1, 0, 0);
        var visitor = new CustomExpressionVisitor("my_table");
        var result = visitor.Visit(filter);
        Assert.Equal("time_bucket(INTERVAL '1:00:00', 'my_table.CreatedAt') = '1/1/2023 1:00:00 AM'", result.ToString());
    }
}

