using System.Linq.Expressions;
using PostgresForDotnetDev.Core.Expressions;
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
        var visitor = new QueryExpressionVisitor("my_table", new TimeScaleOperatorVisitor());
        var result = visitor.Visit(filter) as LambdaExpression;
        var sqlExpression = result?.Body as SqlExpression;
        Assert.Equal("my_table.\"data\"->>'Name' = 'value1'", sqlExpression?.ToString());
    }

    [Fact]
    public void TestGreaterThanOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.Value > 3.0;
        var visitor = new QueryExpressionVisitor("my_table", new TimeScaleOperatorVisitor());
        var result = visitor.Visit(filter) as LambdaExpression;
        var sqlExpression = result?.Body as SqlExpression;
        Assert.Equal("my_table.\"data\"->>'Value' > 3", sqlExpression?.ToString());
    }

    [Fact]
    public void TestLessThanOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.Id < 10;
        var visitor = new QueryExpressionVisitor("my_table", new TimeScaleOperatorVisitor());
        var result = visitor.Visit(filter) as LambdaExpression;
        var sqlExpression = result?.Body as SqlExpression;
        Assert.Equal("my_table.\"data\"->>'Id' < 10", sqlExpression?.ToString());
    }

    [Fact]
    public void TestDateTruncOperator()
    {
        Expression<Func<TestDocument, bool>> filter = doc => doc.CreatedAt.DateTrunc("day") == new DateTime(2023, 1, 1);
        var visitor = new QueryExpressionVisitor("my_table", new TimeScaleOperatorVisitor());
        var result = visitor.Visit(filter) as LambdaExpression;
        var sqlExpression = result?.Body as SqlExpression;
        Assert.Equal("date_trunc('day', my_table.\"data\"->>'CreatedAt') = '2023-01-01T00:00:00Z'",
            sqlExpression?.ToString());
    }

    [Fact]
    public void TestTimeBucketOperator()
    {
        var interval = TimeSpan.FromHours(1);
        Expression<Func<TestDocument, bool>> filter = doc =>
            doc.CreatedAt.TimeBucket(interval) == new DateTimeOffset(new DateTime(2023, 1, 1, 1, 0, 0), TimeSpan.Zero);
        var visitor = new QueryExpressionVisitor("my_table", new TimeScaleOperatorVisitor());
        var result = visitor.Visit(filter) as LambdaExpression;
        var sqlExpression = result?.Body as SqlExpression;
        Assert.Equal("time_bucket(INTERVAL '1 hour', my_table.\"data\"->>'CreatedAt') = '2023-01-01T01:00:00Z'",
            sqlExpression?.ToString());
    }
}
