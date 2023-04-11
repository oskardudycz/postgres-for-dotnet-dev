using Dapper;
using FluentAssertions;
using MongoDB.Driver;
using Npgsql;
using PostgresForDotnetDev.Pongo;
using PostgresForDotnetDevs.Tests.Core;

namespace PostgresForDotnetDevs.Tests.Pongo;

public class TestData
{
    public string _id { get; set; } = default!;
    public string Name { get; set; } = default!;
}

public class PostgresJsonCollectionTests
{
    private readonly string connectionString;
    private readonly PostgresSchemaProvider schemaProvider;
    private readonly NpgsqlConnection connection;

    public PostgresJsonCollectionTests()
    {
        connection = PostgresConnectionProvider.GetFreshDbConnection();
        connectionString = PostgresConnectionProvider.GetFreshConnectionString();
        schemaProvider = new PostgresSchemaProvider(connection);
    }

    [Fact]
    public void GettingCollection_SetsUpTable()
    {
        const string expectedTableName = "postgresfordotnetdevs_tests_pongo_testdata";

        var client = new PongoClient(connectionString);
        var database = client.GetDatabase();
        database.GetCollection<TestData>();

        var collectionTable = schemaProvider.GetTable("postgresfordotnetdevs_tests_pongo_testdata");

        collectionTable.Should().NotBeNull();
        collectionTable!.Name.Should().Be(expectedTableName);
    }

    [Fact]
    public async Task InsertOneAsync_ShouldInsertDocument()
    {
        const string tableName = "postgresfordotnetdevs_tests_pongo_testdata";
        // Arrange
        var client = new PongoClient(connectionString);
        var database = client.GetDatabase();
        var collection = database.GetCollection<TestData>();
        var document = new TestData { Name = "Test Document" };

        // Act
        await collection.InsertOneAsync(document);

        // Assert
        Assert.NotNull(document._id);

        var query = $"SELECT COUNT(*) FROM {tableName} WHERE data->>'_id' = '{document._id}';";
        var count = connection.QuerySingle<long>(query, connection);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task FindOneAsync_ShouldReturnDocument()
    {
        // Arrange
        var client = new PongoClient(connectionString);
        var database = client.GetDatabase();
        var collection = database.GetCollection<TestData>();
        var document = new TestData { Name = "Test Document" };
        await collection.InsertOneAsync(document);

        //Act
        var foundDocument = collection.AsQueryable().FirstOrDefault(x => x._id == document._id);
        var foundDocument2 = collection.AsQueryable().ToList();
        var foundDocument3 = collection.AsQueryable().ToArray();

        // Assert
        Assert.NotNull(foundDocument);
        Assert.NotEmpty(foundDocument2);
        Assert.NotEmpty(foundDocument3);
        Assert.Equal(document._id, foundDocument._id);
        Assert.Equal(document.Name, foundDocument.Name);
    }

    [Fact]
    public async Task UpdateOneAsync_ShouldUpdateDocument()
    {
        const string tableName = "postgresfordotnetdevs_tests_pongo_testdata";
        // Arrange
        var client = new PongoClient(connectionString);
        var database = client.GetDatabase();
        var collection = database.GetCollection<TestData>();
        var document = new TestData { Name = "Test Document" };

        await collection.InsertOneAsync(document);

        var filter = Builders<TestData>.Filter.Eq(x => x._id, document._id);
        var update = Builders<TestData>.Update.Set(x => x.Name, "Updated Document");

        // Act
        var updateResult = await collection.UpdateOneAsync(filter, update);

        // Assert
        Assert.NotNull(document._id);
        Assert.Equal(1, updateResult.ModifiedCount);

        var query = $"SELECT data->>'Name' as Name FROM {tableName} WHERE data->>'_id' = '{document._id}';";
        var updatedName = connection.QuerySingle<string>(query, connection);

        Assert.Equal("Updated Document", updatedName);
    }

}
