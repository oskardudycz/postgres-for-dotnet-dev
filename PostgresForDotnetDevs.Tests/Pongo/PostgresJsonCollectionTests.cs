using Dapper;
using FluentAssertions;
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
    //
    // [Fact]
    // public async Task FindOneAsync_ShouldReturnDocument()
    // {
    //     // Arrange
    //     await ClearTable();
    //     var collection = new PongoCollection<TestData>(_connectionString, _tableName);
    //     var document = new TestData { Name = "Test Document" };
    //     await collection.InsertOneAsync(document);
    //     var filter = new BsonDocument("_id", document._id);
    //
    //     // Act
    //     // var foundDocument = await collection.FindAsync(filter);
    //     //
    //     // // Assert
    //     // Assert.NotNull(foundDocument);
    //     // Assert.Equal(document._id, foundDocument._id);
    //     // Assert.Equal(document.Name, foundDocument.Name);
    // }
}
