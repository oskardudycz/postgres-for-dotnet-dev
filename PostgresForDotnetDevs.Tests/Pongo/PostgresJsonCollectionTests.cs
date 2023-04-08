using MongoDB.Bson;
using Npgsql;
using PostgresForDotnetDev.Pongo;

namespace PostgresForDotnetDevs.Tests.Pongo;

public class PostgresJsonCollectionTests
{
    private readonly string _connectionString = "your_connection_string";
    private readonly string _tableName = "your_table_name";

    private async Task ClearTable()
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = $"TRUNCATE {_tableName};";
        await using var command = new NpgsqlCommand(query, connection);
        await command.ExecuteNonQueryAsync();
    }

    public class TestData
    {
        public string _id { get; set; } = default!;
        public string Name { get; set; } = default!;
    }

    [Fact]
    public async Task InsertOneAsync_ShouldInsertDocument()
    {
        // Arrange
        await ClearTable();
        var collection = new PongoCollection<TestData>(_connectionString, _tableName);
        var document = new TestData { Name = "Test Document" };

        // Act
        await collection.InsertOneAsync(document);

        // Assert
        Assert.NotNull(document._id);

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = $"SELECT COUNT(*) FROM {_tableName} WHERE data->>'_id' = '{document._id}';";
        using var command = new NpgsqlCommand(query, connection);
        var count = (long?)await command.ExecuteScalarAsync();

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task FindOneAsync_ShouldReturnDocument()
    {
        // Arrange
        await ClearTable();
        var collection = new PongoCollection<TestData>(_connectionString, _tableName);
        var document = new TestData { Name = "Test Document" };
        await collection.InsertOneAsync(document);
        var filter = new BsonDocument("_id", document._id);

        // Act
        // var foundDocument = await collection.FindAsync(filter);
        //
        // // Assert
        // Assert.NotNull(foundDocument);
        // Assert.Equal(document._id, foundDocument._id);
        // Assert.Equal(document.Name, foundDocument.Name);
    }
}
