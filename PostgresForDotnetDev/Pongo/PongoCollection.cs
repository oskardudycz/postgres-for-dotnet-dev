using MongoDB.Bson.IO;
using MongoDB.Driver;
using MongoDB.Driver.Core.Operations;
using Npgsql;

namespace PostgresForDotnetDev.Pongo;

public class PongoCollection<T>: IPongoCollection<T>
{
    private readonly string _connectionString;
    private readonly string _tableName;
    //private readonly JsonSerializerSettings _jsonSerializerSettings;

    public PongoCollection(string connectionString, string tableName)//, JsonSerializerSettings jsonSerializerSettings = null)
    {
        _connectionString = connectionString;
        _tableName = tableName;
        //_jsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings();
    }

    public async Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(
        FilterDefinition<T> filter,
        FindOptions<T, TProjection>? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var whereClause = filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.
        var sql = $"SELECT data FROM {_tableName} WHERE {whereClause}";

        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var results = new List<TProjection>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var jsonString = reader.GetString(0);
            var item =default(TProjection)!;//JsonConvert.DeserializeObject<TProjection>(jsonString, _jsonSerializerSettings);
            results.Add(item);
        }

        return new AsyncCursor<TProjection>(results);
    }

    public async Task InsertOneAsync(
        T document,
        InsertOneOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var jsonString = "";//JsonConvert.SerializeObject(document, _jsonSerializerSettings);
        var sql = $"INSERT INTO {_tableName} (data) VALUES (@data::jsonb)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("data", jsonString);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<UpdateResult> UpdateOneAsync(
        FilterDefinition<T> filter,
        UpdateDefinition<T> update,
        UpdateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var whereClause = filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.
        var updateExpression = update.ToSqlExpression(); // You need to implement this method to convert the update to an SQL expression.
        var sql = $"UPDATE {_tableName} SET data = data || @update::jsonb WHERE {whereClause}";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("update", updateExpression);
        var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

        return new UpdateResult.Acknowledged(rowsAffected, rowsAffected, default);
    }

    public Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<T> filter, T replacement, ReplaceOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<DeleteResult> DeleteOneAsync(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}
