using System.Text.Json;
using MongoDB.Driver;
using Npgsql;
using PostgresForDotnetDev.Core;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDev.Pongo;

public class PongoCollection<T>: IPongoCollection<T>
{
    private readonly NpgsqlConnection connection;
    private readonly string tableName;

    public PongoCollection(
        NpgsqlConnection connection,
        string tableName
    )
    {
        this.connection = connection;
        this.tableName = tableName;

        this.connection.CreateTableIfNotExists<T>(this.tableName);
    }

    public async Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(
        FilterDefinition<T> filter,
        FindOptions<T, TProjection>? options = null,
        string? encryptionKey = null,
        CancellationToken cancellationToken = default)
    {
        var whereClause =
            filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.

        var sql = encryptionKey != null
            ? $"SELECT pgp_sym_decrypt(data, @EncryptionKey) FROM {tableName} WHERE {whereClause}"
            : $"SELECT data FROM {tableName} WHERE {whereClause}";

        await using var command = new NpgsqlCommand(sql, connection);

        // Add parameters for location-based queries
        if (filter is NearFilterOperator nearFilter)
        {
            command.Parameters.AddWithValue("Longitude", nearFilter.Point.X);
            command.Parameters.AddWithValue("Latitude", nearFilter.Point.Y);
            command.Parameters.AddWithValue("Distance", nearFilter.Distance);
        }

        if (encryptionKey != null)
        {
            command.Parameters.AddWithValue("EncryptionKey", encryptionKey);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var results = new List<TProjection>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var jsonString = reader.GetString(0);
            var item = JsonSerializer.Deserialize<TProjection>(jsonString)!;
            results.Add(item);
        }

        return default!; //new AsyncCursor<TProjection>(results);
    }

    public async Task InsertOneAsync(
        T document,
        InsertOneOptions? options = null,
        string? encryptionKey = null,
        CancellationToken cancellationToken = default)
    {
        var jsonString = JsonSerializer.Serialize(document);

        var query =
            encryptionKey == null
                ? $@"
                    INSERT INTO {tableName} (data)
                    VALUES ('{jsonString}'::jsonb || jsonb_build_object('_id', uuid_generate_v4()))
                    RETURNING id, data;
                "
                : @"
                    INSERT INTO {_tableName} (data)
                    VALUES (pgp_sym_encrypt('{jsonString}'::jsonb || jsonb_build_object('_id', uuid_generate_v4()), @EncryptionKey))
                    RETURNING id, data;
                ";


        await using var command = new NpgsqlCommand(query, connection);
        if (encryptionKey != null)
        {
            command.Parameters.AddWithValue("EncryptionKey", encryptionKey);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        // if (await reader.ReadAsync(cancellationToken))
        // {
        //     var insertedData = reader.GetString(1);
        //     document = JsonSerializer.Deserialize<T>(insertedData);
        // }
    }

    public async Task<UpdateResult> UpdateOneAsync(
        FilterDefinition<T> filter,
        UpdateDefinition<T> update,
        UpdateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var whereClause =
            filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.
        var updateExpression =
            update.ToSqlExpression(); // You need to implement this method to convert the update to an SQL expression.
        var sql = $"UPDATE {tableName} SET data = data || @update::jsonb WHERE {whereClause}";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("update", updateExpression);
        var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

        return new UpdateResult.Acknowledged(rowsAffected, rowsAffected, default);
    }

    public Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<T> filter, T replacement,
        ReplaceOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<DeleteResult> DeleteOneAsync(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}
