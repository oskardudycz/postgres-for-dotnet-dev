using System.Text.Json;
using MongoDB.Driver;
using Npgsql;
using PostgresForDotnetDev.Core;
using PostgresForDotnetDev.Pongo.Collections;
using PostgresForDotnetDev.Pongo.Filtering;

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
                    INSERT INTO {tableName} (id, data)
                    WITH uuid_generator AS (SELECT uuid_generate_v4() AS uuid)
                    SELECT uuid, ('{jsonString}'::jsonb || jsonb_build_object('_id', uuid)) FROM uuid_generator
                    RETURNING id, data;
                "
                : @"
                    INSERT INTO {_tableName} (data)
                    WITH uuid_generator AS (SELECT uuid_generate_v4() AS uuid)
                    SELECT uuid, (pgp_sym_encrypt('{jsonString}'::jsonb || jsonb_build_object('_id', uuid), @EncryptionKey)) FROM uuid_generator
                    RETURNING id;
                ";


        await using var command = new NpgsqlCommand(query, connection);
        if (encryptionKey != null)
        {
            command.Parameters.AddWithValue("EncryptionKey", encryptionKey);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            var newId = reader.GetString(0);

            document!.GetType().GetProperty("_id")?.SetValue(document, newId);
        }
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
        var sql = $"UPDATE {tableName} SET data = data || {updateExpression}::jsonb WHERE {whereClause}";

        await using var command = new NpgsqlCommand(sql, connection);

        var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

        return new UpdateResult.Acknowledged(rowsAffected, rowsAffected, default);
    }

    public async Task<DeleteResult> DeleteOneAsync(
        FilterDefinition<T> filter,
        CancellationToken cancellationToken = default)
    {
        var whereClause =
            filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.
        var sql = $"DELETE FROM {tableName} WHERE {whereClause}";

        await using var command = new NpgsqlCommand(sql, connection);

        var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

        return new DeleteResult.Acknowledged(rowsAffected);
    }

    public IQueryable<T> AsQueryable() =>
        new PongoQueryable<T>(
            new PongoQueryableProvider(new PongoQueryableExecutor(connection, PongoCollectionName.For))
        );
}
