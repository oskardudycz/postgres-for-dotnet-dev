using System.Drawing;
using System.Text.Json;
using MongoDB.Driver;
using Npgsql;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;

namespace PostgresForDotnetDev.Pongo;

public class PongoCollection<T>: IPongoCollection<T>
{
    private readonly string _connectionString;

    private readonly string _tableName;

    public PongoCollection(
        string connectionString,
        string tableName
    )
    {
        _connectionString = connectionString;
        _tableName = tableName;

        // Generate SQL for generated columns
        var generatedColumnsSql = GetGeneratedColumnsSql();

        if (generatedColumnsSql.Length > 0)
            generatedColumnsSql = "," + generatedColumnsSql;

        // Create the table if it does not exist
        var createTableSql = $@"
        CREATE TABLE IF NOT EXISTS {_tableName} (
            id SERIAL PRIMARY KEY,
            data JSONB
            {generatedColumnsSql}
        );";

        using var connection = new NpgsqlConnection(_connectionString);
        connection.Open();

        using var command = new NpgsqlCommand(createTableSql, connection);
        command.ExecuteNonQuery();
    }

    private static string GetGeneratedColumnsSql()
    {
        var generatedColumns = new List<string>();

        var properties = typeof(T).GetProperties();
        foreach (var property in properties)
        {
            if (property.PropertyType == typeof(Point))
            {
                generatedColumns.Add(
                    $"{property.Name} GEOMETRY GENERATED ALWAYS AS (ST_GeomFromGeoJSON(data ->> '{property.Name}')) STORED");
            }
            else if (property.PropertyType == typeof(DateTimeOffset))
            {
                generatedColumns.Add(
                    $"{property.Name} TIMESTAMPTZ GENERATED ALWAYS AS ((data ->> '{property.Name}')::TIMESTAMPTZ) STORED");
            }
            else if (property.PropertyType == typeof(List<Point>))
            {
                // Assuming that the list of points is stored as a GeoJSON FeatureCollection in the JSONB column
                generatedColumns.Add(
                    $"{property.Name} GEOMETRY GENERATED ALWAYS AS (ST_Collect(ST_GeomFromGeoJSON(feature ->> 'geometry'))) STORED FROM json_array_elements(data -> '{property.Name}' -> 'features') AS feature");
            }
            else if (property.PropertyType == typeof(List<DateTime>))
            {
                // Assuming that the list of timestamps is stored as an array of ISO 8601 formatted strings in the JSONB column
                generatedColumns.Add(
                    $"{property.Name} TIMESTAMP[] GENERATED ALWAYS AS (ARRAY(SELECT (elem ->> 'timestamp')::TIMESTAMPTZ FROM json_array_elements(data -> '{property.Name}') AS elem)) STORED");
            }
            // Add more conditions here for other property types
        }

        return string.Join(",", generatedColumns);
    }


    public async Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(
        FilterDefinition<T> filter,
        FindOptions<T, TProjection>? options = null,
        string? encryptionKey = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var whereClause =
            filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.


        var sql = encryptionKey != null
            ? $"SELECT pgp_sym_decrypt(data, @EncryptionKey) FROM {_tableName} WHERE {whereClause}"
            : $"SELECT data FROM {_tableName} WHERE {whereClause}";

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
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var query =
            encryptionKey == null
                ? $@"
                    INSERT INTO {_tableName} (data)
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
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var whereClause =
            filter.ToSqlExpression(); // You need to implement this method to convert the filter to an SQL expression.
        var updateExpression =
            update.ToSqlExpression(); // You need to implement this method to convert the update to an SQL expression.
        var sql = $"UPDATE {_tableName} SET data = data || @update::jsonb WHERE {whereClause}";

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
