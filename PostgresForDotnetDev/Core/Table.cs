using NetTopologySuite.Geometries;
using Npgsql;

namespace PostgresForDotnetDev.Core;

public static class Table
{
    public static void EnablePgCryptoExtension(this NpgsqlConnection connection) =>
        connection.Run("CREATE EXTENSION IF NOT EXISTS pgcrypto;");

    public static void EnableUuidOsspExtension(this NpgsqlConnection connection) =>
        connection.Run("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";");

    public static void CreateTableIfNotExists<T>(this NpgsqlConnection connection, string tableName)
    {
        // Generate SQL for generated columns
        var generatedColumnsSql = GetGeneratedColumnsSql<T>();

        if (generatedColumnsSql.Length > 0)
            generatedColumnsSql = "," + generatedColumnsSql;

        // Create the table if it does not exist
        connection.Run($@"
            CREATE TABLE IF NOT EXISTS {tableName} (
                id TEXT PRIMARY KEY,
                data JSONB
                {generatedColumnsSql}
            );"
        );
    }

    private static string GetGeneratedColumnsSql<T>()
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
}
