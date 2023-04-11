using System.Data;
using System.Text.Json;
using Npgsql;

namespace PostgresForDotnetDev.Core;

public static class PostgresExecutor
{
    public static void Run(this NpgsqlConnection connection, string sql)
    {
        if(connection.State !=ConnectionState.Open)
            connection.Open();

        using var command = new NpgsqlCommand(sql, connection);
        command.ExecuteNonQuery();
    }

    public static IEnumerable<T> AsEnumerableFromJson<T>(this NpgsqlCommand command, int jsonbColumnIndex = 0)
    {
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var json = reader.GetString(jsonbColumnIndex);
            var document = JsonSerializer.Deserialize<T>(json)!;
            yield return document;
        }
    }
}
