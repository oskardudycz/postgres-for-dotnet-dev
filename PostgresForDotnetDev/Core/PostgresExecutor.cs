using System.Data;
using System.Text.Json;
using Dapper;
using Npgsql;
using Spectre.Console;

namespace PostgresForDotnetDev.Core;

public static class PostgresExecutor
{
    public static void Run(this NpgsqlConnection connection, string sql)
    {
        if (connection.State != ConnectionState.Open)
            connection.Open();

        using var command = new NpgsqlCommand(sql, connection);
        command.ExecuteNonQuery();
    }

    public static async Task PrintAsync(this NpgsqlConnection connection, string query)
    {
        AnsiConsole.Write(new Markup(query));

        var result = ((await connection.QueryAsync(query)).Select(row => (IDictionary<string, object>)row)).ToList();

        var table = new Spectre.Console.Table();

        if (!result.Any())
        {
            AnsiConsole.Write(new Markup("No results!"));
            return;
        }

        foreach (var key in result.First().Keys)
        {
            table.AddColumn(new TableColumn(key));
        }

        foreach (var rowDict in result)
        {
            table.AddRow(rowDict.Values.Select(v => v?.ToString() ?? "NULL").ToArray());
        }

        AnsiConsole.Write(table);
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
