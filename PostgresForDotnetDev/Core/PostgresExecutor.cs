using System.Data;
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
}
