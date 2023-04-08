using System.Diagnostics;
using Npgsql;
using PostgresForDotnetDev.Core;

namespace PostgresForDotnetDevs.Tests;

public static class PostgresConnectionProvider
{
    public static string GetFreshConnectionString(int frame = 1)
    {
        var testClassName = new StackTrace().GetFrame(frame)!.GetMethod()!.DeclaringType!.Name;
        return GetFreshConnectionString(testClassName);
    }


    public static string GetFreshConnectionString(string schemaName) =>
        Settings.ConnectionString + $"Search Path= '{schemaName}'";

    public static NpgsqlConnection GetFreshDbConnection()
    {
        // get the test class name that will be used as POSTGRES schema
        var testClassName = new StackTrace().GetFrame(1)!.GetMethod()!.DeclaringType!.Name;
        // each test will have it's own schema name to run have data isolation and not interfere other tests
        var connection = new NpgsqlConnection(GetFreshConnectionString(testClassName));

        // recreate schema to have it fresh for tests. Kids do not try that on production.
        connection.Run($"DROP SCHEMA IF EXISTS {testClassName} CASCADE; CREATE SCHEMA {testClassName};");

        return connection;
    }
}
