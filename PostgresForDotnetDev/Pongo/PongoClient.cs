using Npgsql;

namespace PostgresForDotnetDev.Pongo;

public interface IPongoClient
{
    IPongoDatabase GetDatabase(string? databaseName = null);
}

public class PongoClient: IPongoClient
{
    private readonly string connectionString;

    public PongoClient(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public IPongoDatabase GetDatabase(string? databaseName = null)
    {
        var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        if (!string.IsNullOrWhiteSpace(databaseName))
            connection.ChangeDatabase(databaseName);

        return new PongoDatabase(connection);
    }
}
