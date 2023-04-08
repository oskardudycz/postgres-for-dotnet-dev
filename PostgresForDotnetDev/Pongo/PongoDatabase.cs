using Npgsql;
using PostgresForDotnetDev.Core;

namespace PostgresForDotnetDev.Pongo;

public interface IPongoDatabase
{
    IPongoCollection<T> GetCollection<T>(string? name = null);
}

public class PongoDatabase: IPongoDatabase
{
    private readonly NpgsqlConnection connection;

    public PongoDatabase(NpgsqlConnection connection)
    {
        this.connection = connection;
        this.connection.EnablePgCryptoExtension();
        this.connection.EnableUuidOsspExtension();
    }

    public IPongoCollection<T> GetCollection<T>(string? name = null) =>
        new PongoCollection<T>(connection, name ?? typeof(T).FullName!.Replace(".", "_").Replace("+", "_").ToLower());
}
