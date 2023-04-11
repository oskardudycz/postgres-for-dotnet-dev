using System.Text.Json;
using MongoDB.Driver;
using Npgsql;

namespace PostgresForDotnetDev.Pongo.Querying;

public class PongoCursor<T> : IAsyncCursor<T>
{
    private readonly NpgsqlDataReader reader;
    private readonly List<T> currentBatch;

    public PongoCursor(NpgsqlDataReader reader)
    {
        this.reader = reader;
        currentBatch = new List<T>();
    }

    public IEnumerable<T> Current => currentBatch;

    public bool MoveNext(CancellationToken cancellationToken = default)
    {
        return MoveNextAsync(cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        currentBatch.Clear();

        while (await reader.ReadAsync(cancellationToken))
        {
            var json = reader.GetString(0);
            var document = JsonSerializer.Deserialize<T>(json)!;
            currentBatch.Add(document);

            // Break if the cancellationToken is requested
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        return currentBatch.Count > 0;
    }

    public void Dispose()
    {
        reader.Dispose();
    }
}

