using System.Text.Json;
using MongoDB.Driver;
using Npgsql;

namespace PostgresForDotnetDev.Pongo.Querying;

public class PongoAsyncCursor<T> : IAsyncCursor<T>
{
    private readonly NpgsqlDataReader _reader;
    private List<T> _currentBatch;

    public PongoAsyncCursor(NpgsqlDataReader reader)
    {
        _reader = reader;
        _currentBatch = new List<T>();
    }

    public IEnumerable<T> Current => _currentBatch;

    public bool MoveNext(CancellationToken cancellationToken = default)
    {
        return MoveNextAsync(cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        _currentBatch.Clear();

        while (await _reader.ReadAsync(cancellationToken))
        {
            var json = _reader.GetString(0);
            var document = JsonSerializer.Deserialize<T>(json)!;
            _currentBatch.Add(document);

            // Break if the cancellationToken is requested
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        return _currentBatch.Count > 0;
    }

    public void Dispose()
    {
        _reader.Dispose();
    }
}

