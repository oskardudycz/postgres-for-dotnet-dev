using MongoDB.Driver;

namespace PostgresForDotnetDev.Pongo;

public interface IPongoCollection<T>
{
    Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(
        FilterDefinition<T> filter,
        FindOptions<T, TProjection>? options = null,
        string? encryptionKey = null,
        CancellationToken cancellationToken = default);

    Task InsertOneAsync(
        T document,
        InsertOneOptions? options = null,
        string? encryptionKey = null,
        CancellationToken cancellationToken = default);

    Task<UpdateResult> UpdateOneAsync(
        FilterDefinition<T> filter,
        UpdateDefinition<T> update,
        UpdateOptions? options = null,
        CancellationToken cancellationToken = default);

    Task<ReplaceOneResult> ReplaceOneAsync(
        FilterDefinition<T> filter,
        T replacement,
        ReplaceOptions? options = null,
        CancellationToken cancellationToken = default);

    Task<DeleteResult> DeleteOneAsync(
        FilterDefinition<T> filter,
        CancellationToken cancellationToken = default);

    IQueryable<T> AsQueryable();
}
