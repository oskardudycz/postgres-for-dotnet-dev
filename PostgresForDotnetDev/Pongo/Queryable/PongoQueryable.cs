using System.Collections;
using System.Linq.Expressions;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;
using Remotion.Linq;

namespace PostgresForDotnetDev.Pongo.Filtering;

public class PongoQueryable<T>: QueryableBase<T>
{
    public PongoQueryable(PongoQueryableProvider provider): base(provider)
    {
    }
}

public class PongoQueryableProvider: IQueryProvider
{
    private readonly PongoQueryableExecutor executor;

    public PongoQueryableProvider(PongoQueryableExecutor executor) =>
        this.executor = executor;

    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException();

    public IQueryable<TElement> CreateQuery<TElement>(Expression _) =>
        throw new NotSupportedException();

    public object Execute(Expression expression) =>
        throw new NotSupportedException();

    public TResult Execute<TResult>(Expression expression) =>
        executor.Execute<TResult>(expression)!;
}
