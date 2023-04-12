using System.Collections;
using System.Linq.Expressions;
using PostgresForDotnetDev.Core.Expressions;
using PostgresForDotnetDev.Pongo.Filtering.TimescaleDB;
using Remotion.Linq;

namespace PostgresForDotnetDev.Pongo.Filtering;

public class PongoQueryable<T>: QueryableBase<T>
{
    public PongoQueryable(IQueryProvider provider): base(provider)
    {
    }

    public PongoQueryable(IQueryProvider provider, Expression expression): base(provider, expression)
    {
    }
}

public class PongoQueryableProvider<TSource>: IQueryProvider
{
    private readonly PongoQueryableExecutor executor;

    public PongoQueryableProvider(PongoQueryableExecutor executor) =>
        this.executor = executor;

    public IQueryable CreateQuery(Expression expression) =>
        new PongoQueryable<TSource>(this, expression);

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) =>
        new PongoQueryable<TElement>(this, expression);

    public object Execute(Expression expression) =>
        throw new NotSupportedException();

    public TResult Execute<TResult>(Expression expression) =>
        executor.Execute<TSource, TResult>(expression)!;
}
