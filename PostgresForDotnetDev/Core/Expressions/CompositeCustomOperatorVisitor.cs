using System.Linq.Expressions;

namespace PostgresForDotnetDev.Core.Expressions;

public class CompositeCustomOperatorVisitor: ICustomLinqOperatorVisitor
{
    private readonly Dictionary<string, ICustomLinqOperatorVisitor> functions;

    public CompositeCustomOperatorVisitor(params Dictionary<string, ICustomLinqOperatorVisitor>[] functions) =>
        this.functions = functions
            .SelectMany(x => x)
            .GroupBy(d => d.Key)
            .ToDictionary(x => x.Key, y => y.First().Value);


    public CompositeCustomOperatorVisitor(params CompositeCustomOperatorVisitor[] customOperatorVisitors):
        this(customOperatorVisitors.Select(c => c.functions).ToArray())
    {
    }

    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit) =>
        functions.TryGetValue(node.Method.Name, out var value) ? value.Visit(node, visit) : null;
}
