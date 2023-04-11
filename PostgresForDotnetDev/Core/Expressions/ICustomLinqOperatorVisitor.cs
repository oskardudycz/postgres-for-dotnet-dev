using System.Linq.Expressions;

namespace PostgresForDotnetDev.Core.Expressions;

public interface ICustomLinqOperatorVisitor
{
    public Expression? Visit(MethodCallExpression node, Func<Expression?, Expression?> visit);
}
