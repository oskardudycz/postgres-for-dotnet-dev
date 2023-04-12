using System.Linq.Expressions;
using ExpressionVisitor = MongoDB.Bson.Serialization.ExpressionVisitor;

namespace PostgresForDotnetDev.Core.Expressions.Where;

public class WhereClauseExtractor : ExpressionVisitor
{
    private readonly List<Expression> criteriaExpressions = new();

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (!IsFilteringMethod(node.Method.Name)) return base.VisitMethodCall(node);
        criteriaExpressions.Add(node.Arguments[1]);
        return base.VisitMethodCall(node);
    }

    private static bool IsFilteringMethod(string methodName)
    {
        return methodName is "Where" or "SingleOrDefault" or "FirstOrDefault" or "LastOrDefault" or "Any" or "Count" or "All";
    }

    public IEnumerable<Expression> GetCriteriaExpressions(Expression expression)
    {
        Visit(expression);
        return criteriaExpressions;
    }
}
