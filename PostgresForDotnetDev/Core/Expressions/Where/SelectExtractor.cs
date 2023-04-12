using System.Linq.Expressions;
using PostgresForDotnetDev.Pongo.Filtering;
using ExpressionVisitor = MongoDB.Bson.Serialization.ExpressionVisitor;

namespace PostgresForDotnetDev.Core.Expressions;

public class SelectExtractor: ExpressionVisitor
{
    private readonly List<LambdaExpression> selectExpressions = new();

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (IsWhereMethod(node.Method.Name))
        {
            // Visit the source of the Where method call to find any nested Select calls
            Visit(node.Arguments[0]);
        }
        else if (IsSelectMethod(node.Method.Name))
        {
            // Cast the second argument of the method call to a LambdaExpression
            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
            selectExpressions.Add(lambda);
        }

        return base.VisitMethodCall(node);
    }

    private static bool IsWhereMethod(string methodName)
    {
        return methodName is "Where" or "SingleOrDefault" or "FirstOrDefault" or "LastOrDefault" or "Any" or "Count"
            or "All";
    }

    private static bool IsSelectMethod(string methodName)
    {
        return methodName == "Select";
    }

    public IEnumerable<LambdaExpression> GetSelectExpressions(Expression expression)
    {
        Visit(expression);
        return selectExpressions;
    }

    private static Expression StripQuotes(Expression e)
    {
        while (e.NodeType == ExpressionType.Quote)
        {
            e = ((UnaryExpression)e).Operand;
        }

        return e;
    }
}
