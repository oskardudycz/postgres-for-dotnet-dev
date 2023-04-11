using System.Linq.Expressions;
using System.Reflection;
using PostgresForDotnetDev.Pongo;

namespace PostgresForDotnetDev.Core.Expressions;

public static class ExpressionExtensions
{
    public static T GetConstantArgument<T>(this Expression argumentExpression)
    {
        switch (argumentExpression)
        {
            case ConstantExpression constantExpression when constantExpression.Type == typeof(T):
                return (T)constantExpression.Value!;
            case MemberExpression { Member: FieldInfo fieldInfo } memberExpression when
                fieldInfo.FieldType == typeof(T):
            {
                var constantExpression = (ConstantExpression)memberExpression.Expression!;
                var instance = constantExpression.Value;
                var value = (T)fieldInfo.GetValue(instance)!;
                return value;
            }
            default:
                throw new ArgumentException("Invalid argument type.");
        }
    }

    public static Type GetQueryableElementType(this Expression expression)
    {
        Type enumerableType = expression.Type;

        if (enumerableType.IsGenericType && enumerableType.GetGenericTypeDefinition() == typeof(IQueryable<>))
        {
            return enumerableType.GetGenericArguments()[0];
        }

        throw new ArgumentException("The provided expression does not represent an IQueryable.", nameof(expression));
    }

    public static string UnwrapSqlExpression(this Expression expression)
    {
        LambdaExpression? lambdaExpression = null;

        switch (expression)
        {
            case UnaryExpression { Operand: LambdaExpression unaryLambdaExpression }:
                lambdaExpression = unaryLambdaExpression;
                break;
            case LambdaExpression visitedLambdaExpression:
                lambdaExpression = visitedLambdaExpression;
                break;
            default:
            {
                if (expression.NodeType == ExpressionType.Quote && expression is UnaryExpression { Operand: LambdaExpression quoteLambdaExpression })
                {
                    lambdaExpression = quoteLambdaExpression;
                }
                break;
            }
        }

        if (lambdaExpression?.Body is not SqlExpression sqlExpression)
        {
            throw new InvalidOperationException("Invalid Expression!");
        }

        return sqlExpression.Sql;
    }
}
