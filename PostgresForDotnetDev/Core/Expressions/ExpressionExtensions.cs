using System.Linq.Expressions;
using System.Reflection;

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

    public static string UnwrapSqlExpression(this Expression expression)
    {
        if (expression is SqlExpression expression1)
            return expression1.Sql;

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
