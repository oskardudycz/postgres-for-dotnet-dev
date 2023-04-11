using System.Linq.Expressions;

namespace PostgresForDotnetDev.Core.Expressions;

public class SqlExpression: Expression
{
    public string Sql { get; }

    public SqlExpression(string sql, Type? type = null)
    {
        Sql = sql;
        Type = type ?? typeof(string);
    }

    public override Type Type { get; }

    public override ExpressionType NodeType => ExpressionType.Extension;

    public override string ToString() => Sql;
}
