namespace PostgresForDotnetDev.Pongo.Collections;

public static class PongoCollectionName
{
    public static string For(Type type) => type.FullName!.Replace(".", "_").Replace("+", "_").ToLower();
    public static string For<T>() => For(typeof(T));
}
