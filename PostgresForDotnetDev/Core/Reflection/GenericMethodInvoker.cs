using System.Collections.Concurrent;
using System.Reflection;

namespace PostgresForDotnetDev.Core.Reflection;

public static class GenericMethodInvoker<T>
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<Type, MethodInfo>> CachedMethods = new();

    public static TResult? Invoke<TResult>(object instance, string methodName, Type genericTypeParam, params object[] parameters)
    {
        var result = GetGenericMethod(methodName, genericTypeParam)
            .Invoke(instance, parameters);

        return (TResult?)result;
    }

    private static MethodInfo GetGenericMethod(string methodName, Type type) =>
        CachedMethods
            .GetOrAdd(methodName, _ => new ConcurrentDictionary<Type, MethodInfo>())
            .GetOrAdd(type, t =>
                typeof(T)
                    .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                    .Single(m => m.Name == methodName && m.GetGenericArguments().Any())
                    .MakeGenericMethod(t)
            );
}

public static class GenericMethodInvoker
{
    public static TResult? Invoke<T, TResult>(this T instance, string methodName, Type genericTypeParam, params object[] parameters) =>
        GenericMethodInvoker<T>.Invoke<TResult>(instance!, methodName, genericTypeParam, parameters);
}
