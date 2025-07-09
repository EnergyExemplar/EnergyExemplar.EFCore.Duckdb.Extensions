namespace EnergyExemplar.Extensions.DuckDb.Internals
{
    /// <summary>
    /// Represents an immutable SQL rewrite rule that can transform SQL queries from one dialect to another.
    /// Each rule consists of a condition function and a transformation function that are applied sequentially
    /// in a pipeline to translate vendor-specific SQL into DuckDB-compatible SQL.
    /// </summary>
    internal sealed record SqlRewriteRule(
        string Name,
        Func<string, bool> ShouldRun,
        Func<string, string> Apply);
}