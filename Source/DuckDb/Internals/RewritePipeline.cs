namespace EnergyExemplar.Extensions.DuckDb.Internals
{
    /// <summary>
    /// Executes a chain of <see cref="SqlRewriteRule"/> instances to translate vendor-specific SQL
    /// into DuckDB-compatible SQL. Processes SQL strings through a series of transformations,
    /// applying each rule's condition and transformation logic in sequence.
    /// </summary>
    internal sealed class RewritePipeline
    {
        private readonly IReadOnlyList<SqlRewriteRule> _rules;
        internal RewritePipeline(IEnumerable<SqlRewriteRule> rules) => _rules = rules.ToList();

        internal string Rewrite(string sql)
        {
            foreach (var r in _rules)
                if (r.ShouldRun(sql))
                    sql = r.Apply(sql);
            return sql;
        }

        /// <summary>
        /// Default pipeline with commonly-needed SQLite → DuckDB translations.
        /// </summary>
        internal static RewritePipeline Default { get; } = new RewritePipeline(new[]
        {
            DuckSqlRules.JsonEachToUnnest,
            DuckSqlRules.LikeEscapeStrip,
            DuckSqlRules.GlobToLike,
            DuckSqlRules.RandomFuncAlias,
            DuckSqlRules.BitwiseAndToLogicalAnd,
            DuckSqlRules.BitwiseOrToLogicalOr,
            DuckSqlRules.LimitMinusOneToOffset
        });
    }
}