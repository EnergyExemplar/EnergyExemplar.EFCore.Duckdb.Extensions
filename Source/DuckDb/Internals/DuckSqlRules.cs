using System.Text.RegularExpressions;

namespace EnergyExemplar.Extensions.DuckDb.Internals
{
    /// <summary>
    /// Provides a collection of predefined SQL rewrite rules for translating SQLite syntax to DuckDB syntax.
    /// Contains commonly needed rules that handle differences between SQLite and DuckDB SQL dialects,
    /// including json_each to UNNEST conversion and GLOB to LIKE translation.
    /// </summary>
    internal static class DuckSqlRules
    {
        internal static readonly SqlRewriteRule JsonEachToUnnest = new(
            "JsonEach→Unnest",
            s => s.Contains("json_each(", StringComparison.OrdinalIgnoreCase),
            RewriterHelpers.ConvertJsonEach);

        internal static readonly SqlRewriteRule LikeEscapeStrip = new(
            "StripEscape",
            s => Regex.IsMatch(s, "\\s+ESCAPE\\s+'\\\\\\\\'", RegexOptions.IgnoreCase),
            s => Regex.Replace(s, "\\s+ESCAPE\\s+'\\\\\\\\'", "", RegexOptions.IgnoreCase));

        internal static readonly SqlRewriteRule GlobToLike = new(
            "Glob→Like",
            s => s.Contains(" GLOB ", StringComparison.OrdinalIgnoreCase),
            s => Regex.Replace(s, "\\bGLOB\\b", "LIKE", RegexOptions.IgnoreCase));

        internal static readonly SqlRewriteRule RandomFuncAlias = new(
            "RandomAlias",
            s => s.Contains("RANDOM()", StringComparison.OrdinalIgnoreCase),
            s => Regex.Replace(s, "RANDOM\\(\\)", "random()", RegexOptions.IgnoreCase));

        internal static readonly SqlRewriteRule BitwiseAndToLogicalAnd = new(
            "BitwiseAnd→LogicalAnd",
            s => s.Contains("&"),
            s => Regex.Replace(s, @"\)\s*&\s*\(", ") AND (", RegexOptions.Multiline));

        internal static readonly SqlRewriteRule BitwiseOrToLogicalOr = new(
            "BitwiseOr→LogicalOr",
            s => s.Contains("|"),
            s => Regex.Replace(s, @"\)\s*\|\s*\(", ") OR (", RegexOptions.Multiline));
    }
}