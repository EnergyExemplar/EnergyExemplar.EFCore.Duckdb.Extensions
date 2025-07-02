using EnergyExemplar.Extensions.DuckDb.Internals;
using NUnit.Framework;

namespace Tests.DuckDb
{
    public class RewritePipelineTests
    {
        [Test]
        public void ConvertJsonEach_WithAlias_Should_Produce_Unnest_With_Value()
        {
            string input = "SELECT * FROM json_each('[1,2]') AS g WHERE 1=1";
            string expectedSegment = "UNNEST([1,2]) AS g(value)";

            string output = RewriterHelpers.ConvertJsonEach(input);

            Assert.That(output, Does.Contain(expectedSegment));
            Assert.That(output, Does.Not.Contain("json_each"));
        }

        [Test]
        public void ConvertJsonEach_WithoutAlias_Should_Produce_Unnest_Only()
        {
            string input = "SELECT * FROM json_each('[1,2]') WHERE 1=1";
            string expectedSegment = "UNNEST([1,2])";

            string output = RewriterHelpers.ConvertJsonEach(input);

            Assert.That(output, Does.Contain(expectedSegment));
            Assert.That(output, Does.Not.Contain("json_each"));
            // Should not append (value) when no alias present
            Assert.That(output, Does.Not.Contain("(value)"));
        }

        [Test]
        public void DefaultPipeline_Should_Apply_All_Registered_Rules()
        {
            string sql = @"SELECT RANDOM() AS r
                           FROM foo
                           WHERE (col1) & (col2) AND name GLOB 'test*' ESCAPE '\\' OR (col3) | (col4);";

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            // RANDOM() → random()
            Assert.That(rewritten, Does.Contain("random()").And.Not.Contain("RANDOM()"));
            // GLOB → LIKE
            Assert.That(rewritten, Does.Contain("LIKE").And.Not.Contain("GLOB"));
            // ESCAPE '\\' removed
            Assert.That(rewritten, Does.Not.Contain("ESCAPE"));
            // Bitwise to logical
            Assert.That(rewritten, Does.Not.Contain("&"));
            Assert.That(rewritten, Does.Not.Contain("|"));
            Assert.That(rewritten, Does.Contain("AND").And.Contain("OR"));
        }
    }
} 