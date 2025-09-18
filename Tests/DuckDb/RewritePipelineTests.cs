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

        [Test]
        public void LimitMinusOneToOffset_Should_Convert_Limit_Minus_One_To_Offset_Only()
        {
            // Test case for issue #19: LIMIT -1 OFFSET N compatibility
            string sql = "SELECT * FROM table LIMIT -1 OFFSET 10";
            string expected = "SELECT * FROM table OFFSET 10";

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            Assert.That(rewritten, Is.EqualTo(expected));
            Assert.That(rewritten, Does.Not.Contain("LIMIT -1"));
        }

        [Test]
        public void LimitMinusOneToOffset_Should_Handle_Case_Insensitive()
        {
            string sql = "SELECT * FROM table limit -1 offset 5";
            string expected = "SELECT * FROM table OFFSET 5";

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            Assert.That(rewritten, Is.EqualTo(expected));
        }

        [Test]
        public void LimitMinusOneToOffset_Should_Not_Affect_Regular_Limit()
        {
            string sql = "SELECT * FROM table LIMIT 10 OFFSET 5";
            string expected = sql; // Should remain unchanged

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            Assert.That(rewritten, Is.EqualTo(expected));
        }

        [Test]
        public void DateTimeNow_Should_Convert_Strftime_To_CurrentTimestamp()
        {
            string sql = "SELECT * FROM users WHERE created_date < strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')";
            string expected = "SELECT * FROM users WHERE created_date < CURRENT_TIMESTAMP";

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            Assert.That(rewritten, Is.EqualTo(expected));
        }

        [Test]
        public void DateTimeNow_Should_Handle_Case_Insensitive()
        {
            string sql = "SELECT * FROM users WHERE created_date < STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW', 'LOCALTIME')";

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            Assert.That(rewritten, Does.Contain("CURRENT_TIMESTAMP"));
            Assert.That(rewritten, Does.Not.Contain("strftime").IgnoreCase);
        }

        [Test]
        public void DateTimeNow_Should_Handle_Different_Format_Strings()
        {
            // Different format strings that EF Core might generate
            string[] testCases = {
                "strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')",
                "strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime')",
                "strftime('%Y-%m-%d', 'now', 'localtime')"
            };

            foreach (var dateTimePattern in testCases)
            {
                string sql = $"SELECT * FROM users WHERE created_date < {dateTimePattern}";
                string rewritten = RewritePipeline.Default.Rewrite(sql);

                Assert.That(rewritten, Does.Contain("CURRENT_TIMESTAMP"),
                    $"Failed to convert pattern: {dateTimePattern}");
                Assert.That(rewritten, Does.Not.Contain("strftime"),
                    $"strftime still present for pattern: {dateTimePattern}");
            }
        }

        [Test]
        public void DateTimeNow_Should_Not_Affect_Regular_Strftime()
        {
            // strftime without 'now' and 'localtime' should not be affected
            string sql = "SELECT strftime('%Y-%m-%d', created_date) FROM users";
            string expected = sql; // Should remain unchanged

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            Assert.That(rewritten, Is.EqualTo(expected));
        }

        [Test]
        public void DateTimeNow_Should_Handle_Multiple_Occurrences()
        {
            string sql = @"SELECT * FROM users
                          WHERE created_date < strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')
                          OR updated_date > strftime('%Y-%m-%d', 'now', 'localtime')";

            string rewritten = RewritePipeline.Default.Rewrite(sql);

            // Count occurrences of CURRENT_TIMESTAMP
            int count = System.Text.RegularExpressions.Regex.Matches(rewritten, "CURRENT_TIMESTAMP").Count;
            Assert.That(count, Is.EqualTo(2), "Should have replaced both strftime calls");
            Assert.That(rewritten, Does.Not.Contain("strftime"));
        }

        [Test]
        public void DateTimeNow_Should_Handle_With_Whitespace_Variations()
        {
            string[] testCases = {
                "strftime('%Y-%m-%d %H:%M:%f','now','localtime')", // No spaces
                "strftime( '%Y-%m-%d %H:%M:%f' , 'now' , 'localtime' )", // Extra spaces
                "strftime  (  '%Y-%m-%d %H:%M:%f'  ,  'now'  ,  'localtime'  )" // Multiple spaces
            };

            foreach (var pattern in testCases)
            {
                string sql = $"SELECT * FROM users WHERE created_date < {pattern}";
                string rewritten = RewritePipeline.Default.Rewrite(sql);

                Assert.That(rewritten, Does.Contain("CURRENT_TIMESTAMP"),
                    $"Failed for pattern with whitespace: {pattern}");
            }
        }

        [Test]
        public void DateTimeNow_Should_Handle_Rtrim_Wrapped_Pattern()
        {
            // EF Core sometimes wraps DateTime patterns with rtrim to handle fractional seconds
            string[] testCases = {
                "rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f', 'now'), '0'), '.')",
                "rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime'), '0'), '.')",
                "rtrim(strftime('%Y-%m-%d', 'now'), '.')"
            };

            foreach (var pattern in testCases)
            {
                string sql = $"SELECT * FROM users WHERE created_date < {pattern}";
                string rewritten = RewritePipeline.Default.Rewrite(sql);

                Assert.That(rewritten, Does.Contain("CURRENT_TIMESTAMP"),
                    $"Failed for rtrim wrapped pattern: {pattern}");
                Assert.That(rewritten, Does.Not.Contain("strftime").IgnoreCase,
                    $"strftime still present for pattern: {pattern}");
            }
        }
    }
}