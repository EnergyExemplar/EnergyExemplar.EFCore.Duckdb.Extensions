using System;
using System.Linq;
using DuckDB.NET.Data;
using EnergyExemplar.EntityFrameworkCore.DuckDb.Interceptors;
using NUnit.Framework;

namespace Tests.DuckDb
{
    [TestFixture]
    public class DbDataReaderCustomCastingTests
    {
        private static DuckDBConnection CreateConnection()
        {
            var conn = new DuckDBConnection("DataSource=:memory:");
            conn.Open();
            return conn;
        }

        [Test]
        public void GetBoolean_Should_Handle_Int_Long_And_String()
        {
            using var conn = CreateConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1 AS intTrue, 0 AS intFalse, 9223372036854775807::BIGINT AS longTrue, 0::BIGINT AS longFalse, 'true' AS strTrue, '0' AS strFalse, NULL AS nullVal";
            using var reader = new DbDataReaderCustomCasting(cmd.ExecuteReader());
            Assert.That(reader.Read(), Is.True);

            Assert.Multiple(() =>
            {
                Assert.That(reader.GetBoolean(0), Is.True, "Int 1 should map to true");
                Assert.That(reader.GetBoolean(1), Is.False, "Int 0 should map to false");
                Assert.That(reader.GetBoolean(2), Is.True, "Long non-zero should map to true");
                Assert.That(reader.GetBoolean(3), Is.False, "Long 0 should map to false");
                Assert.That(reader.GetBoolean(4), Is.True, "String 'true' should map to true");
                Assert.That(reader.GetBoolean(5), Is.False, "String '0' should map to false");
                // Calling IsDBNull before GetBoolean is standard pattern; ensure no exception
                Assert.That(reader.IsDBNull(6), Is.True);
            });
        }

        [Test]
        public void GetInt64_Should_Handle_Diverse_Types()
        {
            using var conn = CreateConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 123 AS intVal, 1234567890123::BIGINT AS longVal, 987.0::DOUBLE AS doubleVal, 456.78::DECIMAL(18,2) AS decVal, '789' AS strVal, NULL AS nullVal";
            using var reader = new DbDataReaderCustomCasting(cmd.ExecuteReader());
            Assert.That(reader.Read(), Is.True);

            Assert.Multiple(() =>
            {
                Assert.That(reader.GetInt64(0), Is.EqualTo(123));
                Assert.That(reader.GetInt64(1), Is.EqualTo(1234567890123));
                Assert.That(reader.GetInt64(2), Is.EqualTo(987)); // double → long (floor)
                Assert.That(reader.GetInt64(3), Is.EqualTo(457)); // decimal → long (floor)
                Assert.That(reader.GetInt64(4), Is.EqualTo(789)); // string numeric
                // Verify NULL handling does not throw and IsDBNull returns true
                Assert.That(reader.IsDBNull(5), Is.True);
            });
        }
    }
} 