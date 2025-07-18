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

        [Test]
        public void GetDecimal_Should_Handle_Double_Float_And_Integer_Types()
        {
            using var conn = CreateConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 123.45::DOUBLE AS doubleVal, 67.89::REAL AS floatVal, 100 AS intVal, 9223372036854775807::BIGINT AS longVal, 12::SMALLINT AS shortVal, 127::TINYINT AS byteVal, '123.456' AS strVal, NULL AS nullVal";
            using var reader = new DbDataReaderCustomCasting(cmd.ExecuteReader());
            Assert.That(reader.Read(), Is.True);

            Assert.Multiple(() =>
            {
                Assert.That(reader.GetDecimal(0), Is.EqualTo(123.45m)); // double → decimal
                Assert.That(reader.GetDecimal(1), Is.EqualTo(67.89m).Within(0.001m)); // float → decimal (with tolerance for float precision)
                Assert.That(reader.GetDecimal(2), Is.EqualTo(100m)); // int → decimal
                Assert.That(reader.GetDecimal(3), Is.EqualTo(9223372036854775807m)); // long → decimal
                Assert.That(reader.GetDecimal(4), Is.EqualTo(12m)); // short → decimal
                Assert.That(reader.GetDecimal(5), Is.EqualTo(127m)); // byte → decimal (127 is max signed byte)
                Assert.That(reader.GetDecimal(6), Is.EqualTo(123.456m)); // string → decimal
                // Verify NULL handling
                Assert.That(reader.IsDBNull(7), Is.True);
            });
        }

        [Test]
        public void GetDecimal_Should_Handle_Large_Double_Values()
        {
            using var conn = CreateConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 999999999.99::DOUBLE AS largeDouble, -123456789.123::DOUBLE AS negativeDouble";
            using var reader = new DbDataReaderCustomCasting(cmd.ExecuteReader());
            Assert.That(reader.Read(), Is.True);

            Assert.Multiple(() =>
            {
                Assert.That(reader.GetDecimal(0), Is.EqualTo(999999999.99m));
                Assert.That(reader.GetDecimal(1), Is.EqualTo(-123456789.123m));
            });
        }

        [Test]
        public void GetDecimal_Should_Handle_Edge_Cases()
        {
            using var conn = CreateConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 0.0::DOUBLE AS zero, -0.0::DOUBLE AS negativeZero, 'NaN' AS nanString, 'invalid' AS invalidString";
            using var reader = new DbDataReaderCustomCasting(cmd.ExecuteReader());
            Assert.That(reader.Read(), Is.True);

            Assert.Multiple(() =>
            {
                Assert.That(reader.GetDecimal(0), Is.EqualTo(0m));
                Assert.That(reader.GetDecimal(1), Is.EqualTo(0m));
                
                // Test invalid string conversion - should throw or handle gracefully
                Assert.Throws<FormatException>(() => reader.GetDecimal(3)); // 'invalid' string should throw
            });
        }
    }
}