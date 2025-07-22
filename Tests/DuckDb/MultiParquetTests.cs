using System;
using System.IO;
using System.Linq;
using EnergyExemplar.EntityFrameworkCore.DuckDb;
using EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System.Collections.Generic;

namespace Tests.DuckDb
{
    public class MultiParquetTests
    {
        private static string GetTestDataPath(string fileName)
        {
            var relative = Path.Combine("Tests", "DuckDb", "TestData", fileName);
            var root = TestContext.CurrentContext.TestDirectory;
            var dir = new DirectoryInfo(root);
            while (dir is not null && !File.Exists(Path.Combine(dir.FullName, relative)))
            {
                dir = dir.Parent;
            }
            if (dir is null) throw new FileNotFoundException($"Could not locate {fileName} in project tree.");
            return Path.GetFullPath(Path.Combine(dir.FullName, relative));
        }

        [Test]
        public void UseDuckDbOnMultipleParquet_Should_Configure_Multiple_Tables()
        {
            // Create test parquet files (in a real scenario, these would be actual parquet files)
            var customersPath = GetTestDataPath("test.parquet"); // Using existing test file for demo
            var ordersPath = GetTestDataPath("test.parquet"); // Using existing test file for demo

            var configuration = new MultiParquetConfiguration()
                .AddTable<Customer>(customersPath, "customers")
                .AddTable<Order>(ordersPath, "orders")
                .AddRelationship<Customer, Order>("CustomerId", "Id");

            var builder = new DbContextOptionsBuilder<MultiParquetContext>();
            builder.UseDuckDbOnMultipleParquet(configuration);

            var options = builder.Options;
            using var context = new MultiParquetContext(options, configuration);

            // Verify that the context can be created and configured
            Assert.That(context.Customers, Is.Not.Null);
            Assert.That(context.Orders, Is.Not.Null);
        }

        [Test]
        public void UseDuckDbOnMultipleParquet_With_Action_Should_Configure_Multiple_Tables()
        {
            var customersPath = GetTestDataPath("test.parquet");
            var ordersPath = GetTestDataPath("test.parquet");

            var configuration = new MultiParquetConfiguration()
                .AddTable<Customer>(customersPath, "customers")
                .AddTable<Order>(ordersPath, "orders")
                .AddRelationship<Customer, Order>("CustomerId", "Id");

            var builder = new DbContextOptionsBuilder<MultiParquetContext>();
            builder.UseDuckDbOnMultipleParquet(configuration);

            var options = builder.Options;
            using var context = new MultiParquetContext(options, configuration);

            // Verify that the context can be created
            Assert.That(context.Customers, Is.Not.Null);
            Assert.That(context.Orders, Is.Not.Null);
        }

        [Test]
        public void MultiParquetConfiguration_Validation_Should_Throw_For_Missing_Files()
        {
            var configuration = new MultiParquetConfiguration()
                .AddTable<Customer>("nonexistent.parquet", "customers");

            Assert.Throws<FileNotFoundException>(() => configuration.Validate());
        }

        [Test]
        public void MultiParquetConfiguration_Validation_Should_Throw_For_NonParquet_Files()
        {
            var tempFile = Path.GetTempFileName();
            try
            {
                var configuration = new MultiParquetConfiguration()
                    .AddTable<Customer>(tempFile, "customers");

                Assert.Throws<ArgumentException>(() => configuration.Validate());
            }
            finally
            {
                if (File.Exists(tempFile))
                    File.Delete(tempFile);
            }
        }

        [Test]
        public void MultiParquetConfiguration_Validation_Should_Throw_For_Invalid_Relationships()
        {
            var customersPath = GetTestDataPath("test.parquet");

            var configuration = new MultiParquetConfiguration()
                .AddTable<Customer>(customersPath, "customers")
                .AddRelationship<Customer, Order>("CustomerId", "Id");
            // Note: Order table is not added, so relationship should fail

            Assert.Throws<InvalidOperationException>(() => configuration.Validate());
        }

        [Test]
        public void ParquetTableConfiguration_Should_Generate_Correct_SQL()
        {
            var config = new ParquetTableConfiguration
            {
                ParquetFilePath = @"C:\data\customers.parquet",
                TableName = "customers",
                CreateAsView = true
            };

            var sql = config.GetCreateSql();
            Assert.That(sql, Does.Contain("CREATE OR REPLACE VIEW"));
            Assert.That(sql, Does.Contain("\"customers\""));
            Assert.That(sql, Does.Contain("read_parquet"));
        }

        [Test]
        public void ParquetTableConfiguration_With_Schema_Should_Generate_Correct_SQL()
        {
            var config = new ParquetTableConfiguration
            {
                ParquetFilePath = @"C:\data\customers.parquet",
                TableName = "customers",
                Schema = "sales",
                CreateAsView = true
            };

            var sql = config.GetCreateSql();
            Assert.That(sql, Does.Contain("\"sales.customers\""));
            Assert.That(sql, Does.Contain("CREATE OR REPLACE VIEW"));
        }

        [Test]
        public void ParquetTableConfiguration_With_Options_Should_Generate_Correct_SQL()
        {
            var config = new ParquetTableConfiguration
            {
                ParquetFilePath = @"C:\data\customers.parquet",
                TableName = "customers",
                DuckDbOptions = new Dictionary<string, object>
                {
                    ["binary_as_string"] = true,
                    ["filename"] = true
                }
            };

            var sql = config.GetCreateSql();
            Assert.That(sql, Does.Contain("binary_as_string=true"));
            Assert.That(sql, Does.Contain("filename=true"));
        }

        private class MultiParquetContext : DbContext
        {
            private readonly MultiParquetConfiguration _configuration;

            public MultiParquetContext(DbContextOptions<MultiParquetContext> options, MultiParquetConfiguration configuration) 
                : base(options)
            {
                _configuration = configuration;
            }

            public DbSet<Customer> Customers => Set<Customer>();
            public DbSet<Order> Orders => Set<Order>();

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                base.OnModelCreating(modelBuilder);

                // Configure the model with parquet relationships
                modelBuilder.ConfigureParquetRelationships(_configuration);

                // Configure entity mappings
                modelBuilder.Entity<Customer>(entity =>
                {
                    entity.HasKey(e => e.Id);
                    entity.Property(e => e.Name).IsRequired();
                });

                modelBuilder.Entity<Order>(entity =>
                {
                    entity.HasKey(e => e.Id);
                    entity.Property(e => e.OrderDate).IsRequired();
                });
            }
        }

        private class Customer
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public string Email { get; set; } = string.Empty;
            public DateTime CreatedDate { get; set; }

            // Navigation property
            public virtual ICollection<Order> Orders { get; set; } = new List<Order>();
        }

        private class Order
        {
            public int Id { get; set; }
            public int CustomerId { get; set; }
            public DateTime OrderDate { get; set; }
            public decimal TotalAmount { get; set; }
            public string Status { get; set; } = string.Empty;

            // Navigation property
            public virtual Customer Customer { get; set; } = null!;
        }
    }
} 