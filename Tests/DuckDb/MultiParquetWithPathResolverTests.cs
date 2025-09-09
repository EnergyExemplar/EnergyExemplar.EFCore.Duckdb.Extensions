using EnergyExemplar.EntityFrameworkCore.DuckDb;
using EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Tests.DuckDb
{
    [TestFixture]
    public class MultiParquetWithPathResolverTests
    {
        private string _testDataDirectory = null!;
        private string _customersParquetPath = null!;
        private string _ordersParquetPath = null!;

        [SetUp]
        public void Setup()
        {
            // Use the existing test data directory structure
            var testDataRelativePath = Path.Combine("Tests", "DuckDb", "TestData");
            var currentDir = TestContext.CurrentContext.TestDirectory;
            var projectRoot = FindProjectRoot(currentDir, testDataRelativePath);
            _testDataDirectory = Path.Combine(projectRoot, testDataRelativePath);
            
            _customersParquetPath = Path.Combine(_testDataDirectory, "test.parquet");
            _ordersParquetPath = Path.Combine(_testDataDirectory, "test.parquet"); // Using same file for test
        }

        private static string FindProjectRoot(string currentDir, string relativePath)
        {
            var dir = new DirectoryInfo(currentDir);
            while (dir != null && !File.Exists(Path.Combine(dir.FullName, relativePath, "test.parquet")))
            {
                dir = dir.Parent;
            }
            return dir?.FullName ?? throw new FileNotFoundException("Could not locate test.parquet in project tree.");
        }

        [Test]
        public void UseDuckDbWithPathResolver_WithValidConfiguration_ConfiguresCorrectly()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test", new Dictionary<string, string>
            {
                ["dataPath"] = _testDataDirectory
            });

            var builder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert - Should not throw
            Assert.DoesNotThrow(() =>
            {
                builder.UseDuckDbWithPathResolver(pathResolver, config =>
                {
                    config.AddTableWithTemplate<TestEntity>("{dataPath}/test.parquet", "customers");
                });
            });

            var options = builder.Options;
            Assert.That(options, Is.Not.Null);
        }

        [Test]
        public void UseDuckDbWithEnvironmentPaths_WithValidConfiguration_ConfiguresCorrectly()
        {
            // Arrange
            var customVariables = new Dictionary<string, string>
            {
                ["dataPath"] = _testDataDirectory
            };

            var builder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert - Should not throw
            Assert.DoesNotThrow(() =>
            {
                builder.UseDuckDbWithEnvironmentPaths("test", config =>
                {
                    config.AddTableWithTemplate<TestEntity>("{dataPath}/test.parquet", "test_entity");
                }, customVariables);
            });

            var options = builder.Options;
            Assert.That(options, Is.Not.Null);
        }

        [Test]
        public void MultiParquetConfiguration_WithPathResolver_ResolvesPathsCorrectly()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("dev", new Dictionary<string, string>
            {
                ["basePath"] = _testDataDirectory
            });

            var config = new MultiParquetConfiguration()
                .WithPathResolver(pathResolver)
                .AddTableWithTemplate<TestEntity>("{basePath}/test.parquet", "entities");

            // Act
            var tableConfig = config.Tables.First();

            // Assert
            Assert.That(tableConfig.ParquetFilePath, Is.EqualTo(Path.Combine(_testDataDirectory, "test.parquet")));
            Assert.That(tableConfig.TableName, Is.EqualTo("entities"));
            Assert.That(tableConfig.EntityType, Is.EqualTo(typeof(TestEntity)));
        }

        [Test]
        public void MultiParquetConfiguration_AddTableWithTemplate_WithEnvironmentOverride_UsesOverride()
        {
            // Arrange  
            var pathResolver = new EnvironmentParquetPathResolver("dev", new Dictionary<string, string>
            {
                ["basePath"] = _testDataDirectory
            });

            var config = new MultiParquetConfiguration()
                .WithPathResolver(pathResolver);

            // Act
            config.AddTableWithTemplate<TestEntity>("{basePath}/test.parquet", "entities", environment: "prod");

            // Assert
            var tableConfig = config.Tables.First();
            Assert.That(tableConfig.ParquetFilePath, Does.Contain(_testDataDirectory));
            Assert.That(tableConfig.ParquetFilePath, Does.EndWith("test.parquet"));
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_TryResolvePath_WithValidTemplate_ReturnsSuccess()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test", new Dictionary<string, string>
            {
                ["dataPath"] = _testDataDirectory
            });

            // Act
            var result = pathResolver.TryResolvePath<TestEntity>("{dataPath}/test.parquet");

            // Assert
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResolvedPath, Does.Contain(_testDataDirectory));
            Assert.That(result.ResolvedPath, Does.EndWith("test.parquet"));
            Assert.That(result.Errors, Is.Empty);
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_TryResolvePath_WithInvalidTemplate_ReturnsFailure()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test");

            // Act
            var result = pathResolver.TryResolvePath<TestEntity>("");

            // Assert
            Assert.That(result.Success, Is.False);
            Assert.That(result.Errors, Is.Not.Empty);
            Assert.That(result.Errors.First(), Does.Contain("Template cannot be null or empty"));
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_TryResolvePath_WithMaliciousInput_SanitizesPath()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test", new Dictionary<string, string>
            {
                ["malicious"] = "../../sensitive/data"
            });

            // Act
            var result = pathResolver.TryResolvePath<TestEntity>("{malicious}/test.parquet");

            // Assert
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResolvedPath, Does.Not.Contain(".."));
            Assert.That(result.ResolvedPath, Does.Contain("sensitive"));
            Assert.That(result.ResolvedPath, Does.Contain("data"));
            Assert.That(result.ResolvedPath, Does.EndWith("test.parquet"));
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_WithNullEntityType_ReturnsFailure()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test");

            // Act
            var result = pathResolver.TryResolvePath(null!, "test.parquet");

            // Assert
            Assert.That(result.Success, Is.False);
            Assert.That(result.Errors.First(), Does.Contain("Entity type cannot be null"));
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_WithUnresolvedPlaceholders_ReturnsWarnings()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test");

            // Act
            var result = pathResolver.TryResolvePath<TestEntity>("{unknownVariable}/test.parquet");

            // Assert
            Assert.That(result.Success, Is.True);
            Assert.That(result.Warnings, Is.Not.Empty);
            Assert.That(result.Warnings.First(), Does.Contain("Unresolved placeholders"));
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_Constructor_WithInvalidEnvironment_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => new EnvironmentParquetPathResolver(""));
            Assert.Throws<ArgumentException>(() => new EnvironmentParquetPathResolver(null!));
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_SetVariable_WithInvalidKey_ThrowsException()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test");

            // Act & Assert
            Assert.Throws<ArgumentException>(() => pathResolver.SetVariable("", "value"));
            Assert.Throws<ArgumentException>(() => pathResolver.SetVariable(null!, "value"));
        }
        
        [Test]
        public void StaticParquetPathResolver_TryResolvePath_WithValidTemplate_ReturnsSuccess()
        {
            // Arrange
            var pathResolver = new StaticParquetPathResolver();

            // Act
            var result = pathResolver.TryResolvePath<TestEntity>("test.parquet");

            // Assert
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResolvedPath, Is.EqualTo("test.parquet"));
            Assert.That(result.Errors, Is.Empty);
        }
        
        [Test]
        public void EnvironmentParquetPathResolver_WithDatePlaceholder_ResolvesCorrectly()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test");
            var today = DateTime.Now.ToString("yyyy-MM-dd");

            // Act
            var result = pathResolver.TryResolvePath<TestEntity>("data/{Date:yyyy-MM-dd}/test.parquet");

            // Assert
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResolvedPath, Does.Contain(today));
        }

        [Test]
        public void UseDuckDbWithPathResolver_WithNullPathResolver_ThrowsArgumentNullException()
        {
            // Arrange
            var builder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
            {
                builder.UseDuckDbWithPathResolver(null!, config => { });
            });
        }

        [Test]
        public void UseDuckDbWithPathResolver_WithNullConfiguration_ThrowsArgumentNullException()
        {
            // Arrange
            var builder = new DbContextOptionsBuilder<TestDbContext>();
            var pathResolver = new EnvironmentParquetPathResolver("test");

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
            {
                builder.UseDuckDbWithPathResolver(pathResolver, null!);
            });
        }

        [Test]
        public void UseDuckDbWithEnvironmentPaths_WithNullEnvironment_ThrowsArgumentNullException()
        {
            // Arrange
            var builder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
            {
                builder.UseDuckDbWithEnvironmentPaths(null!, config => { });
            });
        }

        [Test]
        public void MultiParquetConfiguration_WithPathResolver_FluentChaining_WorksCorrectly()
        {
            // Arrange
            var pathResolver = new EnvironmentParquetPathResolver("test", new Dictionary<string, string>
            {
                ["dataPath"] = _testDataDirectory
            });

            // Act
            var config = new MultiParquetConfiguration()
                .WithPathResolver(pathResolver)
                .AddTableWithTemplate<TestEntity>("{dataPath}/test.parquet", "entities")
                .AddTableWithTemplate<TestOrder>("{dataPath}/test.parquet", "orders")
                .AddRelationship<TestEntity, TestOrder>("EntityId", "Id", "Orders", "Entity");

            // Assert
            Assert.That(config.Tables.Count, Is.EqualTo(2));
            Assert.That(config.Relationships.Count, Is.EqualTo(1));
            Assert.That(config.PathResolver, Is.EqualTo(pathResolver));

            var entityTable = config.Tables.First(t => t.EntityType == typeof(TestEntity));
            var orderTable = config.Tables.First(t => t.EntityType == typeof(TestOrder));
            
            Assert.That(entityTable.ParquetFilePath, Is.EqualTo(Path.Combine(_testDataDirectory, "test.parquet")));
            Assert.That(orderTable.ParquetFilePath, Is.EqualTo(Path.Combine(_testDataDirectory, "test.parquet")));
        }

        // Test entities
        public class TestEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public List<TestOrder> Orders { get; set; } = new();
        }

        public class TestOrder
        {
            public int Id { get; set; }
            public int EntityId { get; set; }
            public string Description { get; set; } = string.Empty;
            public TestEntity Entity { get; set; } = null!;
        }

        // Test DbContext
        public class TestDbContext : DbContext
        {
            public TestDbContext(DbContextOptions<TestDbContext> options) : base(options) { }

            public DbSet<TestEntity> Entities { get; set; } = null!;
            public DbSet<TestOrder> Orders { get; set; } = null!;
        }
    }
}
