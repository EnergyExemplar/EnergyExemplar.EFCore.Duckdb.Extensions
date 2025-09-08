using EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;

namespace Tests.DuckDb
{
    [TestFixture]
    public class DynamicPathResolutionTests
    {
        private EnvironmentParquetPathResolver _resolver = null!;
        private Dictionary<string, string> _customVariables = null!;

        [SetUp]
        public void Setup()
        {
            _customVariables = new Dictionary<string, string>
            {
                ["region"] = "us-east-1",
                ["bucket"] = "test-bucket"
            };
            _resolver = new EnvironmentParquetPathResolver("dev", _customVariables);
        }

        [Test]
        public void ResolvePath_WithEnvironmentPlaceholder_ReplacesCorrectly()
        {
            // Arrange
            var template = "C:/data/{env}/customers.parquet";

            // Act
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert - Use Path.Combine to get the expected platform-specific path
            var expected = Path.Combine("C:", "data", "dev", "customers.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void ResolvePath_WithEntityNamePlaceholder_ReplacesCorrectly()
        {
            // Arrange
            var template = "C:/data/{EntityName}.parquet";

            // Act
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert
            var expected = Path.Combine("C:", "data", "Customer.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void ResolvePath_WithCustomVariables_ReplacesCorrectly()
        {
            // Arrange
            var template = "s3://{bucket}-{env}/data/{region}/{EntityName}.parquet";

            // Act
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert
            Assert.That(result, Is.EqualTo("s3://test-bucket-dev/data/us-east-1/Customer.parquet"));
        }

        [Test]
        public void ResolvePath_WithDatePlaceholders_ReplacesCorrectly()
        {
            // Arrange
            var template = "C:/data/{Date:yyyy}/{Date:yyyy-MM}/{Date:yyyy-MM-dd}/{EntityName}.parquet";
            var now = DateTime.Now;

            // Act
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert
            var expected = Path.Combine("C:", "data", $"{now:yyyy}", $"{now:yyyy-MM}", $"{now:yyyy-MM-dd}", "Customer.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void ResolvePath_WithEnvironmentOverride_UsesOverride()
        {
            // Arrange
            var template = "C:/data/{env}/customers.parquet";

            // Act
            var result = _resolver.ResolvePath<Customer>(template, "prod");

            // Assert
            var expected = Path.Combine("C:", "data", "prod", "customers.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void ResolvePath_CaseInsensitive_ReplacesCorrectly()
        {
            // Arrange
            var template = "C:/data/{ENV}/{entityname}.parquet";

            // Act
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert
            var expected = Path.Combine("C:", "data", "dev", "Customer.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void ResolvePath_WithConfiguration_ReplacesConfigValues()
        {
            // Arrange
            var configData = new Dictionary<string, string>
            {
                ["DataPath"] = "C:/production/data",
                ["BucketName"] = "prod-bucket"
            };
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(configData!)
                .Build();

            var resolver = new EnvironmentParquetPathResolver("prod", null, configuration);
            var template = "{Config:DataPath}/{env}/{Config:BucketName}/{EntityName}.parquet";

            // Act
            var result = resolver.ResolvePath<Customer>(template);

            // Assert
            var expected = Path.Combine("C:", "production", "data", "prod", "prod-bucket", "Customer.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void StaticPathResolver_ReturnsTemplateAsIs()
        {
            // Arrange
            var resolver = new StaticParquetPathResolver();
            var template = "C:/data/{env}/{EntityName}.parquet";

            // Act
            var result = resolver.ResolvePath<Customer>(template);

            // Assert
            Assert.That(result, Is.EqualTo(template));
        }

        [Test]
        public void SetVariable_UpdatesVariableCorrectly()
        {
            // Arrange
            var template = "C:/data/{region}/{EntityName}.parquet";

            // Act
            _resolver.SetVariable("region", "us-west-2");
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert
            var expected = Path.Combine("C:", "data", "us-west-2", "Customer.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void RemoveVariable_RemovesVariableCorrectly()
        {
            // Arrange
            var template = "C:/data/{region}/{EntityName}.parquet";

            // Act
            _resolver.RemoveVariable("region");
            var result = _resolver.ResolvePath<Customer>(template);

            // Assert - variable should not be replaced
            var expected = Path.Combine("C:", "data", "{region}", "Customer.parquet");
            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void Variables_ReturnsReadOnlyDictionary()
        {
            // Act
            var variables = _resolver.Variables;

            // Assert
            Assert.That(variables, Is.Not.Null);
            Assert.That(variables.Count, Is.EqualTo(2));
            Assert.That(variables["region"], Is.EqualTo("us-east-1"));
            Assert.That(variables["bucket"], Is.EqualTo("test-bucket"));

            // Should be read-only
            Assert.That(variables, Is.AssignableTo<IReadOnlyDictionary<string, string>>());
        }

        // Test entity class
        public class Customer
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }
}
