using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration
{
    /// <summary>
    /// Configuration for a parquet table that maps to a specific parquet file.
    /// </summary>
    public class ParquetTableConfiguration
    {
        /// <summary>
        /// The full path to the parquet file.
        /// </summary>
        public string ParquetFilePath { get; set; } = string.Empty;

        /// <summary>
        /// The table/view name to use in DuckDB. If not specified, will use the filename without extension.
        /// </summary>
        public string? TableName { get; set; }

        /// <summary>
        /// The entity type that this parquet file maps to.
        /// </summary>
        public Type EntityType { get; set; } = null!;

        /// <summary>
        /// Optional schema name for the table/view.
        /// </summary>
        public string? Schema { get; set; }

        /// <summary>
        /// Whether to create a view (default) or a table.
        /// </summary>
        public bool CreateAsView { get; set; } = true;

        /// <summary>
        /// Additional DuckDB options for reading the parquet file.
        /// </summary>
        public Dictionary<string, object>? DuckDbOptions { get; set; }

        /// <summary>
        /// Gets the effective table name for this configuration.
        /// </summary>
        public string GetEffectiveTableName()
        {
            if (!string.IsNullOrEmpty(TableName))
            {
                return string.IsNullOrEmpty(Schema) ? TableName : $"{Schema}.{TableName}";
            }

            var fileName = Path.GetFileNameWithoutExtension(ParquetFilePath);
            return string.IsNullOrEmpty(Schema) ? fileName : $"{Schema}.{fileName}";
        }

        /// <summary>
        /// Gets the DuckDB SQL for creating the view/table.
        /// </summary>
        public string GetCreateSql()
        {
            var tableName = GetEffectiveTableName();
            var quotedTableName = $"\"{tableName}\"";
            var escapedFilePath = ParquetFilePath.Replace("'", "''");

            var optionsClause = "";
            if (DuckDbOptions?.Any() == true)
            {
                var options = string.Join(", ", DuckDbOptions.Select(kvp => 
                {
                    var value = kvp.Value is bool boolValue ? boolValue.ToString().ToLower() : kvp.Value.ToString();
                    return $"{kvp.Key}={value}";
                }));
                optionsClause = $" ({options})";
            }

            if (CreateAsView)
            {
                return $"CREATE OR REPLACE VIEW {quotedTableName} AS SELECT * FROM read_parquet('{escapedFilePath}'{optionsClause});";
            }
            else
            {
                return $"CREATE OR REPLACE TABLE {quotedTableName} AS SELECT * FROM read_parquet('{escapedFilePath}'{optionsClause});";
            }
        }
    }

    /// <summary>
    /// Configuration for relationships between parquet tables.
    /// </summary>
    public class ParquetRelationshipConfiguration
    {
        /// <summary>
        /// The principal entity type (the "one" side in one-to-many).
        /// </summary>
        public Type PrincipalEntityType { get; set; } = null!;

        /// <summary>
        /// The dependent entity type (the "many" side in one-to-many).
        /// </summary>
        public Type DependentEntityType { get; set; } = null!;

        /// <summary>
        /// The foreign key property name in the dependent entity.
        /// </summary>
        public string ForeignKeyPropertyName { get; set; } = string.Empty;

        /// <summary>
        /// The primary key property name in the principal entity.
        /// </summary>
        public string PrincipalKeyPropertyName { get; set; } = string.Empty;

        /// <summary>
        /// The navigation property name in the principal entity.
        /// </summary>
        public string? PrincipalNavigationPropertyName { get; set; }

        /// <summary>
        /// The navigation property name in the dependent entity.
        /// </summary>
        public string? DependentNavigationPropertyName { get; set; }

        /// <summary>
        /// Whether this is a required relationship (not nullable).
        /// </summary>
        public bool IsRequired { get; set; } = false;

        /// <summary>
        /// The delete behavior for this relationship.
        /// </summary>
        public DeleteBehavior DeleteBehavior { get; set; } = DeleteBehavior.Restrict;
    }

    /// <summary>
    /// Configuration for multiple parquet files with relationships.
    /// </summary>
    public class MultiParquetConfiguration
    {
        /// <summary>
        /// The parquet table configurations.
        /// </summary>
        public List<ParquetTableConfiguration> Tables { get; set; } = new();

        /// <summary>
        /// The relationship configurations between tables.
        /// </summary>
        public List<ParquetRelationshipConfiguration> Relationships { get; set; } = new();

        /// <summary>
        /// Optional path resolver for dynamic path resolution.
        /// When set, path templates can be used instead of absolute paths.
        /// </summary>
        public IParquetPathResolver? PathResolver { get; set; }

        /// <summary>
        /// Adds a parquet table configuration.
        /// </summary>
        public MultiParquetConfiguration AddTable<TEntity>(string parquetFilePath, string? tableName = null, string? schema = null)
        {
            Tables.Add(new ParquetTableConfiguration
            {
                ParquetFilePath = parquetFilePath,
                TableName = tableName,
                EntityType = typeof(TEntity),
                Schema = schema
            });
            return this;
        }

        /// <summary>
        /// Adds a parquet table configuration using a path template.
        /// The template will be resolved using the configured PathResolver.
        /// </summary>
        /// <typeparam name="TEntity">The entity type</typeparam>
        /// <param name="pathTemplate">Path template with placeholders like {env}, {EntityName}, etc.</param>
        /// <param name="tableName">Optional table name override</param>
        /// <param name="schema">Optional schema name</param>
        /// <param name="environment">Optional environment override for this specific table</param>
        /// <returns>The configuration instance for method chaining</returns>
        public MultiParquetConfiguration AddTableWithTemplate<TEntity>(
            string pathTemplate, 
            string? tableName = null, 
            string? schema = null,
            string? environment = null)
        {
            var resolvedPath = PathResolver?.ResolvePath<TEntity>(pathTemplate, environment) ?? pathTemplate;
            return AddTable<TEntity>(resolvedPath, tableName, schema);
        }

        /// <summary>
        /// Sets the path resolver for this configuration.
        /// This enables template-based path resolution.
        /// </summary>
        /// <param name="pathResolver">The path resolver to use</param>
        /// <returns>The configuration instance for method chaining</returns>
        public MultiParquetConfiguration WithPathResolver(IParquetPathResolver pathResolver)
        {
            PathResolver = pathResolver;
            return this;
        }

        /// <summary>
        /// Adds a relationship configuration.
        /// </summary>
        public MultiParquetConfiguration AddRelationship<TPrincipal, TDependent>(
            string foreignKeyPropertyName,
            string principalKeyPropertyName = "Id",
            string? principalNavigationPropertyName = null,
            string? dependentNavigationPropertyName = null,
            bool isRequired = false,
            DeleteBehavior deleteBehavior = DeleteBehavior.Restrict)
        {
            Relationships.Add(new ParquetRelationshipConfiguration
            {
                PrincipalEntityType = typeof(TPrincipal),
                DependentEntityType = typeof(TDependent),
                ForeignKeyPropertyName = foreignKeyPropertyName,
                PrincipalKeyPropertyName = principalKeyPropertyName,
                PrincipalNavigationPropertyName = principalNavigationPropertyName,
                DependentNavigationPropertyName = dependentNavigationPropertyName,
                IsRequired = isRequired,
                DeleteBehavior = deleteBehavior
            });
            return this;
        }

        /// <summary>
        /// Validates the configuration.
        /// </summary>
        public void Validate()
        {
            if (!Tables.Any())
                throw new InvalidOperationException("At least one table configuration is required.");

            foreach (var table in Tables)
            {
                if (string.IsNullOrEmpty(table.ParquetFilePath))
                    throw new InvalidOperationException($"ParquetFilePath is required for table {table.EntityType.Name}.");

                if (!File.Exists(table.ParquetFilePath))
                    throw new FileNotFoundException($"Parquet file not found: {table.ParquetFilePath}");

                if (!table.ParquetFilePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
                    throw new ArgumentException($"File must be a .parquet file: {table.ParquetFilePath}");
            }

            foreach (var relationship in Relationships)
            {
                var principalTable = Tables.FirstOrDefault(t => t.EntityType == relationship.PrincipalEntityType);
                var dependentTable = Tables.FirstOrDefault(t => t.EntityType == relationship.DependentEntityType);

                if (principalTable == null)
                    throw new InvalidOperationException($"Principal entity type {relationship.PrincipalEntityType.Name} not found in table configurations.");

                if (dependentTable == null)
                    throw new InvalidOperationException($"Dependent entity type {relationship.DependentEntityType.Name} not found in table configurations.");
            }
        }
    }
} 