using DuckDB.NET.Data;
using EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System.Reflection;

namespace EnergyExemplar.Extensions.DuckDb.Internals
{
    /// <summary>
    /// Resolves entity types to their corresponding parquet table configurations and manages table/view creation.
    /// </summary>
    internal class ParquetTableResolver
    {
        private readonly MultiParquetConfiguration _configuration;
        private readonly Dictionary<Type, ParquetTableConfiguration> _entityTypeToTableMap;
        private readonly Dictionary<string, ParquetTableConfiguration> _tableNameToTableMap;

        public ParquetTableResolver(MultiParquetConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _configuration.Validate();

            _entityTypeToTableMap = configuration.Tables.ToDictionary(t => t.EntityType, t => t);
            _tableNameToTableMap = configuration.Tables.ToDictionary(t => t.GetEffectiveTableName(), t => t);
        }

        /// <summary>
        /// Gets the table configuration for a given entity type.
        /// </summary>
        public ParquetTableConfiguration? GetTableConfiguration(Type entityType)
        {
            return _entityTypeToTableMap.TryGetValue(entityType, out var config) ? config : null;
        }

        /// <summary>
        /// Gets the table configuration for a given table name.
        /// </summary>
        public ParquetTableConfiguration? GetTableConfiguration(string tableName)
        {
            return _tableNameToTableMap.TryGetValue(tableName, out var config) ? config : null;
        }

        /// <summary>
        /// Gets all table configurations.
        /// </summary>
        public IEnumerable<ParquetTableConfiguration> GetAllTableConfigurations()
        {
            return _configuration.Tables;
        }

        /// <summary>
        /// Gets all relationship configurations.
        /// </summary>
        public IEnumerable<ParquetRelationshipConfiguration> GetAllRelationshipConfigurations()
        {
            return _configuration.Relationships;
        }

        /// <summary>
        /// Gets the relationships where the given entity type is the principal.
        /// </summary>
        public IEnumerable<ParquetRelationshipConfiguration> GetPrincipalRelationships(Type entityType)
        {
            return _configuration.Relationships.Where(r => r.PrincipalEntityType == entityType);
        }

        /// <summary>
        /// Gets the relationships where the given entity type is the dependent.
        /// </summary>
        public IEnumerable<ParquetRelationshipConfiguration> GetDependentRelationships(Type entityType)
        {
            return _configuration.Relationships.Where(r => r.DependentEntityType == entityType);
        }

        /// <summary>
        /// Creates all views/tables in DuckDB for the configured parquet files.
        /// </summary>
        public async Task CreateViewsAsync(DuckDBConnection connection, CancellationToken cancellationToken = default)
        {
            foreach (var tableConfig in _configuration.Tables)
            {
                var createSql = tableConfig.GetCreateSql();
                using var command = connection.CreateCommand();
                command.CommandText = createSql;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Configures the Entity Framework model with the relationships defined in the configuration.
        /// </summary>
        public void ConfigureModel(ModelBuilder modelBuilder)
        {
            foreach (var relationship in _configuration.Relationships)
            {
                var principalEntityType = modelBuilder.Entity(relationship.PrincipalEntityType);
                var dependentEntityType = modelBuilder.Entity(relationship.DependentEntityType);

                // Configure the relationship - only add navigation properties if they exist
                if (!string.IsNullOrEmpty(relationship.DependentNavigationPropertyName))
                {
                    var relationshipBuilder = dependentEntityType.HasOne(relationship.PrincipalEntityType, relationship.PrincipalNavigationPropertyName)
                        .WithMany(relationship.DependentNavigationPropertyName)
                        .HasForeignKey(relationship.ForeignKeyPropertyName)
                        .HasPrincipalKey(relationship.PrincipalKeyPropertyName);

                    // Configure required/optional
                    if (relationship.IsRequired)
                    {
                        relationshipBuilder.IsRequired();
                    }
                    else
                    {
                        relationshipBuilder.IsRequired(false);
                    }

                    // Configure delete behavior
                    relationshipBuilder.OnDelete(relationship.DeleteBehavior);
                }
                else
                {
                    var relationshipBuilder = dependentEntityType.HasOne(relationship.PrincipalEntityType, relationship.PrincipalNavigationPropertyName)
                        .WithMany()
                        .HasForeignKey(relationship.ForeignKeyPropertyName)
                        .HasPrincipalKey(relationship.PrincipalKeyPropertyName);

                    // Configure required/optional
                    if (relationship.IsRequired)
                    {
                        relationshipBuilder.IsRequired();
                    }
                    else
                    {
                        relationshipBuilder.IsRequired(false);
                    }

                    // Configure delete behavior
                    relationshipBuilder.OnDelete(relationship.DeleteBehavior);
                }
            }
        }

        /// <summary>
        /// Gets the table name for a given entity type.
        /// </summary>
        public string? GetTableName(Type entityType)
        {
            return GetTableConfiguration(entityType)?.GetEffectiveTableName();
        }

        /// <summary>
        /// Gets the schema name for a given entity type.
        /// </summary>
        public string? GetSchemaName(Type entityType)
        {
            return GetTableConfiguration(entityType)?.Schema;
        }

        /// <summary>
        /// Validates that all entity types in the model have corresponding table configurations.
        /// </summary>
        public void ValidateModel(IEnumerable<IEntityType> entityTypes)
        {
            var missingEntities = entityTypes
                .Where(e => !_entityTypeToTableMap.ContainsKey(e.ClrType))
                .Select(e => e.ClrType.Name)
                .ToList();

            if (missingEntities.Any())
            {
                throw new InvalidOperationException(
                    $"The following entity types are not configured in the parquet configuration: {string.Join(", ", missingEntities)}. " +
                    $"All entity types must have corresponding table configurations.");
            }
        }

        /// <summary>
        /// Gets the SQL for creating all views/tables.
        /// </summary>
        public IEnumerable<string> GetCreateSqlStatements()
        {
            return _configuration.Tables.Select(t => t.GetCreateSql());
        }

        /// <summary>
        /// Checks if a table name exists in the configuration.
        /// </summary>
        public bool HasTable(string tableName)
        {
            return _tableNameToTableMap.ContainsKey(tableName);
        }

        /// <summary>
        /// Checks if an entity type exists in the configuration.
        /// </summary>
        public bool HasEntityType(Type entityType)
        {
            return _entityTypeToTableMap.ContainsKey(entityType);
        }
    }
} 