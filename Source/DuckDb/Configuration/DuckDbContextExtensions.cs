using EnergyExemplar.Extensions.DuckDb.Internals;
using Microsoft.EntityFrameworkCore;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration
{
    /// <summary>
    /// Extension methods for DbContext to configure parquet table relationships.
    /// </summary>
    public static class DuckDbContextExtensions
    {
        /// <summary>
        /// Configures the DbContext model with parquet table relationships.
        /// This should be called in the OnModelCreating method of your DbContext.
        /// </summary>
        public static void ConfigureParquetRelationships(this ModelBuilder modelBuilder, MultiParquetConfiguration configuration)
        {
            var resolver = new ParquetTableResolver(configuration);
            resolver.ConfigureModel(modelBuilder);
        }

        /// <summary>
        /// Validates that all entity types in the model have corresponding parquet table configurations.
        /// This should be called after the model is built to ensure all entities are properly configured.
        /// </summary>
        public static void ValidateParquetConfiguration(this ModelBuilder modelBuilder, MultiParquetConfiguration configuration)
        {
            var resolver = new ParquetTableResolver(configuration);
            var entityTypes = modelBuilder.Model.GetEntityTypes().Cast<Microsoft.EntityFrameworkCore.Metadata.IEntityType>();
            resolver.ValidateModel(entityTypes);
        }

        /// <summary>
        /// Gets the table name for an entity type from the parquet configuration.
        /// </summary>
        public static string? GetParquetTableName<TEntity>(this ModelBuilder modelBuilder, MultiParquetConfiguration configuration)
        {
            var resolver = new ParquetTableResolver(configuration);
            return resolver.GetTableName(typeof(TEntity));
        }

        /// <summary>
        /// Gets the schema name for an entity type from the parquet configuration.
        /// </summary>
        public static string? GetParquetSchemaName<TEntity>(this ModelBuilder modelBuilder, MultiParquetConfiguration configuration)
        {
            var resolver = new ParquetTableResolver(configuration);
            return resolver.GetSchemaName(typeof(TEntity));
        }
    }
} 