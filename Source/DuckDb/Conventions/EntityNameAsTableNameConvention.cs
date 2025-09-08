// OPTIONAL: This convention automatically sets table names to match entity names.
// Usage example:
// DbContextOptionsBuilder<PlexosInputDbContext> contextBuilder = new DbContextOptionsBuilder<PlexosInputDbContext>();
// contextBuilder.ReplaceService<IProviderConventionSetBuilder, CustomConventionSetBuilder>();
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb.Conventions
{
    public class EntityNameAsTableNameConvention : IEntityTypeAddedConvention
    {
        public void ProcessEntityTypeAdded(
            IConventionEntityTypeBuilder entityTypeBuilder,
            IConventionContext<IConventionEntityTypeBuilder> context)
        {

            entityTypeBuilder.ToTable(entityTypeBuilder.Metadata.ShortName(), fromDataAnnotation: true);
        }
    }
}