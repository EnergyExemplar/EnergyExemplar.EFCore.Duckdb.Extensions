// OPTIONAL: This convention automatically sets column names to match property names.
// Usage example:
// DbContextOptionsBuilder<PlexosInputDbContext> contextBuilder = new DbContextOptionsBuilder<PlexosInputDbContext>();
// contextBuilder.ReplaceService<IProviderConventionSetBuilder, CustomConventionSetBuilder>();
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb.Conventions
{
    public class PropertyNameAsColumnNameConvention : IPropertyAddedConvention
    {
        public void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        IConventionContext<IConventionPropertyBuilder> context)
        {
            propertyBuilder.HasColumnName(propertyBuilder.Metadata.Name, fromDataAnnotation: true);
        }

    }
}
