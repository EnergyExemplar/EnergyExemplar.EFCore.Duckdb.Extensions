// OPTIONAL: Custom convention set builder that applies EntityNameAsTableNameConvention and PropertyNameAsColumnNameConvention.
// Usage example:
// DbContextOptionsBuilder<PlexosInputDbContext> contextBuilder = new DbContextOptionsBuilder<PlexosInputDbContext>();
// contextBuilder.ReplaceService<IProviderConventionSetBuilder, CustomConventionSetBuilder>();
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb.Conventions
{
    public class CustomConventionSetBuilder : SqlServerConventionSetBuilder
    {
        public CustomConventionSetBuilder(ProviderConventionSetBuilderDependencies dependencies, RelationalConventionSetBuilderDependencies relationalDependencies, ISqlGenerationHelper sqlGenerationHelper) : base(dependencies, relationalDependencies, sqlGenerationHelper)
        {
        }
        public override ConventionSet CreateConventionSet()
        {
            var set = base.CreateConventionSet();
            set.EntityTypeAddedConventions.Add(new EntityNameAsTableNameConvention());
            set.PropertyAddedConventions.Add(new PropertyNameAsColumnNameConvention());
            return set;

        }
    }
}
