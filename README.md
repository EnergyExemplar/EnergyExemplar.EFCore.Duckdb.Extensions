# DuckDB Entity Framework Core Integration

[![NuGet](https://img.shields.io/nuget/v/EnergyExemplar.EntityFrameworkCore.DuckDb.svg)](https://www.nuget.org/packages/EnergyExemplar.EntityFrameworkCore.DuckDb)
[![NuGet Downloads](https://img.shields.io/nuget/dt/EnergyExemplar.EntityFrameworkCore.DuckDb.svg)](https://www.nuget.org/packages/EnergyExemplar.EntityFrameworkCore.DuckDb)

This provides a seamless integration between Entity Framework Core and DuckDB, allowing you to use DuckDB as your database while maintaining the familiar EF Core API.

## Features

- **Simple API**: Just use `UseDuckDb()` similar to `UseSqlite()` or `UseSqlServer()`
- **Automatic SQL Translation**: Converts EF Core generated SQL to DuckDB-compatible SQL
- **Full EF Core Support**: Works with LINQ queries, navigation properties, and all EF Core features
- **Performance**: Leverages DuckDB's columnar storage for fast analytical queries on Parquet files with significant performance improvements over SQLite
- **Developer-Friendly API**: The extension replaces fragile, manual setups with a clean, fluent API. This reduces code complexity, improves maintainability, and accelerates development.

## Performance

The performance improvements are significant when using DuckDB over SQLite for analytical queries:

| Database | OData Query Performance |
|----------|-------------------------|
| **SQLite** | 7.61s - 8.83s |
| **DuckDB** | 800ms - 1s |

**Performance Gain: ~8-10x faster** 🚀

DuckDB's columnar storage and vectorized execution engine provide substantial performance benefits for analytical workloads, making it an excellent choice for data analysis and reporting scenarios.

## Installation

Ensure you have the necessary NuGet packages:
```xml
<PackageReference Include="Microsoft.EntityFrameworkCore" Version="8.0.8" />
<PackageReference Include="Microsoft.EntityFrameworkCore.Sqlite" Version="8.0.8" />
<PackageReference Include="DuckDB.NET.Data.Full" Version="1.3.2" />
```

## Basic Usage

### 1. Lambda-Based Configuration (Recommended)

```csharp
using EnergyExemplar.EntityFrameworkCore.DuckDb;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDuckDb(opts =>
    {
        opts.ConnectionString = "DataSource=C:/data/mydb.duckdb";
        opts.MemoryLimitGB = 4; // Optional: set DuckDB memory limit to 4GB
        opts.FileSearchPath = "C:/data/"; // Optional: set file_search_path for relative views
    })
    .Options;

using var context = new MyDbContext(options);
var data = await context.MyEntities.Where(e => e.Name.StartsWith("Gen")).ToListAsync();
```

### 2. Using Dependency Injection (ASP.NET Core)

```csharp
// In your Program.cs or Startup.cs
services.AddDbContext<MyDbContext>(options =>
    options.UseDuckDb(opts =>
    {
        opts.ConnectionString = builder.Configuration["DuckDb:ConnectionString"];
        opts.MemoryLimitGB = 8;
        opts.FileSearchPath = builder.Configuration["DuckDb:FileSearchPath"];
    })
);
```

### 3. Using a Configuration Object

```csharp
var duckDbOptions = new DuckDbConnectionOptions
{
    ConnectionString = "DataSource=C:/data/mydb.duckdb",
    MemoryLimitGB = 16,
    FileSearchPath = "C:/data/"
};

var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDuckDb(duckDbOptions)
    .Options;
```

### 4. Parquet File Only (No .duckdb Database)

```csharp
var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDuckDbOnParquet("C:/data/mydata.parquet")
    .Options;
```

> **Tip**: Change tracking is disabled by default for maximum performance. To enable it, pass `noTracking: false` to `UseDuckDbOnParquet()` or set `NoTracking = false` on `DuckDbConnectionOptions`.

## Migration from Previous Implementation

### Before (Manual Setup)  
*Manual DbConnection management, custom interceptors, and scattered configuration.*

### After (Using UseDuckDb)
```csharp
var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDuckDb(opts =>
    {
        opts.ConnectionString = "DataSource=C:/data/mydb.duckdb";
    })
    .Options;
```

## Configuration Options

### DuckDbConnectionOptions Properties

- **ConnectionString** (required): Path to the DuckDB database file (e.g., `DataSource=mydb.duckdb`)
- **FileSearchPath** (optional): Base directory for resolving relative paths in DuckDB views
- **MemoryLimitGB** (optional): Memory limit for DuckDB operations (in GB)
- **NoTracking** (optional): If true (the default), all queries will be executed with `AsNoTracking()`. Set to `false` to enable change tracking.

## Example: Complete Application

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using EnergyExemplar.EntityFrameworkCore.DuckDb;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddDbContext<MyDbContext>(options =>
            options.UseDuckDb(opts =>
            {
                opts.ConnectionString = context.Configuration["DuckDb:ConnectionString"];
                opts.MemoryLimitGB = 8;
                opts.FileSearchPath = context.Configuration["DuckDb:FileSearchPath"];
            })
        );
        services.AddScoped<IDataService, DataService>();
    })
    .Build();

await host.RunAsync();

public class DataService : IDataService
{
    private readonly MyDbContext _context;
    public DataService(MyDbContext context) => _context = context;
    public async Task<List<MyEntity>> GetDataAsync()
        => await _context.MyEntities.Where(e => e.IsActive).ToListAsync();
}
```

## Advanced Scenarios

### Multiple Contexts

```csharp
services.AddDbContext<InputContext>(options =>
    options.UseDuckDb(opts =>
    {
        opts.ConnectionString = "DataSource=input.duckdb";
    })
);
services.AddDbContext<OutputContext>(options =>
    options.UseDuckDb(opts =>
    {
        opts.ConnectionString = "DataSource=output.duckdb";
    })
);
```

### Parquet-Only Context

```csharp
services.AddDbContext<MyParquetContext>(options =>
    options.UseDuckDbOnParquet("C:/data/mydata.parquet")
);
```

## Troubleshooting

1. **Memory Issues**: Set `MemoryLimitGB` in the options lambda or object.
2. **Path Issues**: Ensure `FileSearchPath` is set correctly for relative paths in views.
3. **Write Operations**: The integration is read-only; any write operation will throw `NotSupportedException`.
4. **Performance**: DuckDB excels at analytical queries but may be slower for OLTP workloads.

## Benefits Over Manual Configuration

1. **Cleaner Code**: No need to manually configure interceptors and options
2. **Type Safety**: Configuration options are strongly typed
3. **Consistency**: Ensures all required components are properly configured
4. **Maintainability**: Changes to DuckDB integration are centralized
5. **Testability**: Easy to mock or substitute in tests

## Notes

- The implementation uses SQLite as the base provider but intercepts and translates all SQL to DuckDB
- Query tracking is disabled by default for better performance (can be overridden via `NoTracking = false`)
- All DuckDB dependencies are automatically registered when using the extensions

## Custom Conventions (Optional)

The library includes optional EF Core conventions that can simplify your model configuration by automatically mapping entity and property names directly to table and column names.

### Available Conventions

1. **EntityNameAsTableNameConvention**: Automatically sets table names to match entity class names
2. **PropertyNameAsColumnNameConvention**: Automatically sets column names to match property names
3. **CustomConventionSetBuilder**: Applies both conventions together

### Usage

```csharp
using EnergyExemplar.EntityFrameworkCore.DuckDb.Conventions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

// Option 1: Apply conventions when configuring DbContext
var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDuckDb(opts =>
    {
        opts.ConnectionString = "DataSource=mydb.duckdb";
    })
    .ReplaceService<IProviderConventionSetBuilder, CustomConventionSetBuilder>()
    .Options;

// Option 2: Apply in dependency injection
services.AddDbContext<MyDbContext>(options =>
{
    options.UseDuckDb(opts =>
    {
        opts.ConnectionString = configuration["DuckDb:ConnectionString"];
    })
    .ReplaceService<IProviderConventionSetBuilder, CustomConventionSetBuilder>();
});
```

### Benefits

- **Reduced Boilerplate**: No need to manually configure table and column names
- **Consistency**: Ensures naming conventions are applied uniformly across your model
- **DuckDB Compatibility**: Particularly useful when working with DuckDB views or Parquet files where column names must match exactly
- **Simplified Migrations**: When entity/property names match database schema, there's less configuration needed

### Example Scenario

Without conventions:
```csharp
public class CustomerOrder
{
    public int OrderId { get; set; }
    public string CustomerName { get; set; }
}

// Requires manual configuration
modelBuilder.Entity<CustomerOrder>()
    .ToTable("CustomerOrder")
    .Property(e => e.CustomerName)
    .HasColumnName("CustomerName");
```

With conventions (automatically applied):
```csharp
public class CustomerOrder
{
    public int OrderId { get; set; }
    public string CustomerName { get; set; }
}
// No additional configuration needed!
```

### When to Use

These conventions are particularly helpful when:
- Your entity and property names already match your database schema
- You're working with existing Parquet files or DuckDB views
- You want to reduce EF Core configuration code
- You prefer convention over configuration approach

> **Note**: These conventions are optional. If you need different table/column names than your entity/property names, simply don't apply the conventions and use standard EF Core configuration.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### Third-Party Licenses

This project depends on the following third-party libraries:

- **Microsoft.EntityFrameworkCore** - Licensed under the MIT License. See [Entity Framework Core repository](https://github.com/dotnet/efcore) for details.
- **Microsoft.EntityFrameworkCore.Sqlite** - Licensed under the MIT License. See [Entity Framework Core repository](https://github.com/dotnet/efcore) for details.
- **DuckDB.NET.Data.Full** - Licensed under the MIT License. See [DuckDB.NET repository](https://github.com/DuckDB/duckdb-dotnet) for details.
