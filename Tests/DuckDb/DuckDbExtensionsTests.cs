using System;
using System.IO;
using System.Linq;
using EnergyExemplar.EntityFrameworkCore.DuckDb;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using NUnit.Framework;
using System.Reflection;

namespace Tests.DuckDb
{
    public class DuckDbExtensionsTests
    {
        private static string GetParquetPath()
        {
            // Build absolute path to the embedded parquet sample
            var relative = Path.Combine("Tests", "DuckDb", "TestData", "test.parquet");
            var root = TestContext.CurrentContext.TestDirectory; // points to bin/Debug/net8.0 during test run
            // Walk up until we find the workspace root at which the relative file exists
            var dir = new DirectoryInfo(root);
            while (dir is not null && !File.Exists(Path.Combine(dir.FullName, relative)))
            {
                dir = dir.Parent;
            }
            if (dir is null) throw new FileNotFoundException("Could not locate test.parquet in project tree.");
            return Path.GetFullPath(Path.Combine(dir.FullName, relative));
        }

        // ------------------------------------------------------------------
        // Positive path: configuration succeeds, interceptor registered and
        // data from the parquet file is reachable through EF Core LINQ.
        // ------------------------------------------------------------------
        [Test]
        public void UseDuckDbOnParquet_Should_RegisterInterceptor_And_ReadData()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            DbContextOptions<ParquetContext> options = builder.Options;

            // Assert QueryTrackingBehavior.NoTracking is set
            var coreExt = options.Extensions.OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>().Single();
            Assert.That(coreExt.QueryTrackingBehavior, Is.EqualTo(QueryTrackingBehavior.NoTracking));

            // Assert interceptor present
            Assert.That(coreExt.Interceptors.Any(i => i.GetType().Name == "DuckDbCommandInterceptor"), "DuckDbCommandInterceptor was not registered.");

            // Run a simple query against the parquet data
            using var ctx = new ParquetContext(options);
            bool hasRows = ctx.Items.Any();
            Assert.That(hasRows, Is.True, "Expected at least one row in parquet_view.");
        }

        [Test]
        public void UseDuckDbOnParquet_Should_Throw_When_File_Not_Parquet()
        {
            var builder = new DbContextOptionsBuilder();
            var ex = Assert.Throws<ArgumentException>(() => builder.UseDuckDbOnParquet("notParquet.txt"));
            Assert.That(ex!.Message, Does.Contain("Must be a .parquet file"));
        }

        [Test]
        public void UseDuckDb_Should_Append_MemoryLimit_To_ConnectionString()
        {
            var optionsObj = new DuckDbConnectionOptions
            {
                ConnectionString = "DataSource=:memory:",
                MemoryLimitGB = 2
            };

            var builder = new DbContextOptionsBuilder();
            builder.UseDuckDb(optionsObj);

            var coreExt = builder.Options.Extensions.OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>().Single();
            var interceptor = coreExt.Interceptors.Single(i => i.GetType().Name == "DuckDbCommandInterceptor");

            // Use reflection to access the private _src field to validate connection string
            var srcField = interceptor.GetType().GetField("_src", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var src = (ValueType)srcField.GetValue(interceptor)!; // DuckDbSource is an internal readonly struct
            var connStrProp = src.GetType().GetProperty("ConnectionString", BindingFlags.Public | BindingFlags.Instance)!;
            var connStr = (string)connStrProp.GetValue(src)!;
            Assert.That(connStr, Does.Contain("memory_limit=2GB"));
        }

        [Test]
        public void UseDuckDb_Should_Throw_When_ConnectionString_Missing()
        {
            var options = new DuckDbConnectionOptions(); // ConnectionString left empty
            var builder = new DbContextOptionsBuilder();
            var ex = Assert.Throws<ArgumentException>(() => builder.UseDuckDb(options));
            Assert.That(ex!.ParamName, Is.EqualTo("options"));
        }

        [Test]
        public void UseDuckDb_Should_Throw_When_MemoryLimit_NonPositive()
        {
            var options = new DuckDbConnectionOptions
            {
                ConnectionString = "DataSource=my.db",
                MemoryLimitGB = 0
            };
            var builder = new DbContextOptionsBuilder();
            Assert.Throws<ArgumentOutOfRangeException>(() => builder.UseDuckDb(options));
        }

        // ---------------------------------------------------------------
        // Test infrastructure
        // ---------------------------------------------------------------
        private class ParquetContext : DbContext
        {
            public ParquetContext(DbContextOptions<ParquetContext> options) : base(options) { }
            public DbSet<ParquetItem> Items => Set<ParquetItem>();

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                modelBuilder.Entity<ParquetItem>().ToView("parquet_view").HasNoKey();
            }
        }

        private class ParquetItem
        {
            public long? ID { get; set; }
            public string? Name { get; set; }
            public string? Category { get; set; }
            public double? Price { get; set; }
            public bool? InStock { get; set; }
            public double? Rating { get; set; }
            public DateTime? DateAdded { get; set; }
        }

        // ---------------------------------------------------------------
        // Complex EF Core query tests with interceptor validation
        // ---------------------------------------------------------------
        [Test]
        public void Interceptor_Should_Handle_Where_Clause_With_Parameters()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // Query with parameters to test SQL parameter replacement
            var minPrice = 50.0;
            var items = ctx.Items.Where(i => i.Price > minPrice).ToList();
            
            Assert.That(items.Count, Is.GreaterThan(0));
            Assert.That(items.All(i => i.Price > minPrice), Is.True);
        }

        [Test]
        public void Interceptor_Should_Handle_Complex_Linq_With_Multiple_Conditions()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // Complex query with AND/OR conditions
            var items = ctx.Items
                .Where(i => (i.InStock == true && i.Rating > 4.0) || i.Category == "Electronics")
                .OrderBy(i => i.Price)
                .ToList();
            
            Assert.That(items.Count, Is.GreaterThan(0));
            // Verify ordering
            for (int i = 1; i < items.Count; i++)
            {
                Assert.That(items[i].Price, Is.GreaterThanOrEqualTo(items[i-1].Price));
            }
        }

        [Test]
        public void Interceptor_Should_Handle_String_Operations()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // First check if we have any items with names
            var allItems = ctx.Items.Where(i => i.Name != null).ToList();
            if (allItems.Count == 0)
            {
                Assert.Inconclusive("No items with names in test data");
                return;
            }
            
            // Use a search term from actual data
            var firstItemName = allItems.First().Name;
            var searchTerm = firstItemName.Substring(0, Math.Min(3, firstItemName.Length));
            
            var items = ctx.Items
                .Where(i => i.Name.Contains(searchTerm))
                .Select(i => new { i.ID, i.Name })
                .ToList();
            
            Assert.That(items.Count, Is.GreaterThan(0));
            Assert.That(items.All(i => i.Name.Contains(searchTerm)), Is.True);
        }

        [Test]
        public void Interceptor_Should_Handle_Aggregation_Queries()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // Test various aggregations
            var count = ctx.Items.Count();
            var avgPrice = ctx.Items.Average(i => i.Price);
            var maxRating = ctx.Items.Max(i => i.Rating);
            var minPrice = ctx.Items.Min(i => i.Price);
            var totalPrice = ctx.Items.Sum(i => i.Price);
            
            Assert.That(count, Is.GreaterThan(0));
            Assert.That(avgPrice, Is.GreaterThan(0));
            Assert.That(maxRating, Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(5));
            Assert.That(minPrice, Is.GreaterThanOrEqualTo(0));
            Assert.That(totalPrice, Is.GreaterThan(0));
        }

        [Test]
        public void Interceptor_Should_Handle_GroupBy_Queries()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // Group by category and calculate aggregates
            var categoryStats = ctx.Items
                .GroupBy(i => i.Category)
                .Select(g => new 
                { 
                    Category = g.Key,
                    Count = g.Count(),
                    AvgPrice = g.Average(i => i.Price),
                    MaxRating = g.Max(i => i.Rating)
                })
                .ToList();
            
            Assert.That(categoryStats.Count, Is.GreaterThan(0));
            Assert.That(categoryStats.All(c => c.Count > 0), Is.True);
            Assert.That(categoryStats.All(c => c.AvgPrice > 0), Is.True);
        }

        [Test]
        public void Interceptor_Should_Handle_Null_Comparisons()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // Test null handling
            var itemsWithRating = ctx.Items.Where(i => i.Rating != null).ToList();
            var itemsWithoutRating = ctx.Items.Where(i => i.Rating == null).ToList();
            
            // At least verify the queries execute without error
            Assert.That(itemsWithRating.Count + itemsWithoutRating.Count, Is.GreaterThan(0));
        }

        [Test]
        public void Interceptor_Should_Handle_Date_Comparisons()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            var cutoffDate = new DateTime(2023, 1, 1);
            var recentItems = ctx.Items
                .Where(i => i.DateAdded > cutoffDate)
                .OrderByDescending(i => i.DateAdded)
                .ToList();
            
            // Verify query executes and ordering is correct
            for (int i = 1; i < recentItems.Count; i++)
            {
                Assert.That(recentItems[i].DateAdded, Is.LessThanOrEqualTo(recentItems[i-1].DateAdded));
            }
        }

        [Test]
        public void Interceptor_Should_Handle_Skip_Take_Pagination()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            var pageSize = 5;
            var page1 = ctx.Items.OrderBy(i => i.ID).Take(pageSize).ToList();
            var page2 = ctx.Items.OrderBy(i => i.ID).Skip(pageSize).Take(pageSize).ToList();
            
            Assert.That(page1.Count, Is.LessThanOrEqualTo(pageSize));
            Assert.That(page2.Count, Is.LessThanOrEqualTo(pageSize));
            
            // Ensure no overlap between pages
            if (page1.Any() && page2.Any())
            {
                var page1Ids = page1.Select(i => i.ID).ToHashSet();
                var page2Ids = page2.Select(i => i.ID).ToHashSet();
                Assert.That(page1Ids.Overlaps(page2Ids), Is.False);
            }
        }

        [Test]
        public void Interceptor_Should_Handle_FirstOrDefault_SingleOrDefault()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // FirstOrDefault
            var first = ctx.Items.FirstOrDefault();
            Assert.That(first, Is.Not.Null);
            
            // FirstOrDefault with predicate
            var expensive = ctx.Items.FirstOrDefault(i => i.Price > 1000);
            // May or may not exist, just ensure no exception
            
            // SingleOrDefault with impossible condition (should return null)
            var impossible = ctx.Items.SingleOrDefault(i => i.ID == -999);
            Assert.That(impossible, Is.Null);
        }

        [Test]
        public void Interceptor_Should_Handle_Boolean_Projection_From_Subquery()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // This tests the DbDataReaderCustomCasting boolean conversion
            // EF Core often generates CASE WHEN EXISTS(...) THEN 1 ELSE 0 END
            var projections = ctx.Items
                .Select(i => new 
                {
                    i.ID,
                    i.Name,
                    IsExpensive = i.Price > 100,
                    IsHighRated = i.Rating > 4.5,
                    IsAvailable = i.InStock == true
                })
                .ToList();
            
            Assert.That(projections.Count, Is.GreaterThan(0));
            // Verify boolean projections work correctly
            foreach (var item in projections)
            {
                Assert.That(item.IsExpensive, Is.TypeOf<bool>());
                Assert.That(item.IsHighRated, Is.TypeOf<bool>());
                Assert.That(item.IsAvailable, Is.TypeOf<bool>());
            }
        }

        [Test]
        public void Interceptor_Should_Handle_Select_Many_And_Joins()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath);
            
            using var ctx = new ParquetContext(builder.Options);
            
            // Self-join to find items in same category
            var sameCategoryPairs = (from i1 in ctx.Items
                                    from i2 in ctx.Items
                                    where i1.Category == i2.Category && i1.ID < i2.ID
                                    select new { Item1 = i1.Name, Item2 = i2.Name, Category = i1.Category })
                                    .Take(10)
                                    .ToList();
            
            // Just ensure query executes
            Assert.That(sameCategoryPairs, Is.Not.Null);
        }

        [Test]
        public void FileSearchPath_Should_Be_Applied_When_Configured()
        {
            var options = new DuckDbConnectionOptions
            {
                ConnectionString = "DataSource=:memory:",
                FileSearchPath = "/some/path"
            };
            
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDb(options);
            
            var coreExt = builder.Options.Extensions.OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>().Single();
            var interceptor = coreExt.Interceptors.Single(i => i.GetType().Name == "DuckDbCommandInterceptor");
            
            // Verify FileSearchPath is stored in DuckDbSource
            var srcField = interceptor.GetType().GetField("_src", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var src = srcField.GetValue(interceptor)!;
            var fileSearchPathProp = src.GetType().GetProperty("FileSearchPath", BindingFlags.Public | BindingFlags.Instance)!;
            var fileSearchPath = (string?)fileSearchPathProp.GetValue(src);
            
            Assert.That(fileSearchPath, Is.EqualTo("/some/path"));
        }

        [Test]
        public void UseDuckDb_With_Action_Overload_Should_Configure_Options()
        {
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDb(opts =>
            {
                opts.ConnectionString = "DataSource=:memory:";
                opts.MemoryLimitGB = 4;
                opts.FileSearchPath = "/test/path";
            });
            
            var coreExt = builder.Options.Extensions.OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>().Single();
            var interceptor = coreExt.Interceptors.Single(i => i.GetType().Name == "DuckDbCommandInterceptor");
            
            // Verify all options were applied
            var srcField = interceptor.GetType().GetField("_src", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var src = srcField.GetValue(interceptor)!;
            
            var connStrProp = src.GetType().GetProperty("ConnectionString", BindingFlags.Public | BindingFlags.Instance)!;
            var connStr = (string)connStrProp.GetValue(src)!;
            Assert.That(connStr, Does.Contain("memory_limit=4GB"));
            
            var fileSearchPathProp = src.GetType().GetProperty("FileSearchPath", BindingFlags.Public | BindingFlags.Instance)!;
            var fileSearchPath = (string?)fileSearchPathProp.GetValue(src);
            Assert.That(fileSearchPath, Is.EqualTo("/test/path"));
        }

        [Test]
        public void UseDuckDbOnParquet_NoTrackingFalse_Should_Enable_Tracking()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath, noTracking: false);

            var coreExt = builder.Options.Extensions
                                   .OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>()
                                   .Single();
            Assert.That(coreExt.QueryTrackingBehavior, Is.EqualTo(QueryTrackingBehavior.TrackAll));
        }

        [Test]
        public void UseDuckDb_NoTrackingFalse_Should_Enable_Tracking()
        {
            var optionsObj = new DuckDbConnectionOptions
            {
                ConnectionString = "DataSource=:memory:",
                NoTracking = false
            };

            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDb(optionsObj);

            var coreExt = builder.Options.Extensions
                                   .OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>()
                                   .Single();
            Assert.That(coreExt.QueryTrackingBehavior, Is.EqualTo(QueryTrackingBehavior.TrackAll));
        }

        [Test]
        public void UseDuckDbOnParquet_Should_Apply_SqliteOptionsAction()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<ParquetContext>();
            builder.UseDuckDbOnParquet(parquetPath, sqliteOptionsAction: s => s.CommandTimeout(77));

            // Locate the internal SqliteOptionsExtension via reflection
            var sqliteExt = builder.Options.Extensions
                                   .Single(e => e.GetType().Name.Contains("SqliteOptionsExtension"));

            var timeoutProp = sqliteExt.GetType().GetProperty("CommandTimeout", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            var timeoutVal = (int?)timeoutProp?.GetValue(sqliteExt);

            Assert.That(timeoutVal, Is.EqualTo(77));
        }
    }
}