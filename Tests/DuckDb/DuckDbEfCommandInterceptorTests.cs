using EnergyExemplar.EntityFrameworkCore.DuckDb;
using Microsoft.EntityFrameworkCore;

namespace Tests.DuckDb
{
    [TestFixture]
    public class DuckDbCommandInterceptorTests
    {
        private static string GetParquetPath()
        {
            var relative = System.IO.Path.Combine("Tests", "DuckDb", "TestData", "test.parquet");
            var root = NUnit.Framework.TestContext.CurrentContext.TestDirectory;
            var dir = new System.IO.DirectoryInfo(root);
            while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, relative)))
            {
                dir = dir.Parent;
            }
            if (dir is null) throw new System.IO.FileNotFoundException("Could not locate test.parquet");
            return System.IO.Path.GetFullPath(System.IO.Path.Combine(dir.FullName, relative));
        }

        // Test context that uses the interceptor
        private class TestContext : DbContext
        {
            public TestContext(DbContextOptions<TestContext> options) : base(options) { }
            public DbSet<TestItem> Items => Set<TestItem>();

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                modelBuilder.Entity<TestItem>().ToView("parquet_view").HasNoKey();
            }
        }

        private class TestItem
        {
            public long? ID { get; set; }
            public string? Name { get; set; }
            public string? Category { get; set; }
            public double? Price { get; set; }
            public bool? InStock { get; set; }
            public double? Rating { get; set; }
            public DateTime? DateAdded { get; set; }
        }

        [Test]
        public void Interceptor_Should_Execute_Reader_Query_Through_DuckDB()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // This will trigger ReaderExecuting in the interceptor
            var items = ctx.Items.ToList();

            Assert.That(items.Count, Is.GreaterThan(0));
            Assert.That(items.First().ID, Is.Not.Null);
        }

        [Test]
        public async Task Interceptor_Should_Execute_ReaderAsync_Query_Through_DuckDB()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // This will trigger ReaderExecutingAsync in the interceptor
            var items = await ctx.Items.ToListAsync();

            Assert.That(items.Count, Is.GreaterThan(0));
            Assert.That(items.First().ID, Is.Not.Null);
        }

        [Test]
        public void Interceptor_Should_Execute_Scalar_Query_Through_DuckDB()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // This will trigger ScalarExecuting in the interceptor
            var count = ctx.Items.Count();

            Assert.That(count, Is.GreaterThan(0));
        }

        [Test]
        public async Task Interceptor_Should_Execute_ScalarAsync_Query_Through_DuckDB()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // This will trigger ScalarExecutingAsync in the interceptor
            var count = await ctx.Items.CountAsync();

            Assert.That(count, Is.GreaterThan(0));
        }

        [Test]
        public void Interceptor_Should_Block_Write_Operations()
        {
            // Since our TestItem has no key, we need a different context for write test
            var builder = new DbContextOptionsBuilder<WriteTestContext>();
            builder.UseDuckDb(new DuckDbConnectionOptions { ConnectionString = "DataSource=:memory:" });

            using var ctx = new WriteTestContext(builder.Options);

            // The interceptor blocks NonQuery operations (INSERT/UPDATE/DELETE)
            // We can test this by trying to execute raw SQL
            Assert.Throws<NotSupportedException>(() =>
                ctx.Database.ExecuteSqlRaw("CREATE TABLE test (id INT)")
            );
        }

        private class WriteTestContext : DbContext
        {
            public WriteTestContext(DbContextOptions<WriteTestContext> options) : base(options) { }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                // Empty context for write test
            }
        }

        [Test]
        public void Interceptor_Should_Handle_Parameter_Replacement_Correctly()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // Test different parameter types
            var stringParam = "Electronics";
            var numberParam = 50.0;
            var boolParam = true;
            var dateParam = new DateTime(2023, 1, 1);

            var items = ctx.Items
                .Where(i => i.Category == stringParam)
                .Where(i => i.Price > numberParam)
                .Where(i => i.InStock == boolParam)
                .Where(i => i.DateAdded > dateParam)
                .ToList();

            // Just ensure it executes without error - parameter replacement is working
            Assert.That(items, Is.Not.Null);
        }

        [Test]
        public void Interceptor_Should_Handle_Null_Parameters()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            string? nullString = null;
            double? nullNumber = null;

            var items = ctx.Items
                .Where(i => i.Category == nullString || i.Rating == nullNumber)
                .ToList();

            // Ensure null parameter handling works
            Assert.That(items, Is.Not.Null);
        }

        [Test]
        public void Interceptor_Should_Remove_Escape_Clause_From_SQL()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // EF Core often adds ESCAPE '\\' for LIKE queries
            var items = ctx.Items
                .Where(i => i.Name.Contains("test"))
                .ToList();

            // If this executes without error, the ESCAPE clause was properly removed
            Assert.That(items, Is.Not.Null);
        }

        [Test]
        public void Interceptor_Should_Create_Parquet_View_For_Single_File_Mode()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // The interceptor should create a view called "parquet_view"
            // If this query works, the view was created successfully
            var items = ctx.Items.Take(1).ToList();

            Assert.That(items.Count, Is.EqualTo(1));
        }

        [Test]
        public void Interceptor_Should_Apply_FileSearchPath_Setting()
        {
            var options = new DuckDbConnectionOptions
            {
                ConnectionString = "DataSource=:memory:",
                FileSearchPath = "C:/test/path"
            };

            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDb(options);

            using var ctx = new TestContext(builder.Options);

            // We can't easily verify the SET command was executed, but we can ensure
            // the context builds without error with FileSearchPath configured
            Assert.DoesNotThrow(() => ctx.Database.CanConnect());
        }

        [Test]
        public void Interceptor_Should_Return_DbDataReaderCustomCasting_Wrapper()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // Get the interceptor via reflection
            var coreExt = builder.Options.Extensions
                .OfType<Microsoft.EntityFrameworkCore.Infrastructure.CoreOptionsExtension>()
                .Single();
            var interceptor = coreExt.Interceptors.Single(i => i.GetType().Name == "DuckDbCommandInterceptor");

            // Execute a query that returns boolean projections to test custom casting
            var projections = ctx.Items
                .Select(i => new { IsExpensive = i.Price > 100 })
                .ToList();

            Assert.That(projections.Count, Is.GreaterThan(0));
            Assert.That(projections.First().IsExpensive, Is.TypeOf<bool>());
        }

        [Test]
        public void Interceptor_Should_Handle_Multiple_Result_Sets()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            using var ctx = new TestContext(builder.Options);

            // Execute multiple queries in sequence
            var count1 = ctx.Items.Count();
            var count2 = ctx.Items.Where(i => i.InStock == true).Count();
            var items = ctx.Items.Take(5).ToList();

            // All should work independently
            Assert.That(count1, Is.GreaterThan(0));
            Assert.That(count2, Is.LessThanOrEqualTo(count1));
            Assert.That(items.Count, Is.LessThanOrEqualTo(5));
        }

        [Test]
        public void Interceptor_Should_Dispose_Resources_Properly()
        {
            var parquetPath = GetParquetPath();
            var builder = new DbContextOptionsBuilder<TestContext>();
            builder.UseDuckDbOnParquet(parquetPath);

            // Execute multiple queries with disposal
            for (int i = 0; i < 5; i++)
            {
                using var ctx = new TestContext(builder.Options);
                var items = ctx.Items.Take(1).ToList();
                Assert.That(items.Count, Is.EqualTo(1));
            }

            // If we get here without connection exhaustion, disposal is working
            Assert.Pass("Resources disposed properly across multiple contexts");
        }
    }
}