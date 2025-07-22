using DuckDB.NET.Data;
using EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration;
using EnergyExemplar.EntityFrameworkCore.DuckDb.Interceptors;
using EnergyExemplar.Extensions.DuckDb.Internals;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using System.Data;
using System.Data.Common;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb
{
    // Record representing how we connect to DuckDB
    internal readonly record struct DuckDbSource(
        string ConnectionString,
        string? ParquetFile = null,
        string? FileSearchPath = null,
        MultiParquetConfiguration? MultiParquetConfig = null);

    /// <summary>
    /// A contract for holding all DuckDB connection and configuration settings.
    /// This can be constructed manually or bound from configuration (e.g., appsettings.json).
    /// </summary>
    public class DuckDbConnectionOptions
    {
        /// <summary>
        /// The full DuckDB connection string.
        /// e.g., "DataSource=my.db" or "DataSource=:memory:"
        /// </summary>
        public string ConnectionString { get; set; } = string.Empty;

        /// <summary>
        /// Optional: The base path for resolving relative paths in a .ddb file's views.
        /// </summary>
        public string? FileSearchPath { get; set; }

        /// <summary>
        /// Optional: A memory limit for DuckDB, specified in gigabytes.
        /// </summary>
        public int? MemoryLimitGB { get; set; }

        /// <summary>
        /// Optional: Specifies whether to use NoTracking queries by default. Defaults to true.
        /// </summary>
        public bool NoTracking { get; set; } = true;
    }

    /// <summary>
    /// Public entry-point – mirrors UseSqlite / UseSqlServer.
    /// </summary>
    public static class DuckDbOptionsExtensions
    {
        /// <summary>
        /// Connect to a DuckDB source using a dedicated configuration object.
        /// This is the primary overload and is ideal for configuration-driven applications.
        /// </summary>
        public static DbContextOptionsBuilder UseDuckDb(
            this DbContextOptionsBuilder builder,
            DuckDbConnectionOptions options,
            Action<Microsoft.EntityFrameworkCore.Infrastructure.SqliteDbContextOptionsBuilder>? sqliteOptionsAction = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(options);
            if (string.IsNullOrWhiteSpace(options.ConnectionString))
                throw new ArgumentException("ConnectionString is required in the options.", nameof(options));

            var finalConnectionString = options.ConnectionString;
            if (options.MemoryLimitGB.HasValue)
            {
                if (options.MemoryLimitGB.Value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(options.MemoryLimitGB), "Memory limit must be a positive number.");

                if (!finalConnectionString.Contains("memory_limit", StringComparison.OrdinalIgnoreCase))
                {
                    finalConnectionString = $"{finalConnectionString.TrimEnd(';')};memory_limit={options.MemoryLimitGB.Value}GB";
                }
            }

            var src = new DuckDbSource(finalConnectionString, null, options.FileSearchPath);
            return Configure(builder, src, options.NoTracking, sqliteOptionsAction);
        }

        /// <summary>
        /// Convenience overload to connect to a DuckDB source via a configuration action.
        /// </summary>
        public static DbContextOptionsBuilder UseDuckDb(
            this DbContextOptionsBuilder builder,
            Action<DuckDbConnectionOptions> configureOptions,
            Action<Microsoft.EntityFrameworkCore.Infrastructure.SqliteDbContextOptionsBuilder>? sqliteOptionsAction = null)
        {
            ArgumentNullException.ThrowIfNull(configureOptions, nameof(configureOptions));
            var options = new DuckDbConnectionOptions();
            configureOptions(options);
            return builder.UseDuckDb(options, sqliteOptionsAction);
        }

        /// <summary>
        /// Treat a single parquet file as a DuckDB data source.
        /// </summary>
        public static DbContextOptionsBuilder UseDuckDbOnParquet(
            this DbContextOptionsBuilder builder,
            string parquetFileFullPath,
            bool noTracking = true,
            Action<Microsoft.EntityFrameworkCore.Infrastructure.SqliteDbContextOptionsBuilder>? sqliteOptionsAction = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            if (!parquetFileFullPath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Must be a .parquet file", nameof(parquetFileFullPath));

            var conn = "DataSource=:memory:"; // keep DB in-memory and read parquet via view
            var src = new DuckDbSource(conn, parquetFileFullPath);
            return Configure(builder, src, noTracking, sqliteOptionsAction);
        }

        /// <summary>
        /// Connect to multiple parquet files with relationships as a DuckDB data source.
        /// This allows for navigation properties across multiple parquet files.
        /// </summary>
        public static DbContextOptionsBuilder UseDuckDbOnMultipleParquet(
            this DbContextOptionsBuilder builder,
            MultiParquetConfiguration configuration,
            bool noTracking = true,
            Action<Microsoft.EntityFrameworkCore.Infrastructure.SqliteDbContextOptionsBuilder>? sqliteOptionsAction = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(configuration);

            var conn = "DataSource=:memory:"; // keep DB in-memory and read parquet via views
            var src = new DuckDbSource(conn, null, null, configuration);
            return Configure(builder, src, noTracking, sqliteOptionsAction);
        }

        /// <summary>
        /// Connect to multiple parquet files with relationships as a DuckDB data source using a configuration action.
        /// </summary>
        public static DbContextOptionsBuilder UseDuckDbOnMultipleParquet(
            this DbContextOptionsBuilder builder,
            Action<MultiParquetConfiguration> configureConfiguration,
            bool noTracking = true,
            Action<Microsoft.EntityFrameworkCore.Infrastructure.SqliteDbContextOptionsBuilder>? sqliteOptionsAction = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(configureConfiguration);

            var configuration = new MultiParquetConfiguration();
            configureConfiguration(configuration);
            return builder.UseDuckDbOnMultipleParquet(configuration, noTracking, sqliteOptionsAction);
        }

        // internal shared config helper
        private static DbContextOptionsBuilder Configure(DbContextOptionsBuilder b, DuckDbSource src, bool noTrack,
            Action<Microsoft.EntityFrameworkCore.Infrastructure.SqliteDbContextOptionsBuilder>? sqliteOptionsAction)
        {
            // EF **requires** that a database provider be specified on the options builder.  It refuses to
            // start without one and throws:
            //   System.InvalidOperationException: No database provider has been configured for this DbContext.
            //
            // We register the lightweight Sqlite provider as a _placeholder_ only so that this check passes.
            //   • It ships with EF Core – no extra NuGet is needed.
            //   • Pointing at "DataSource=:memory:" keeps it fully in-memory; no files are touched.
            //   • The provider is never actually used at runtime because our interceptor captures the SQL
            //     **after** EF Core has generated it and executes the query against DuckDB instead.
            //
            // If EF Core ever gains a first-class DuckDB provider you can swap this line for that and remove
            // the placeholder.
            //
            // Note: Because we build the interceptor by hand and attach it directly (`AddInterceptors(...)`),
            // we don't use an `IDbContextOptionsExtension` → `ApplyServices` method.  The simple one-liner here
            // is enough.
            b.UseSqlite("DataSource=:memory:", sqliteOptionsAction); // ← placeholder provider
            if (noTrack) b.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);

            b.AddInterceptors(new DuckDbCommandInterceptor(src));
            return b;
        }
    }

    /// <summary>
    /// Intercepts EF SQL and executes it against DuckDB.
    /// Wraps the raw reader with <see cref="DbDataRaederCustomCasting"/> to smooth OData-related type mismatches.
    /// </summary>
    internal sealed class DuckDbCommandInterceptor : DbCommandInterceptor
    {
        private readonly DuckDbSource _src;
        private readonly ParquetTableResolver? _tableResolver;

        public DuckDbCommandInterceptor(DuckDbSource src)
        {
            _src = src;
            _tableResolver = src.MultiParquetConfig != null ? new ParquetTableResolver(src.MultiParquetConfig) : null;
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            var reader = ExecuteAsync(command, false).GetAwaiter().GetResult();
            return InterceptionResult<DbDataReader>.SuppressWithResult(reader);
        }

        public override async ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            var reader = await ExecuteAsync(command, true, cancellationToken);
            return InterceptionResult<DbDataReader>.SuppressWithResult(reader);
        }

        private async Task<DbDataReader> ExecuteAsync(DbCommand efCommand, bool async, CancellationToken token = default)
        {
            string sql = BuildFinalSql(efCommand);

            var conn = new DuckDBConnection(_src.ConnectionString);
            try
            {
                if (async)
                    await conn.OpenAsync(token);
                else
                    conn.Open();

                // apply file_search_path if supplied
                if (!string.IsNullOrWhiteSpace(_src.FileSearchPath))
                {
                    using var setPath = conn.CreateCommand();
                    setPath.CommandText = $"set file_search_path = '{_src.FileSearchPath.Replace("'", "''")}';";
                    if (async) await setPath.ExecuteNonQueryAsync(token); else setPath.ExecuteNonQuery();
                }

                // create views for parquet files – done per-connection; cheap and avoids race concerns
                if (_src.ParquetFile is not null)
                {
                    // Single parquet file mode
                    using var createView = conn.CreateCommand();
                    string fileEsc = _src.ParquetFile.Replace("'", "''");
                    // Derive view name from the parquet file name (without extension)
                    string viewName = System.IO.Path.GetFileNameWithoutExtension(_src.ParquetFile);
                    // Quote the view name to preserve case and handle special characters
                    createView.CommandText = $"CREATE OR REPLACE VIEW \"{viewName}\" AS SELECT * FROM read_parquet('{fileEsc}');";
                    if (async) await createView.ExecuteNonQueryAsync(token); else createView.ExecuteNonQuery();
                }
                else if (_tableResolver is not null)
                {
                    // Multi-parquet file mode
                    await _tableResolver.CreateViewsAsync(conn, token);
                }

                var duckCmd = conn.CreateCommand();
                duckCmd.CommandText = sql;

                // By using CommandBehavior.CloseConnection, we ensure that when EF Core disposes the
                // DataReader, the underlying DuckDB connection is automatically closed and disposed,
                // preventing resource leaks. Our custom DbDataRaederCustomCasting wrapper correctly
                // passes the Dispose call down to the underlying reader.
                DbDataReader rawReader = async
                    ? await duckCmd.ExecuteReaderAsync(CommandBehavior.CloseConnection, token)
                    : duckCmd.ExecuteReader(CommandBehavior.CloseConnection);

                // wrap for OData bool casting quirks
                var wrapped = new DbDataReaderCustomCasting(rawReader);
                return wrapped;
            }
            catch
            {
                // Ensure connection is disposed if we fail before creating the reader
                conn?.Dispose();
                throw;
            }
        }

        #region SQL parameter replacement helpers
        private static string BuildFinalSql(DbCommand command)
        {
            string sql = command.CommandText;

            // inline parameters first
            var parameters = command.Parameters.Cast<DbParameter>()
                                              .OrderByDescending(p => p.ParameterName.Length);
            foreach (var p in parameters)
                sql = sql.Replace(p.ParameterName, Format(p));

            // Run through translation pipeline
            sql = RewritePipeline.Default.Rewrite(sql);

            return sql;
        }

        private static string Format(DbParameter p)
        {
            if (p.Value is null || p.Value == DBNull.Value) return "NULL";

            return p.DbType switch
            {
                DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength
                    => $"'{p.Value.ToString()!.Replace("'", "''")}'",
                DbType.Boolean => (bool)p.Value ? "true" : "false",
                DbType.Date or DbType.DateTime or DbType.DateTime2
                    => $"'{((DateTime)p.Value):yyyy-MM-dd HH:mm:ss.fff}'",
                _ => p.Value.ToString() ?? "NULL"
            };
        }
        #endregion

        // intercept scalar queries (e.g., Any(), Count())
        public override InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
        {
            using var reader = ExecuteAsync(command, false).GetAwaiter().GetResult();
            object? val = reader.Read() ? reader.GetValue(0) : null;
            return InterceptionResult<object>.SuppressWithResult(val!);
        }

        public override async ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result, CancellationToken cancellationToken = default)
        {
            await using var reader = await ExecuteAsync(command, true, cancellationToken);
            object? val = await reader.ReadAsync(cancellationToken) ? reader.GetValue(0) : null;
            return InterceptionResult<object>.SuppressWithResult(val!);
        }

        // block write operations – this interceptor is read-only by design
        public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
            => throw new NotSupportedException("DuckDb interceptor is read-only – write operations are not supported.");

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
            => throw new NotSupportedException("DuckDb interceptor is read-only – write operations are not supported.");
    }
}