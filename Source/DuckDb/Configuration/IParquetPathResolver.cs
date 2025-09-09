using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Text.RegularExpressions;

namespace EnergyExemplar.EntityFrameworkCore.DuckDb.Configuration
{
    /// <summary>
    /// Result of path resolution with success status and detailed error information.
    /// </summary>
    public record PathResolutionResult(
        string ResolvedPath,
        bool Success,
        IReadOnlyList<string> Errors,
        IReadOnlyList<string> Warnings)
    {
        public static PathResolutionResult CreateSuccess(string resolvedPath, IReadOnlyList<string>? warnings = null)
            => new(resolvedPath, true, Array.Empty<string>(), warnings ?? Array.Empty<string>());
            
        public static PathResolutionResult CreateFailure(IReadOnlyList<string> errors, IReadOnlyList<string>? warnings = null)
            => new("", false, errors, warnings ?? Array.Empty<string>());
    }

    /// <summary>
    /// Context information for path resolution.
    /// </summary>
    public record PathResolutionContext(
        Type EntityType,
        string Environment,
        IReadOnlyDictionary<string, string> Variables,
        IConfiguration? Configuration);

    /// <summary>
    /// Interface for resolving specific placeholder types.
    /// </summary>
    public interface IPlaceholderResolver
    {
        /// <summary>
        /// Determines if this resolver can handle the given placeholder.
        /// </summary>
        bool CanResolve(string placeholder);
        
        /// <summary>
        /// Resolves the placeholder value.
        /// </summary>
        string Resolve(string placeholder, PathResolutionContext context);
    }

    /// <summary>
    /// Interface for resolving parquet file paths dynamically based on templates and environment variables.
    /// This allows for environment-specific path resolution (dev, test, prod) and template substitution.
    /// </summary>
    public interface IParquetPathResolver
    {
        /// <summary>
        /// Resolves a path template for a specific entity type.
        /// </summary>
        /// <typeparam name="T">The entity type</typeparam>
        /// <param name="template">The path template with placeholders like {env}, {EntityName}, etc.</param>
        /// <param name="environment">Optional environment override</param>
        /// <returns>The resolved absolute file path</returns>
        string ResolvePath<T>(string template, string? environment = null);

        /// <summary>
        /// Resolves a path template for a specific entity type.
        /// </summary>
        /// <param name="entityType">The entity type</param>
        /// <param name="template">The path template with placeholders like {env}, {EntityName}, etc.</param>
        /// <param name="environment">Optional environment override</param>
        /// <returns>The resolved absolute file path</returns>
        string ResolvePath(Type entityType, string template, string? environment = null);
        
        /// <summary>
        /// Attempts to resolve a path template with detailed error reporting.
        /// </summary>
        PathResolutionResult TryResolvePath<T>(string template, string? environment = null);
        
        /// <summary>
        /// Attempts to resolve a path template with detailed error reporting.
        /// </summary>
        PathResolutionResult TryResolvePath(Type entityType, string template, string? environment = null);
    }

    /// <summary>
    /// Resolves environment placeholders like {env}.
    /// </summary>
    public class EnvironmentPlaceholderResolver : IPlaceholderResolver
    {
        public bool CanResolve(string placeholder) => 
            placeholder.Equals("env", StringComparison.OrdinalIgnoreCase);
        
        public string Resolve(string placeholder, PathResolutionContext context) => 
            context.Environment;
    }
    
    /// <summary>
    /// Resolves entity name placeholders like {EntityName}.
    /// </summary>
    public class EntityNamePlaceholderResolver : IPlaceholderResolver
    {
        public bool CanResolve(string placeholder) => 
            placeholder.Equals("EntityName", StringComparison.OrdinalIgnoreCase);
        
        public string Resolve(string placeholder, PathResolutionContext context) => 
            context.EntityType.Name;
    }
    
    /// <summary>
    /// Resolves date placeholders like {Date:yyyy-MM-dd}.
    /// </summary>
    public class DatePlaceholderResolver : IPlaceholderResolver
    {
        public bool CanResolve(string placeholder) => 
            placeholder.StartsWith("Date:", StringComparison.OrdinalIgnoreCase);
        
        public string Resolve(string placeholder, PathResolutionContext context)
        {
            var format = placeholder.Substring(5); // Remove "Date:"
            return DateTime.Now.ToString(format);
        }
    }
    
    /// <summary>
    /// Resolves custom variable placeholders.
    /// </summary>
    public class CustomVariablePlaceholderResolver : IPlaceholderResolver
    {
        public bool CanResolve(string placeholder) => true; // Fallback resolver
        
        public string Resolve(string placeholder, PathResolutionContext context)
        {
            return context.Variables.TryGetValue(placeholder, out var value) ? value : $"{{{placeholder}}}";
        }
    }
    
    /// <summary>
    /// Resolves configuration placeholders like {Config:SomeKey}.
    /// </summary>
    public class ConfigurationPlaceholderResolver : IPlaceholderResolver
    {
        public bool CanResolve(string placeholder) => 
            placeholder.StartsWith("Config:", StringComparison.OrdinalIgnoreCase);
        
        public string Resolve(string placeholder, PathResolutionContext context)
        {
            if (context.Configuration == null) 
                return $"{{{placeholder}}}";
                
            var configKey = placeholder.Substring(7); // Remove "Config:"
            var configValue = context.Configuration[configKey];
            return !string.IsNullOrEmpty(configValue) ? configValue : $"{{{placeholder}}}";
        }
    }

    /// <summary>
    /// Enhanced implementation of IParquetPathResolver with security, performance, and comprehensive error handling.
    /// Supports templates like: "s3://bucket-{env}/data/{EntityName}.parquet", "C:/data/{env}/{EntityName}.parquet"
    /// </summary>
    public class EnvironmentParquetPathResolver : IParquetPathResolver
    {
        private readonly string _defaultEnvironment;
        private readonly Dictionary<string, string> _variables;
        private readonly IConfiguration? _configuration;
        private readonly List<IPlaceholderResolver> _placeholderResolvers;
        
        // Compiled regex for better performance
        private static readonly Regex PlaceholderRegex = new(
            @"\{([^}]+)\}", 
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Initializes a new instance of EnvironmentParquetPathResolver.
        /// </summary>
        /// <param name="defaultEnvironment">Default environment to use when not specified</param>
        /// <param name="variables">Custom variables for template substitution</param>
        /// <param name="configuration">Optional IConfiguration for reading environment variables</param>
        public EnvironmentParquetPathResolver(
            string defaultEnvironment = "prod", 
            Dictionary<string, string>? variables = null,
            IConfiguration? configuration = null)
        {
            if (string.IsNullOrWhiteSpace(defaultEnvironment))
                throw new ArgumentException("Default environment cannot be null or empty", nameof(defaultEnvironment));
                
            _defaultEnvironment = SanitizeInput(defaultEnvironment);
            _variables = SanitizeVariables(variables ?? new Dictionary<string, string>());
            _configuration = configuration;
            
            // Initialize placeholder resolvers in order of precedence
            _placeholderResolvers = new List<IPlaceholderResolver>
            {
                new EnvironmentPlaceholderResolver(),
                new EntityNamePlaceholderResolver(),
                new DatePlaceholderResolver(),
                new ConfigurationPlaceholderResolver(),
                new CustomVariablePlaceholderResolver() // Fallback
            };
        }

        /// <summary>
        /// Resolves a path template for a specific entity type.
        /// </summary>
        public string ResolvePath<T>(string template, string? environment = null)
            => ResolvePath(typeof(T), template, environment);

        /// <summary>
        /// Resolves a path template for a specific entity type.
        /// </summary>
        public string ResolvePath(Type entityType, string template, string? environment = null)
        {
            var result = TryResolvePath(entityType, template, environment);
            if (!result.Success)
            {
                throw new InvalidOperationException($"Path resolution failed: {string.Join(", ", result.Errors)}");
            }
            return result.ResolvedPath;
        }
        
        /// <summary>
        /// Attempts to resolve a path template with detailed error reporting.
        /// </summary>
        public PathResolutionResult TryResolvePath<T>(string template, string? environment = null)
            => TryResolvePath(typeof(T), template, environment);
        
        /// <summary>
        /// Attempts to resolve a path template with detailed error reporting.
        /// </summary>
        public PathResolutionResult TryResolvePath(Type entityType, string template, string? environment = null)
        {
            var errors = new List<string>();
            var warnings = new List<string>();
            
            try
            {
                // Input validation
                if (entityType == null)
                {
                    errors.Add("Entity type cannot be null");
                    return PathResolutionResult.CreateFailure(errors);
                }
                
                if (string.IsNullOrWhiteSpace(template))
                {
                    errors.Add("Template cannot be null or empty");
                    return PathResolutionResult.CreateFailure(errors);
                }
                
                var sanitizedTemplate = SanitizeTemplate(template);
                if (sanitizedTemplate != template)
                {
                    warnings.Add($"Template was sanitized from '{template}' to '{sanitizedTemplate}'");
                }
                
                var env = SanitizeInput(environment ?? _defaultEnvironment);
                var result = sanitizedTemplate;
                
                // Create resolution context
                var context = new PathResolutionContext(
                    entityType, 
                    env, 
                    _variables.AsReadOnly(), 
                    _configuration);
                
                // Find and replace all placeholders
                var matches = PlaceholderRegex.Matches(result);
                var unresolvedPlaceholders = new List<string>();
                
                foreach (Match match in matches)
                {
                    var placeholder = match.Groups[1].Value;
                    var resolved = false;
                    
                    foreach (var resolver in _placeholderResolvers)
                    {
                        if (resolver.CanResolve(placeholder))
                        {
                            try
                            {
                                var resolvedValue = resolver.Resolve(placeholder, context);
                                if (resolvedValue != $"{{{placeholder}}}") // Not unresolved marker
                                {
                                    result = result.Replace(match.Value, SanitizePathComponent(resolvedValue));
                                    resolved = true;
                                    break;
                                }
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"Failed to resolve placeholder '{placeholder}': {ex.Message}");
                            }
                        }
                    }
                    
                    if (!resolved)
                    {
                        unresolvedPlaceholders.Add(placeholder);
                    }
                }
                
                if (unresolvedPlaceholders.Any())
                {
                    warnings.Add($"Unresolved placeholders: {string.Join(", ", unresolvedPlaceholders)}");
                }
                
                // Normalize the final path
                var finalPath = IsUrl(result) ? result : NormalizeLocalPath(result);
                
                return errors.Any() 
                    ? PathResolutionResult.CreateFailure(errors, warnings)
                    : PathResolutionResult.CreateSuccess(finalPath, warnings);
            }
            catch (Exception ex)
            {
                errors.Add($"Unexpected error during path resolution: {ex.Message}");
                return PathResolutionResult.CreateFailure(errors, warnings);
            }
        }

        private static bool IsUrl(string path)
        {
            return path.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || 
                   path.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
                   path.StartsWith("s3://", StringComparison.OrdinalIgnoreCase) ||
                   path.StartsWith("ftp://", StringComparison.OrdinalIgnoreCase);
        }

        private static string NormalizeLocalPath(string path)
        {
            try
            {
                // Use Path.GetFullPath for proper normalization
                return Path.GetFullPath(path);
            }
            catch
            {
                // Fallback to manual normalization if Path.GetFullPath fails
                var parts = path.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries);
                
                // Handle absolute paths on Windows (C:, D:, etc.)
                if (parts.Length > 0 && parts[0].Length == 2 && parts[0].EndsWith(':'))
                {
                    // Reconstruct as absolute path
                    return parts.Length > 1 
                        ? Path.Combine(parts[0] + Path.DirectorySeparatorChar, Path.Combine(parts.Skip(1).ToArray()))
                        : parts[0] + Path.DirectorySeparatorChar;
                }
                
                // Handle relative paths or absolute Unix paths
                if (path.StartsWith('/'))
                {
                    // Unix absolute path - preserve the leading slash
                    return Path.DirectorySeparatorChar + (parts.Length > 0 ? Path.Combine(parts) : "");
                }
                
                // Relative path
                return parts.Length > 1 ? Path.Combine(parts) : (parts.Length == 1 ? parts[0] : path);
            }
        }

        /// <summary>
        /// Adds or updates a custom variable for template substitution.
        /// </summary>
        /// <param name="key">The variable key (without braces)</param>
        /// <param name="value">The variable value</param>
        public void SetVariable(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Variable key cannot be null or empty", nameof(key));
            
            _variables[SanitizeInput(key)] = SanitizePathComponent(value ?? "");
        }

        /// <summary>
        /// Removes a custom variable.
        /// </summary>
        /// <param name="key">The variable key to remove</param>
        public void RemoveVariable(string key)
        {
            if (!string.IsNullOrWhiteSpace(key))
            {
                _variables.Remove(key);
            }
        }

        /// <summary>
        /// Gets all custom variables.
        /// </summary>
        public IReadOnlyDictionary<string, string> Variables => _variables.AsReadOnly();
        
        // Security and validation methods
        private static string SanitizeInput(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return "";
                
            return input.Trim();
        }
        
        private static string SanitizeTemplate(string template)
        {
            if (string.IsNullOrWhiteSpace(template))
                return "";
                
            // Remove null bytes and other dangerous characters
            return template.Replace("\0", "").Trim();
        }
        
        private static string SanitizePathComponent(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "";
                
            // Prevent path traversal attacks
            value = value.Replace("..", "").Replace("~", "");
            
            // Remove dangerous characters for file paths (but allow URL schemes and Windows drive letters)
            if (!IsUrl(value))
            {
                // Preserve Windows drive letters (C:, D:, etc.) but remove other colons
                bool hasWindowsDriveLetter = value.Length >= 2 && char.IsLetter(value[0]) && value[1] == ':';
                
                var invalidChars = new[] { '<', '>', '"', '|', '?', '*', '\0' };
                foreach (var invalidChar in invalidChars)
                {
                    value = value.Replace(invalidChar.ToString(), "");
                }
                
                // Only remove colons that are NOT part of a Windows drive letter
                if (!hasWindowsDriveLetter)
                {
                    value = value.Replace(":", "");
                }
                else
                {
                    // Remove colons except the first one (drive letter)
                    var firstColon = value.IndexOf(':');
                    if (firstColon >= 0)
                    {
                        var beforeColon = value.Substring(0, firstColon + 1);
                        var afterColon = value.Substring(firstColon + 1).Replace(":", "");
                        value = beforeColon + afterColon;
                    }
                }
            }
            
            return value.Trim();
        }
        
        private static Dictionary<string, string> SanitizeVariables(Dictionary<string, string> variables)
        {
            var sanitized = new Dictionary<string, string>();
            foreach (var (key, value) in variables)
            {
                if (!string.IsNullOrWhiteSpace(key))
                {
                    sanitized[SanitizeInput(key)] = SanitizePathComponent(value ?? "");
                }
            }
            return sanitized;
        }
    }

    /// <summary>
    /// Simple path resolver that doesn't perform any template substitution.
    /// Just returns the template as-is. Useful for static paths.
    /// </summary>
    public class StaticParquetPathResolver : IParquetPathResolver
    {
        /// <summary>
        /// Returns the template as-is without any substitution.
        /// </summary>
        public string ResolvePath<T>(string template, string? environment = null)
            => ResolvePath(typeof(T), template, environment);

        /// <summary>
        /// Returns the template as-is without any substitution.
        /// </summary>
        public string ResolvePath(Type entityType, string template, string? environment = null)
        {
            var result = TryResolvePath(entityType, template, environment);
            return result.Success ? result.ResolvedPath : template;
        }
        
        /// <summary>
        /// Attempts to resolve a path template (always succeeds for static resolver).
        /// </summary>
        public PathResolutionResult TryResolvePath<T>(string template, string? environment = null)
            => TryResolvePath(typeof(T), template, environment);
        
        /// <summary>
        /// Attempts to resolve a path template (always succeeds for static resolver).
        /// </summary>
        public PathResolutionResult TryResolvePath(Type entityType, string template, string? environment = null)
        {
            if (string.IsNullOrWhiteSpace(template))
            {
                return PathResolutionResult.CreateFailure(new[] { "Template cannot be null or empty" });
            }
            
            return PathResolutionResult.CreateSuccess(template.Trim());
        }
    }
}
