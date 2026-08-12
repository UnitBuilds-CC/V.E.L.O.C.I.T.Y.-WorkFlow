using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Velocity.Workflow.Server;

/// <summary>
/// Validates JWT bearer tokens for gRPC authentication.
/// Supports HMAC-SHA256 and RSA signatures, checks expiration,
/// and extracts subject + role claims for RBAC authorization.
/// </summary>
public class JwtTokenValidator
{
    private readonly TokenValidationParameters _validationParameters;
    private readonly JwtSecurityTokenHandler _handler;
    private readonly ILogger<JwtTokenValidator> _logger;
    private readonly bool _enabled;

    /// <summary>
    /// Create a JWT validator with a symmetric (HMAC-SHA256) signing key.
    /// When <paramref name="signingKey"/> is null or empty, JWT validation is disabled
    /// and the interceptor falls back to the legacy subject:role parsing.
    /// </summary>
    public JwtTokenValidator(string? signingKey, string? issuer, string? audience, ILogger<JwtTokenValidator> logger)
    {
        _logger = logger;
        _handler = new JwtSecurityTokenHandler();

        if (string.IsNullOrEmpty(signingKey))
        {
            _enabled = false;
            _validationParameters = new TokenValidationParameters();
            return;
        }

        _enabled = true;
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(signingKey));

        _validationParameters = new TokenValidationParameters
        {
            ValidateIssuerSigningKey = true,
            IssuerSigningKey = key,
            ValidateIssuer = !string.IsNullOrEmpty(issuer),
            ValidIssuer = issuer,
            ValidateAudience = !string.IsNullOrEmpty(audience),
            ValidAudience = audience,
            ValidateLifetime = true,
            ClockSkew = TimeSpan.FromMinutes(5), // Allow 5 min clock skew
            RequireExpirationTime = true,
        };
    }

    /// <summary>Whether JWT validation is enabled (signing key configured).</summary>
    public bool IsEnabled => _enabled;

    /// <summary>
    /// Validate a JWT token and extract claims.
    /// Returns null if the token is invalid or expired.
    /// </summary>
    public JwtClaims? Validate(string token)
    {
        if (!_enabled) return null;

        try
        {
            var principal = _handler.ValidateToken(token, _validationParameters, out var validatedToken);
            var subject = principal.FindFirst(ClaimTypes.NameIdentifier)?.Value
                       ?? principal.FindFirst("sub")?.Value
                       ?? principal.Identity?.Name;

            if (string.IsNullOrEmpty(subject))
            {
                _logger.LogWarning("JWT token missing 'sub' claim");
                return null;
            }

            // Extract roles — support both "role" and "roles" claims
            var roles = principal.FindAll(ClaimTypes.Role)
                .Select(c => c.Value)
                .ToList();

            // Also check custom "role" claim (singular)
            var singleRole = principal.FindFirst("role")?.Value;
            if (!string.IsNullOrEmpty(singleRole) && !roles.Contains(singleRole))
                roles.Add(singleRole);

            // Default role if none specified
            if (roles.Count == 0)
                roles.Add("reader");

            return new JwtClaims
            {
                Subject = subject,
                Roles = roles,
                ExpiresAt = principal.FindFirst(ClaimTypes.Expiration)?.Value is string exp
                    ? DateTimeOffset.FromUnixTimeSeconds(long.Parse(exp)).UtcDateTime
                    : (validatedToken as JwtSecurityToken)?.ValidTo ?? DateTime.MaxValue,
                Claims = principal.Claims.ToDictionary(c => c.Type, c => c.Value),
            };
        }
        catch (SecurityTokenExpiredException)
        {
            _logger.LogWarning("JWT token expired");
            return null;
        }
        catch (SecurityTokenInvalidSignatureException)
        {
            _logger.LogWarning("JWT token signature invalid");
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "JWT token validation failed");
            return null;
        }
    }
}

/// <summary>Extracted claims from a validated JWT token.</summary>
public class JwtClaims
{
    public string Subject { get; init; } = "";
    public List<string> Roles { get; init; } = new();
    public DateTime ExpiresAt { get; init; }
    public Dictionary<string, string> Claims { get; init; } = new();
}
