package io.kestra.plugin.kestra;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.sdk.KestraClient;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Getter
public abstract class AbstractKestraTask extends Task {
    private static final String DEFAULT_KESTRA_URL = "http://localhost:8080";
    private static final String KESTRA_URL_TEMPLATE = "{{ kestra.url }}";

    @Schema(
        title = "Override Kestra API endpoint",
        description = """
            URL used for calls to the Kestra API. When null, falls back to the `url` configured alongside the default SDK authentication (Namespace or Tenant level, Enterprise edition), then renders `{{ kestra.url }}` from configuration; if still empty, defaults to `http://localhost:8080`. Trailing slashes are stripped before use.
            """
    )
    @PluginProperty(group = "connection")
    private Property<String> kestraUrl;

    @Schema(
        title = "Select API authentication",
        description = "Use either an API token or HTTP Basic (username/password); do not provide both."
    )
    @PluginProperty(group = "connection")
    private Auth auth;

    @Schema(title = "Override target tenant", description = "Tenant identifier applied to API calls; defaults to the current execution tenant.")
    @Setter
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(
        title = "Connection timeout",
        description = """
            Maximum time to wait while opening a connection to the Kestra API, as an ISO-8601 duration (e.g. `PT10S`). Must be positive. When unset, the SDK default applies."""
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> connectTimeout;

    @Schema(
        title = "Read timeout",
        description = """
            Maximum time to wait for the Kestra API to answer a request, as an ISO-8601 duration (e.g. `PT30M`). Must be positive. When unset, the task `timeout` is used if set, otherwise the SDK default applies (about 3 minutes), which can interrupt long-running calls."""
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> readTimeout;

    protected Duration defaultReadTimeout() {
        return null;
    }

    protected Duration resolveReadTimeout(RunContext runContext) throws IllegalVariableEvaluationException {
        var rReadTimeout = runContext.render(readTimeout).as(Duration.class)
            .or(() -> renderTaskTimeout(runContext))
            .orElseGet(this::defaultReadTimeout);
        return requirePositive(rReadTimeout, "readTimeout");
    }

    private Optional<Duration> renderTaskTimeout(RunContext runContext) {
        try {
            return runContext.render(getTimeout()).as(Duration.class);
        } catch (IllegalVariableEvaluationException e) {
            throw new IllegalArgumentException("Unable to render task timeout: " + e.getMessage(), e);
        }
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value != null && (value.isNegative() || value.isZero())) {
            throw new IllegalArgumentException(name + " must be a positive duration");
        }
        return value;
    }

    protected void hintOnReadTimeout(RunContext runContext, Throwable error) {
        if (readTimeout != null) {
            return;
        }
        for (var cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketTimeoutException) {
                runContext.logger()
                    .warn("The Kestra API call timed out while waiting for a response. Set the `readTimeout` property to a larger duration (e.g. PT30M) for long-running calls.");
                return;
            }
        }
    }

    protected String resolveKestraUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        return resolveKestraUrl(runContext, defaultAuth(runContext));
    }

    private String resolveKestraUrl(RunContext runContext, DefaultAuthSupplier defaultAuth) throws IllegalVariableEvaluationException {
        String raw = runContext.render(kestraUrl).as(String.class)
            .or(() -> defaultAuth.get().flatMap(SDK.Auth::url).filter(url -> !url.isBlank()))
            .orElseGet(() ->
            {
                try {
                    return runContext.render(KESTRA_URL_TEMPLATE);
                } catch (IllegalVariableEvaluationException e) {
                    return DEFAULT_KESTRA_URL;
                }
            });
        return raw.trim().replaceAll("/+$", "");
    }

    private DefaultAuthSupplier defaultAuth(RunContext runContext) throws IllegalVariableEvaluationException {
        boolean useAutoAuth = auth == null || runContext.render(auth.auto).as(Boolean.class).orElse(Boolean.TRUE);
        return new DefaultAuthSupplier(useAutoAuth ? runContext.sdk() : null);
    }

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        DefaultAuthSupplier defaultAuth = defaultAuth(runContext);
        String normalizedUrl = resolveKestraUrl(runContext, defaultAuth);

        runContext.logger().info("Kestra URL: {}", normalizedUrl);

        var builder = KestraClient.builder();
        builder.url(normalizedUrl);
        var rConnectTimeout = requirePositive(runContext.render(connectTimeout).as(Duration.class).orElse(null), "connectTimeout");
        if (rConnectTimeout != null) {
            builder.connectTimeout(rConnectTimeout);
        }
        var rReadTimeout = resolveReadTimeout(runContext);
        if (rReadTimeout != null) {
            builder.readTimeout(rReadTimeout);
        }
        if (auth != null) {
            if (auth.apiToken != null && (auth.username != null || auth.password != null)) {
                throw new IllegalArgumentException("Cannot use both API Token authentication and HTTP Basic authentication");
            }

            String rApiToken = runContext.render(auth.apiToken).as(String.class).orElse(null);
            if (rApiToken != null) {
                builder.tokenAuth(rApiToken);
                return builder.build();
            }

            Optional<String> maybeUsername = runContext.render(auth.username).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.password).as(String.class);
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                builder.basicAuth(maybeUsername.get(), maybePassword.get());
                return builder.build();
            }
            if (maybeUsername.isPresent() || maybePassword.isPresent()) {
                throw new IllegalArgumentException("Both username and password are required for HTTP Basic authentication");
            }

            Optional<SDK.Auth> autoAuth = defaultAuth.get();
            if (autoAuth.isPresent()) {
                if (autoAuth.get().apiToken().isPresent()) {
                    return builder.tokenAuth(autoAuth.get().apiToken().get()).build();
                }
                if (autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                    return builder.basicAuth(autoAuth.get().username().get(), autoAuth.get().password().get()).build();
                }
            }

            if (runContext.render(auth.auto).as(Boolean.class).orElse(Boolean.TRUE)) {
                throw new IllegalArgumentException("No authentication method provided. Set 'auth.auto' to false to call a Kestra API that requires no authentication.");
            }

            return builder.noAuth().build();
        } else {
            // try automatic authentication
            Optional<SDK.Auth> autoAuth = defaultAuth.get();
            if (autoAuth.isPresent()) {
                if (autoAuth.get().apiToken().isPresent()) {
                    return builder.tokenAuth(autoAuth.get().apiToken().get()).build();
                }
                if (autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                    return builder.basicAuth(autoAuth.get().username().get(), autoAuth.get().password().get()).build();
                }
            }
        }
        return builder.build();
    }

    @Builder
    @Getter
    public static class Auth {
        @Schema(title = "API token for bearer auth")
        @PluginProperty(group = "connection", secret = true)
        private Property<String> apiToken;

        @Schema(title = "Username for HTTP Basic auth")
        @PluginProperty(secret = true, group = "connection")
        private Property<String> username;

        @Schema(title = "Password for HTTP Basic auth")
        @PluginProperty(group = "connection", secret = true)
        private Property<String> password;

        @Schema(
            title = "Automatically retrieve credentials from Kestra's configuration if available",
            description = """
                The default configuration can be configured globally inside the Kestra configuration file:
                - Set `kestra.tasks.sdk.authentication.api-token` to use an API token
                - Set `kestra.tasks.sdk.authentication.username` and `kestra.tasks.sdk.authentication.password` for HTTP basic authentication
                - Set `kestra.tasks.sdk.authentication.url` to also default the Kestra API endpoint (see `kestraUrl` above)
                The Enterprise edition also provides setting a default configuration at the Namespace or Tenant level by an administrator. Set to false to also opt out of the default URL, and to call a Kestra API that requires no authentication when no credentials are set either."""
        )
        @Builder.Default
        @PluginProperty(group = "advanced")
        private Property<Boolean> auto = Property.ofValue(Boolean.TRUE);
    }
}
