package io.kestra.plugin.kestra;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.sdk.KestraClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Getter
public abstract class AbstractKestraTrigger extends AbstractTrigger {
    private static final String DEFAULT_KESTRA_URL = "http://localhost:8080";
    private static final String KESTRA_URL_TEMPLATE = "{{ kestra.url }}";

    @Schema(
        title = "Override Kestra API endpoint",
        description = """
        URL used for calls to the Kestra API. When null, renders `{{ kestra.url }}` from configuration; if still empty, defaults to http://localhost:8080. Trailing slashes are stripped before use.
        """
    )
    private Property<String> kestraUrl;

    @Schema(
        title = "Select API authentication",
        description = "Use either an API token or HTTP Basic (username/password); do not provide both."
    )
    private Auth auth;

    @Schema(title = "Override target tenant", description = "Tenant identifier applied to API calls; defaults to the current execution tenant.")
    @Setter
    protected Property<String> tenantId;

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        // use the kestraUrl property if set, otherwise the config value, or else the default
        String rKestraUrl = runContext.render(kestraUrl).as(String.class)
            .orElseGet(() -> {
                try {
                    return runContext.render(KESTRA_URL_TEMPLATE);
                } catch (IllegalVariableEvaluationException e) {
                    return DEFAULT_KESTRA_URL;
                }
            });

        runContext.logger().info("Kestra URL: {}", rKestraUrl);

        String normalizedUrl = rKestraUrl.trim().replaceAll("/+$", "");

        var builder = KestraClient.builder();
        builder.url(normalizedUrl);
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

            if (runContext.render(auth.auto).as(Boolean.class).orElse(Boolean.TRUE)) {
                Optional<SDK.Auth> autoAuth = runContext.sdk().defaultAuthentication();
                if (autoAuth.isPresent()) {
                    if (autoAuth.get().apiToken().isPresent()) {
                        return builder.tokenAuth(autoAuth.get().apiToken().get()).build();
                    }
                    if (autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                        return builder.basicAuth(autoAuth.get().username().get(), autoAuth.get().password().get()).build();
                    }
                }
            }

            throw new IllegalArgumentException("No authentication method provided");
        } else {
            // try automatic authentication
            Optional<SDK.Auth> autoAuth = runContext.sdk().defaultAuthentication();
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
        private Property<String> apiToken;

        @Schema(title = "Username for HTTP Basic auth")
        private Property<String> username;

        @Schema(title = "Password for HTTP Basic auth")
        private Property<String> password;

        @Schema(
            title = "Automatically retrieve credentials from Kestra's configuration if available",
            description = """
                The default configuration can be configured globally inside the Kestra configuration file:
                - Set `kestra.tasks.sdk.authentication.api-token` to use an API token
                - Set `kestra.tasks.sdk.authentication.username` and `kestra.tasks.sdk.authentication.password` for HTTP basic authentication
                The Enterprise edition also provides setting a default configuration at the Namespace of Tenant level by an administrator."""
        )
        @Builder.Default
        private Property<Boolean> auto = Property.ofValue(Boolean.TRUE);
    }
}
