package io.kestra.plugin.kestra;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
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
public abstract class AbstractKestraTask extends Task {
    @Schema(title = "Kestra API URL, if null, `http://localhost:8080` will be used.")
    private Property<String> kestraUrl;

    @Schema(title = "Authentication information.")
    private Auth auth;

    @Schema(title = "The tenant ID to use for the request, defaults to current tenant.")
    @Setter
    protected Property<String> tenantId;

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        String rKestraUrl = runContext.render(kestraUrl).as(String.class).orElse("http://localhost:8080");
        var builder = KestraClient.builder();
        builder.url(rKestraUrl);
        if (auth != null) {
            if (auth.apiToken != null && (auth.username != null || auth.password != null)) {
                throw new IllegalArgumentException("Cannot use both API Token authentication and HTTP Basic authentication");
            }

            runContext.render(auth.apiToken).as(String.class).ifPresent(token -> builder.tokenAuth(token));

            Optional<String> maybeUsername = runContext.render(auth.username).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.password).as(String.class);
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                builder.basicAuth(maybeUsername.get(), maybePassword.get());
                return builder.build();
            }

            if (maybeUsername.isPresent() || maybePassword.isPresent()) {
                throw new IllegalArgumentException("Both username and password are required for HTTP Basic authentication");
            }
        }
        return builder.build();
    }

    @Builder
    @Getter
    public static class Auth {
        @Schema(title = "API token.")
        private Property<String> apiToken;

        @Schema(title = "Username for HTTP Basic authentication.")
        private Property<String> username;

        @Schema(title = "Password for HTTP Basic authentication.")
        private Property<String> password;
    }
}
