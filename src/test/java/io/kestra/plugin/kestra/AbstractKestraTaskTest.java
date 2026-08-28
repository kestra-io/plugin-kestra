package io.kestra.plugin.kestra;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.RunContextInitializer;
import io.kestra.plugin.kestra.namespaces.List;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(rebuildContext = true)
public class AbstractKestraTaskTest extends AbstractKestraOssContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected RunContextInitializer runContextInitializer;

    @Test
    public void KestraClientTest() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Both API Token and HTTP Basic authentication used
        List listApiAndUsername = List.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .apiToken(Property.ofValue("token"))
                    .username(Property.ofValue("username"))
                    .build()
            ).build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> listApiAndUsername.run(runContext));

        String expectedMessage = "Cannot use both API Token authentication and HTTP Basic authentication";
        assertThat(exception.getMessage(), is(expectedMessage));

        // Only username provided for HTTP Basic authentication
        List listOnlyUsername = listApiAndUsername.toBuilder()
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue("username"))
                    .build()
            )
            .build();

        exception = assertThrows(IllegalArgumentException.class, () -> listOnlyUsername.run(runContext));

        expectedMessage = "Both username and password are required for HTTP Basic authentication";
        assertThat(exception.getMessage(), is(expectedMessage));

    }

    @Test
    public void shouldFallbackToDefaultUrlAndFailConnection() throws Exception {
        RunContext runContext = runContextInitializer.forExecutor((DefaultRunContext) runContextFactory.of());

        List task = List.builder()
            .tenantId(Property.ofValue(TENANT_ID))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .build();

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));

        // Message format is locale-dependent (e.g. "connexion refusée" in French), check only the stable parts
        String message = exception.getMessage().toLowerCase();
        assertThat(message.contains("localhost:8080") && message.contains("failed:"), is(true));
    }

    @Test
    @io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.url", value = "https://sdk-default.example.com")
    public void resolveKestraUrlShouldUseDefaultCredUrlWhenNoExplicitKestraUrl() throws Exception {
        RunContext runContext = runContextInitializer.forExecutor((DefaultRunContext) runContextFactory.of());
        List task = List.builder().build();

        assertThat(task.resolveKestraUrl(runContext), is("https://sdk-default.example.com"));
    }

    @Test
    @io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.url", value = "https://sdk-default.example.com")
    public void resolveKestraUrlShouldPreferExplicitKestraUrlOverDefaultCredUrl() throws Exception {
        RunContext runContext = runContextFactory.of();
        List task = List.builder()
            .kestraUrl(Property.ofValue("https://explicit.example.com"))
            .build();

        assertThat(task.resolveKestraUrl(runContext), is("https://explicit.example.com"));
    }

    @Test
    @io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.url", value = "   ")
    public void resolveKestraUrlShouldFallThroughWhenDefaultCredUrlIsBlank() throws Exception {
        RunContext runContext = runContextInitializer.forExecutor((DefaultRunContext) runContextFactory.of());
        List task = List.builder().build();

        assertThat(task.resolveKestraUrl(runContext), is("http://localhost:8080"));
    }

    @Test
    @io.micronaut.context.annotation.Property(name = "kestra.tasks.sdk.authentication.url", value = "https://sdk-default.example.com")
    public void resolveKestraUrlShouldIgnoreDefaultCredUrlWhenAutoIsFalse() throws Exception {
        RunContext runContext = runContextFactory.of();
        List task = List.builder()
            .auth(AbstractKestraTask.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        assertThat(task.resolveKestraUrl(runContext), is("http://localhost:8080"));
    }
}
