package io.kestra.plugin;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.AbstractKestraTask;
import jakarta.inject.Inject;
import io.kestra.plugin.kestra.namespaces.List;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class AbstractKestraTaskTest extends AbstractKestraContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    public void KestraClientTest() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Both API Token and HTTP Basic authentication used
        List listApiAndUsername = List.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTask.Auth.builder()
                .apiToken(Property.ofValue("token"))
                .username(Property.ofValue("username"))
                .build()
            ).build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> listApiAndUsername.run(runContext));

        String expectedMessage = "Cannot use both API Token authentication and HTTP Basic authentication";
        assertThat(exception.getMessage(), is(expectedMessage));

        // Only username provided for HTTP Basic authentication
        List listOnlyUsername = listApiAndUsername.toBuilder()
            .auth(AbstractKestraTask.Auth.builder()
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
        RunContext runContext = runContextFactory.of();

        List task = List.builder()
            .tenantId(Property.ofValue(TENANT_ID))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build())
            .build();

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));

        String message = exception.getMessage().toLowerCase();
        assertThat(message.contains("failed: connection refused") && message.contains("localhost:8080"), is(true));
    }
}
