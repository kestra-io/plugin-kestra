package io.kestra.plugin.kestra;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.namespaces.List;
import io.kestra.plugin.kestra.triggers.ScheduleMonitor;
import io.kestra.sdk.KestraClient;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class KestraClientAuthTest {
    @Inject
    private RunContextFactory runContextFactory;

    /** Reads the default headers the client was built with, which is where the SDK puts the `Authorization` one. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> defaultHeaders(KestraClient client) throws Exception {
        Field apiClientField = KestraClient.class.getDeclaredField("apiClient");
        apiClientField.setAccessible(true);
        Object apiClient = apiClientField.get(client);

        return (Map<String, String>) apiClient.getClass().getMethod("getDefaultHeaders").invoke(apiClient);
    }

    private static List.ListBuilder<?, ?> task() {
        return List.builder().kestraUrl(Property.ofValue("http://localhost:8080"));
    }

    /** `auth.auto: false` with no credentials is the explicit way to call a Kestra API that requires no authentication. */
    @Test
    void shouldSendNoAuthorizationHeaderWhenAutoIsDisabledWithoutCredentials() throws Exception {
        List task = task()
            .auth(AbstractKestraTask.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        KestraClient client = task.kestraClient(runContextFactory.of());

        assertThat(defaultHeaders(client).containsKey("Authorization"), is(false));
    }

    /** Only an explicit opt-out is unauthenticated: a missing credential with `auth.auto` left on stays a configuration error. */
    @Test
    void shouldFailWhenAutoRetrievalFindsNoCredentials() {
        List task = task()
            .auth(AbstractKestraTask.Auth.builder().build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContextFactory.of()));
    }

    @Test
    void shouldSendTheApiTokenAsABearerHeader() throws Exception {
        List task = task()
            .auth(AbstractKestraTask.Auth.builder().apiToken(Property.ofValue("a-token")).build())
            .build();

        KestraClient client = task.kestraClient(runContextFactory.of());

        assertThat(defaultHeaders(client).get("Authorization"), is("Bearer a-token"));
    }

    /** Triggers poll the API with a raw header rather than through the SDK client, so the opt-out has to reach that path too. */
    @Test
    void shouldResolveNoAuthorizationHeaderForATriggerWhenAutoIsDisabledWithoutCredentials() throws Exception {
        ScheduleMonitor trigger = ScheduleMonitor.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTrigger.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        assertThat(trigger.resolveConnection(runContextFactory.of()).authorizationHeader(), is(nullValue()));
    }

    @Test
    void shouldSendNoAuthorizationHeaderForATriggerClientWhenAutoIsDisabledWithoutCredentials() throws Exception {
        ScheduleMonitor trigger = ScheduleMonitor.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTrigger.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        KestraClient client = trigger.kestraClient(runContextFactory.of());

        assertThat(defaultHeaders(client).containsKey("Authorization"), is(false));
    }

    @Test
    void shouldFailForATriggerClientWhenAutoRetrievalFindsNoCredentials() {
        ScheduleMonitor trigger = ScheduleMonitor.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTrigger.Auth.builder().build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> trigger.kestraClient(runContextFactory.of()));
    }

    @Test
    void shouldResolveTheApiTokenAsABearerHeaderForATrigger() throws Exception {
        ScheduleMonitor trigger = ScheduleMonitor.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("a-token")).build())
            .build();

        assertThat(trigger.resolveConnection(runContextFactory.of()).authorizationHeader(), is("Bearer a-token"));
    }
}
