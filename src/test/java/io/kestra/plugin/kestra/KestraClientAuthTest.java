package io.kestra.plugin.kestra;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.namespaces.List;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class KestraClientAuthTest {
    @Inject
    private RunContextFactory runContextFactory;

    /** The SDK builder defaults to Basic auth, so building a client without credentials would send `Basic base64("null:null")`. */
    @Test
    void shouldFailWhenAutoIsDisabledWithoutCredentials() {
        List task = List.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTask.Auth.builder().auto(Property.ofValue(false)).build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContextFactory.of()));
    }

    @Test
    void shouldFailWhenAutoRetrievalFindsNoCredentials() {
        List task = List.builder()
            .kestraUrl(Property.ofValue("http://localhost:8080"))
            .auth(AbstractKestraTask.Auth.builder().build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.kestraClient(runContextFactory.of()));
    }
}
