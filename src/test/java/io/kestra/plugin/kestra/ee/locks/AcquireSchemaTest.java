package io.kestra.plugin.kestra.ee.locks;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.Task;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class AcquireSchemaTest {

    @Inject
    JsonSchemaGenerator jsonSchemaGenerator;

    @Test
    @SuppressWarnings("unchecked")
    void ttlDefaultsToFiveMinutes() {
        Map<String, Object> generate = jsonSchemaGenerator.properties(Task.class, Acquire.class);
        var properties = (Map<String, Map<String, Object>>) generate.get("properties");

        assertThat(properties.get("ttl").get("default")).isEqualTo("PT5M");
    }
}
