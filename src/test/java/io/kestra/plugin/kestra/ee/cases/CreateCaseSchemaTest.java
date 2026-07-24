package io.kestra.plugin.kestra.ee.cases;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.Task;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class CreateCaseSchemaTest {

    @Inject
    JsonSchemaGenerator jsonSchemaGenerator;

    @Test
    @SuppressWarnings("unchecked")
    void labelsAcceptsListOrMap() {
        Map<String, Object> generate = jsonSchemaGenerator.properties(Task.class, CreateCase.class);
        var properties = (Map<String, Map<String, Object>>) generate.get("properties");
        Map<String, Object> labels = properties.get("labels");

        // oneOf renders as "anyOf" in JSON schema; its absence is what made the editor reject the map form.
        assertThat(labels).containsKey("anyOf");
        String rendered = labels.toString();
        assertThat(rendered).contains("array").contains("object");
    }
}
