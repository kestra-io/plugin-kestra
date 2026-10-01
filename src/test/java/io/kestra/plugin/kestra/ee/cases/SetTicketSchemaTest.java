package io.kestra.plugin.kestra.ee.cases;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.Task;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class SetTicketSchemaTest {

    @Inject
    JsonSchemaGenerator jsonSchemaGenerator;

    @Test
    @SuppressWarnings("unchecked")
    void requiresTheTicketFieldsButNotTheCaseId() {
        Map<String, Object> generate = jsonSchemaGenerator.properties(Task.class, SetTicket.class);
        var properties = (Map<String, Map<String, Object>>) generate.get("properties");
        var required = (List<String>) generate.getOrDefault("required", List.of());

        assertThat(properties).containsKeys("caseId", "system", "key", "url");
        assertThat(required).contains("system", "key", "url");
        assertThat(required).doesNotContain("caseId");
    }
}
