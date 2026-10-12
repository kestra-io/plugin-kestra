package io.kestra.plugin.kestra.ee.reusableinputs;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.sdk.internal.ApiException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReusableInputsSourcesTest {
    private static final String BOM = """
        id: bom
        namespace: company.team
        description: Bill of materials
        inputs:
          - id: environment
            type: SELECT
            values: [dev, prod]
            defaults: dev
          - id: sizing
            type: FORM
            inputs:
              - id: cpus
                type: INT
                min: 1
                defaults: 2
        """;

    private static Map<String, Object> parse(String source) throws Exception {
        return JacksonMapper.ofYaml().readValue(source, new TypeReference<>() {
        });
    }

    @Test
    void shouldOnlyChangeTheDefaultsOfExistingInputsWhenDefaultsAreSet() throws Exception {
        String source = ReusableInputsSources.apply(BOM, "company.team", "bom", null, null, Map.of("environment", "prod", "sizing.cpus", 4));

        assertThat(parse(source)).isEqualTo(parse(BOM.replace("defaults: dev", "defaults: prod").replace("defaults: 2", "defaults: 4")));
    }

    @Test
    void shouldReplaceEveryInputAndKeepTheDescriptionWhenInputsAreSet() throws Exception {
        String source = ReusableInputsSources.apply(BOM, "company.team", "bom", null, List.of(Map.of("id", "region", "type", "STRING")), null);

        assertThat(parse(source))
            .containsEntry("description", "Bill of materials")
            .containsEntry("inputs", List.of(Map.of("id", "region", "type", "STRING")));
    }

    @Test
    void shouldSetTheDefaultsOfInputsDeclaredInTheSameTaskWhenCreatingASet() throws Exception {
        String source = ReusableInputsSources.apply(
            null, "company.team", "bom", "New", List.of(Map.of("id", "region", "type", "STRING")), Map.of("region", "eu")
        );

        assertThat(parse(source)).isEqualTo(
            Map.of(
                "id", "bom",
                "namespace", "company.team",
                "description", "New",
                "inputs", List.of(Map.of("id", "region", "type", "STRING", "defaults", "eu"))
            )
        );
    }

    @Test
    void shouldFailWhenADefaultTargetsAnInputTheSetDoesNotHave() {
        assertThatThrownBy(() -> ReusableInputsSources.apply(BOM, "company.team", "bom", null, null, Map.of("sizing.memory", 8)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("`sizing.memory` is not an input of reusable inputs 'bom'.");
    }

    @Test
    void shouldFailWhenCreatingASetWithoutInputs() {
        assertThatThrownBy(() -> ReusableInputsSources.apply(null, "company.team", "bom", null, null, Map.of("environment", "prod")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("`inputs` must be set to create them");
    }

    @Test
    void shouldReportTheProblemDetailWhenTheApiRejectsTheSet() {
        ApiException rejected = new ApiException(
            422, "Unprocessable Entity", Map.of(), "{\"title\":\"Unprocessable Entity\",\"detail\":\"cpus: Invalid default: `abc` is not a valid INT value\"}"
        );

        assertThat(ReusableInputsSources.errorMessage(rejected)).isEqualTo("cpus: Invalid default: `abc` is not a valid INT value");
    }
}
