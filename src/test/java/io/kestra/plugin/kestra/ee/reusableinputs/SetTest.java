package io.kestra.plugin.kestra.ee.reusableinputs;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.ReusableInputsWithSource;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class SetTest extends AbstractKestraEeContainerTest {
    @Inject
    TestRunContextFactory runContextFactory;

    private Set.SetBuilder<?, ?> set(String namespace) {
        return Set.builder()
            .id(Set.class.getSimpleName())
            .type(Set.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder().username(Property.ofValue(USERNAME)).password(Property.ofValue(PASSWORD)).build())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace))
            .reusableInputsId(Property.ofValue("bom"));
    }

    private Set.Output run(Set task) throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        return task.run(runContext);
    }

    @Test
    void shouldCreateASetThenChangeOnlyItsDefaults() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();

        Set.Output created = run(
            set(namespace)
                .reusableInputsDescription(Property.ofValue("Bill of materials"))
                .inputs(
                    Property.ofValue(
                        List.of(
                            Map.of("id", "environment", "type", "SELECT", "values", List.of("dev", "prod"), "defaults", "dev"),
                            Map.of("id", "cpus", "type", "INT", "min", 1, "defaults", 2)
                        )
                    )
                )
                .build()
        );
        Set.Output updated = run(set(namespace).defaults(Property.ofValue(Map.of("cpus", 4))).build());

        ReusableInputsWithSource saved = kestraTestDataUtils.getReusableInputs(namespace, "bom");
        assertThat(created.getRevision()).isEqualTo(1);
        assertThat(updated.getRevision()).isEqualTo(2);
        assertThat(saved.getDescription()).isEqualTo("Bill of materials");
        assertThat(saved.getInputs()).extracting(input -> input.getId()).containsExactly("environment", "cpus");
        assertThat(saved.getSource()).contains("defaults: dev").contains("defaults: 4");
    }

    @Test
    void shouldFailWithTheEditorMessageWhenTheSetIsInvalid() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(namespace, "bom", "inputs:\n  - id: cpus\n    type: INT\n");

        assertThatThrownBy(() -> run(set(namespace).defaults(Property.ofValue(Map.of("cpus", "abc"))).build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("could not be saved")
            .hasMessageContaining("cpus: Invalid default");
    }

    @Test
    void shouldSaveAnInvalidDraftThatExecutionsSkipThenEditTheRevisionTheyUse() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(namespace, "bom", "inputs:\n  - id: cpus\n    type: INT\n    min: 1\n    defaults: 2\n  - id: region\n    type: STRING\n");

        Set.Output draft = run(set(namespace).defaults(Property.ofValue(Map.of("cpus", 0))).draft(Property.ofValue(true)).build());
        Set.Output published = run(set(namespace).defaults(Property.ofValue(Map.of("region", "eu"))).build());

        ReusableInputsWithSource saved = kestraTestDataUtils.getReusableInputs(namespace, "bom");
        assertThat(draft.getDraft()).isTrue();
        assertThat(published.getDraft()).isFalse();
        assertThat(saved.getRevision()).isEqualTo(published.getRevision());
        assertThat(saved.getSource()).contains("defaults: 2").contains("defaults: eu").doesNotContain("defaults: 0");
    }

    @Test
    void shouldPublishASetThatOnlyHasDrafts() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createDraftReusableInputs(namespace, "bom", "inputs:\n  - id: region\n    type: STRING\n");

        Set.Output published = run(set(namespace).defaults(Property.ofValue(Map.of("region", "eu"))).build());

        assertThat(published.getRevision()).isEqualTo(2);
        assertThat(published.getDraft()).isFalse();
        assertThat(kestraTestDataUtils.getReusableInputs(namespace, "bom").getSource()).contains("defaults: eu");
    }

    @Test
    void shouldSaveTheExpressionsOfTheInputsAsWritten() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();

        run(
            set(namespace)
                .inputs(
                    Property.ofValue(
                        List.of(
                            Map.of("id", "region", "type", "SELECT", "expression", "{{ ['eu', 'us'] }}"),
                            Map.of("id", "started", "type", "DATETIME", "defaults", "{{ now() }}")
                        )
                    )
                )
                .build()
        );

        assertThat(kestraTestDataUtils.getReusableInputs(namespace, "bom").getSource())
            .contains("{{ ['eu', 'us'] }}")
            .contains("{{ now() }}");
    }

    @Test
    void shouldReplaceTheRowsOfATableWithTheRowsOfAFile() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(
            namespace, "bom", "inputs:\n  - id: servers\n    type: TABLE\n    columns:\n      - id: name\n        type: STRING\n      - id: cpus\n        type: INT\n"
        );
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, set(namespace).build(), Map.of());
        File rows = runContext.workingDir().createTempFile(".ion").toFile();
        Files.writeString(rows.toPath(), "{name:\"web\",cpus:\"2\"}\n{name:\"pg\",cpus:\"4\"}\n");
        URI uri = runContext.storage().putFile(rows);

        set(namespace).defaults(Property.ofValue(Map.of("servers", uri.toString()))).build().run(runContext);

        assertThat(kestraTestDataUtils.getReusableInputs(namespace, "bom").getSource())
            .contains("name: web")
            .contains("cpus: 4")
            .doesNotContain("\"4\"");
    }

    @Test
    void shouldCreateTheSetInTheNamespaceInsteadOfEditingTheOneInheritedFromItsParent() throws Exception {
        String parent = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        String child = parent + ".team";
        kestraTestDataUtils.createReusableInputs(parent, "bom", "inputs:\n  - id: region\n    type: STRING\n    defaults: us\n");

        assertThatThrownBy(() -> run(set(child).defaults(Property.ofValue(Map.of("region", "eu"))).build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("`inputs` must be set to create them");
        run(set(child).inputs(Property.ofValue(List.of(Map.of("id", "region", "type", "STRING", "defaults", "eu")))).build());

        assertThat(kestraTestDataUtils.getReusableInputs(child, "bom").getNamespace()).isEqualTo(child);
        assertThat(kestraTestDataUtils.getReusableInputs(parent, "bom").getSource()).contains("defaults: us");
    }
}
