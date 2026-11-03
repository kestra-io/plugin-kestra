package io.kestra.plugin.kestra.ee.reusableinputs;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

@KestraTest
class ListTest extends AbstractKestraEeContainerTest {
    @Inject
    TestRunContextFactory runContextFactory;

    private List.ListBuilder<?, ?> list(String namespace) {
        return storedList(namespace).fetchType(Property.ofValue(FetchType.FETCH));
    }

    private List.ListBuilder<?, ?> storedList(String namespace) {
        return List.builder()
            .id(IdUtils.create())
            .type(List.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder().username(Property.ofValue(USERNAME)).password(Property.ofValue(PASSWORD)).build())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace));
    }

    private List.Output run(List list) throws Exception {
        return list.run(TestsUtils.mockRunContext(runContextFactory, list, Map.of()));
    }

    @Test
    void shouldListTheSetsOfTheNamespaceAndTheInheritedOnes() throws Exception {
        String parent = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        String child = parent + ".team";
        kestraTestDataUtils.createReusableInputs(parent, "shared", "inputs:\n  - id: region\n    type: STRING\n");
        kestraTestDataUtils.createReusableInputs(child, "bom", "inputs:\n  - id: cpus\n    type: INT\n");
        kestraTestDataUtils.createReusableInputs(child, "bom", "inputs:\n  - id: cpus\n    type: INT\n    defaults: 2\n");

        List.Output output = run(list(child).size(Property.ofValue(1)).build());

        assertThat(output.getSize()).isEqualTo(2L);
        assertThat(output.getReusableInputs())
            .extracting(List.Summary::id, List.Summary::namespace, List.Summary::revision)
            .containsExactlyInAnyOrder(tuple("bom", child, 2), tuple("shared", parent, 1));
    }

    @Test
    void shouldListEachSetAsItsLatestRevisionOrAsTheRevisionExecutionsUseWithoutAllowDraft() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(namespace, "bom", "inputs:\n  - id: cpus\n    type: INT\n");
        kestraTestDataUtils.createDraftReusableInputs(namespace, "bom", "inputs:\n  - id: cpus\n    type: INT\n    defaults: 2\n");
        kestraTestDataUtils.createDraftReusableInputs(namespace, "wip", "inputs:\n  - id: region\n    type: STRING\n");

        List.Output withDrafts = run(list(namespace).build());
        List.Output executable = run(list(namespace).allowDraft(Property.ofValue(false)).build());

        assertThat(executable.getReusableInputs())
            .extracting(List.Summary::id, List.Summary::revision, List.Summary::draft)
            .containsExactly(tuple("bom", 1, false));
        assertThat(withDrafts.getReusableInputs())
            .extracting(List.Summary::id, List.Summary::revision, List.Summary::draft)
            .containsExactlyInAnyOrder(tuple("bom", 2, true), tuple("wip", 1, true));
    }

    @Test
    void shouldStoreTheSetsAsAnIonFileByDefault() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(namespace, "bom", "inputs:\n  - id: cpus\n    type: INT\n");
        List list = storedList(namespace).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, list, Map.of());

        List.Output output = list.run(runContext);

        assertThat(output.getSize()).isEqualTo(1L);
        assertThat(output.getReusableInputs()).isNull();
        try (var stored = runContext.storage().getFile(output.getUri())) {
            assertThat(FileSerde.readAll(stored, new TypeReference<List.Summary>() {
            }).collectList().block())
                .extracting(List.Summary::id, List.Summary::namespace, List.Summary::revision)
                .containsExactly(tuple("bom", namespace, 1));
        }
    }

    @Test
    void shouldFetchOnlyTheFirstSetWithFetchOne() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(namespace, "bom", "inputs:\n  - id: cpus\n    type: INT\n");
        kestraTestDataUtils.createReusableInputs(namespace, "network", "inputs:\n  - id: vpc\n    type: STRING\n");

        List.Output output = run(list(namespace).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build());

        assertThat(output.getSize()).isEqualTo(1L);
        assertThat(output.getReusableInputs()).hasSize(1);
    }

    @Test
    void shouldRejectASizeBelowOneBeforeCallingTheApi() {
        assertThatThrownBy(() -> run(list("kestra.tests.reusableinputs").size(Property.ofValue(0)).build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("`page` and `size` must be 1 or higher, got page null and size 0.");
    }
}
