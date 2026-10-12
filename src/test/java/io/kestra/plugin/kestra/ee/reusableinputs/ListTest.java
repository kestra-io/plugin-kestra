package io.kestra.plugin.kestra.ee.reusableinputs;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@KestraTest
class ListTest extends AbstractKestraEeContainerTest {
    @Inject
    TestRunContextFactory runContextFactory;

    @Test
    void shouldListTheSetsOfTheNamespaceAndTheInheritedOnes() throws Exception {
        String parent = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        String child = parent + ".team";
        kestraTestDataUtils.createReusableInputs(parent, "shared", "inputs:\n  - id: region\n    type: STRING\n");
        kestraTestDataUtils.createReusableInputs(child, "bom", "inputs:\n  - id: cpus\n    type: INT\n");
        kestraTestDataUtils.createReusableInputs(child, "bom", "inputs:\n  - id: cpus\n    type: INT\n    defaults: 2\n");

        List list = List.builder()
            .id(List.class.getSimpleName())
            .type(List.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder().username(Property.ofValue(USERNAME)).password(Property.ofValue(PASSWORD)).build())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(child))
            .size(Property.ofValue(1))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        List.Output output = list.run(TestsUtils.mockRunContext(runContextFactory, list, Map.of()));

        assertThat(output.getSize()).isEqualTo(2L);
        assertThat(output.getReusableInputs())
            .extracting(List.Summary::id, List.Summary::namespace, List.Summary::revision)
            .containsExactlyInAnyOrder(tuple("bom", child, 2), tuple("shared", parent, 1));
    }
}
