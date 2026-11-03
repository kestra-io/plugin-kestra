package io.kestra.plugin.kestra.ee.reusableinputs;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.internal.ApiException;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class DeleteTest extends AbstractKestraEeContainerTest {
    @Inject
    TestRunContextFactory runContextFactory;

    private void delete(String namespace, String id) throws Exception {
        Delete delete = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder().username(Property.ofValue(USERNAME)).password(Property.ofValue(PASSWORD)).build())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace))
            .reusableInputsId(Property.ofValue(id))
            .build();
        delete.run(TestsUtils.mockRunContext(runContextFactory, delete, Map.of()));
    }

    @Test
    void shouldDeleteTheReusableInputsOfTheNamespace() throws Exception {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(namespace, "bom", "inputs:\n  - id: region\n    type: STRING\n");

        delete(namespace, "bom");

        assertThatThrownBy(() -> kestraTestDataUtils.getReusableInputs(namespace, "bom"))
            .isInstanceOf(ApiException.class)
            .extracting(e -> ((ApiException) e).getCode())
            .isEqualTo(404);
    }

    @Test
    void shouldFailWithTheSetAndNamespaceWhenTheSetDoesNotExist() {
        String namespace = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();

        assertThatThrownBy(() -> delete(namespace, "missing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Reusable inputs 'missing' could not be deleted in namespace '%s': ".formatted(namespace));
    }

    @Test
    void shouldNotDeleteASetInheritedFromAParentNamespace() throws Exception {
        String parent = "kestra.tests.reusableinputs." + IdUtils.create().toLowerCase();
        kestraTestDataUtils.createReusableInputs(parent, "shared", "inputs:\n  - id: region\n    type: STRING\n");

        assertThatThrownBy(() -> delete(parent + ".team", "shared"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Reusable inputs 'shared' could not be deleted in namespace '%s.team': ".formatted(parent));
        assertThat(kestraTestDataUtils.getReusableInputs(parent, "shared").getNamespace()).isEqualTo(parent);
    }
}
