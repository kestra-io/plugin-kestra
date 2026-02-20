package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.ee.assets.Delete;
import io.kestra.sdk.internal.ApiException;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class DeleteTest extends AbstractKestraEeContainerTest {
    protected static final String NAMESPACE = "kestra.tests.assets.delete";

    @Inject
    TestRunContextFactory runContextFactory;

    @Test
    void defaultDelete() throws Exception {
        String assetId = IdUtils.create();

        kestraTestDataUtils.createAsset(assetId, "MY_ASSET_TYPE");

        Assertions.assertDoesNotThrow(() -> kestraTestDataUtils.getAsset(assetId));

        // Given
        Delete delete = Delete.builder()
            .id(io.kestra.plugin.core.kv.Set.class.getSimpleName())
            .type(io.kestra.plugin.core.kv.Set.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .assetId(Property.ofExpression("{{ inputs.assetId }}"))
            .build();

        final RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, delete, Map.of(
            "assetId", assetId
        ));

        // When
        delete.run(runContext);

        ApiException apiException = Assertions.assertThrows(ApiException.class, () -> kestraTestDataUtils.getAsset(assetId));
        assertThat(apiException.getCode()).isEqualTo(HttpStatus.NOT_FOUND.getCode());
    }
}
