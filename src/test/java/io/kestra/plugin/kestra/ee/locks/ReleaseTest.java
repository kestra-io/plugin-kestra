package io.kestra.plugin.kestra.ee.locks;

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
import io.kestra.sdk.model.AssetsControllerApiAssetLock;
import io.kestra.sdk.model.AssetsControllerAssetLockRequest;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@KestraTest
class ReleaseTest extends AbstractKestraEeContainerTest {

    @Inject
    TestRunContextFactory runContextFactory;

    @Test
    void shouldReleaseOwnExecutionLock() throws Exception {
        String assetId = IdUtils.create().toLowerCase();
        kestraTestDataUtils.createAsset(assetId, "TABLE");

        Release release = Release.builder()
            .id(Release.class.getSimpleName())
            .type(Release.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .assetId(Property.ofExpression("{{ inputs.assetId }}"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(
            this.runContextFactory, release, Map.of("assetId", assetId)
        );
        String executionId = runContext.render("{{ execution.id }}");

        kestraTestDataUtils.getKestraClient().assets().lockAsset(assetId, TENANT_ID,
            new AssetsControllerAssetLockRequest().ttl("PT5M").executionId(executionId));

        assertThatCode(() -> release.run(runContext)).doesNotThrowAnyException();

        AssetsControllerApiAssetLock relock = kestraTestDataUtils.getKestraClient().assets()
            .lockAsset(assetId, TENANT_ID, new AssetsControllerAssetLockRequest().ttl("PT5M"));
        assertThat(relock).isNotNull();
    }
}
