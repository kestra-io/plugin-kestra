package io.kestra.plugin.kestra.ee.locks;

import java.time.Duration;
import java.time.Instant;
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
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.AssetsControllerAssetLockRequest;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class AcquireTest extends AbstractKestraEeContainerTest {

    @Inject
    TestRunContextFactory runContextFactory;

    private Acquire.AcquireOutput acquire(String assetId) throws Exception {
        Acquire acquire = Acquire.builder()
            .id(Acquire.class.getSimpleName())
            .type(Acquire.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .assetId(Property.ofExpression("{{ inputs.assetId }}"))
            .ttl(Property.ofValue(Duration.ofMinutes(5)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(
            this.runContextFactory, acquire, Map.of("assetId", assetId)
        );
        return acquire.run(runContext);
    }

    @Test
    void shouldAcquireExecutionOwnedLock() throws Exception {
        String assetId = IdUtils.create().toLowerCase();
        kestraTestDataUtils.createAsset(assetId, "TABLE");

        Acquire.AcquireOutput output = acquire(assetId);

        assertThat(output.getOwnerType()).isEqualTo("EXECUTION");
        assertThat(output.getExecutionId()).isNotBlank();
        assertThat(output.getLockedUntil()).isAfter(Instant.now());
    }

    @Test
    void shouldRejectLockingAnAlreadyLockedAsset() throws Exception {
        String assetId = IdUtils.create().toLowerCase();
        kestraTestDataUtils.createAsset(assetId, "TABLE");
        acquire(assetId);

        assertThatThrownBy(() -> kestraTestDataUtils.getKestraClient().assets()
            .lockAsset(assetId, TENANT_ID, new AssetsControllerAssetLockRequest().ttl("PT5M")))
            .isInstanceOf(ApiException.class);
    }
}
