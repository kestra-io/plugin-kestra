package io.kestra.plugin.kestra.ee.locks;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.AssetsControllerApiAssetLock;
import io.kestra.sdk.model.AssetsControllerAssetLockRequest;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Acquire a lock on an asset for the duration of the execution.",
    description = """
        Acquires an EXECUTION-owned lock on the asset on behalf of the current execution, so concurrent \
        flow writes to the asset are rejected while it is held. The lock is released by the `Release` task, \
        or automatically when its `ttl` expires. The calling credential must hold the `IMPERSONATE` permission \
        on the `ASSET` resource (typically a dedicated service account); other principals are rejected with 403."""
)
@Plugin(
    examples = {
        @Example(
            title = "Lock an asset while a flow writes to it, then release it.",
            full = true,
            code = """
                id: lock_asset
                namespace: company.team

                tasks:
                  - id: acquire
                    type: io.kestra.plugin.kestra.ee.locks.Acquire
                    assetId: customers_by_country
                    ttl: PT1H
                  - id: write
                    type: io.kestra.plugin.core.log.Log
                    message: writing to the locked asset
                  - id: release
                    type: io.kestra.plugin.kestra.ee.locks.Release
                    assetId: customers_by_country
                """
        )
    }
)
public class Acquire extends AbstractKestraTask implements RunnableTask<Acquire.AcquireOutput> {
    @NotNull
    @Schema(title = "The ID of the asset to lock.")
    @PluginProperty(group = "main")
    private Property<String> assetId;

    @Schema(
        title = "How long the lock is held before it expires.",
        description = "An ISO-8601 duration (e.g. `PT1H`). When unset, the server default TTL applies."
    )
    @PluginProperty(group = "main")
    private Property<Duration> ttl;

    @Override
    public AcquireOutput run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);

        String tenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rAssetId = runContext.render(assetId).as(String.class).orElseThrow();

        AssetsControllerAssetLockRequest request = new AssetsControllerAssetLockRequest()
            .executionId(runContext.render("{{ execution.id }}"))
            .flowId(runContext.flowInfo().id())
            .flowNamespace(runContext.flowInfo().namespace())
            .taskRunId(runContext.render("{{ taskrun.id }}"));
        runContext.render(ttl).as(Duration.class).ifPresent(t -> request.ttl(t.toString()));

        AssetsControllerApiAssetLock lock = kestraClient.assets().lockAsset(rAssetId, tenant, request);

        OffsetDateTime lockedUntil = lock.getLockedUntil();
        return AcquireOutput.builder()
            .lockedUntil(lockedUntil != null ? lockedUntil.toInstant() : null)
            .ownerType(lock.getOwnerType())
            .executionId(lock.getExecutionId())
            .build();
    }

    @Builder
    @Getter
    public static class AcquireOutput implements Output {
        @Schema(title = "The instant at which the lock expires.")
        private final Instant lockedUntil;

        @Schema(title = "The lock owner type (always EXECUTION for a task-acquired lock).")
        private final String ownerType;

        @Schema(title = "The id of the execution holding the lock.")
        private final String executionId;
    }
}
