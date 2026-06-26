package io.kestra.plugin.kestra.ee.locks;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Release the lock held on an asset by the current execution.",
    description = """
        Releases the EXECUTION-owned lock acquired by `Acquire` for the current execution. The release is \
        owner-checked: it only removes a lock still held by this execution and is a no-op otherwise (e.g. if \
        the lock already expired or was taken over), so it is safe to run unconditionally. Requires the \
        `IMPERSONATE` permission on the `ASSET` resource."""
)
@Plugin(
    examples = {
        @Example(
            title = "Release a lock acquired earlier in the flow.",
            full = true,
            code = """
                id: release_asset_lock
                namespace: company.team

                tasks:
                  - id: release
                    type: io.kestra.plugin.kestra.ee.locks.Release
                    assetId: customers_by_country
                """
        )
    }
)
public class Release extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @NotNull
    @Schema(title = "The ID of the asset to unlock.")
    @PluginProperty(group = "main")
    private Property<String> assetId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);

        String tenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rAssetId = runContext.render(assetId).as(String.class).orElseThrow();

        // owner-checked: passing the current execution id releases only this execution's lock.
        kestraClient.assets().unlockAsset(rAssetId, tenant, runContext.render("{{ execution.id }}"));

        return null;
    }
}
