package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete an asset by ID",
    description = "Removes an asset in the current tenant using its identifier; requires delete permissions on assets."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete an asset.",
            full = true,
            code = """
                id: asset_delete
                namespace: company.team

                tasks:
                  - id: delete_asset
                    type: io.kestra.plugin.kestra.ee.assets.Delete
                    assetId: "customers_by_country"
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @NotNull
    @Schema(
        title = "Asset ID to delete"
    )
    private Property<String> assetId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);

        kestraClient.assets().deleteAsset(
            runContext.render(assetId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );

        return null;
    }
}
