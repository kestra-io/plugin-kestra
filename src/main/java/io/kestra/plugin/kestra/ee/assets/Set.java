package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.Asset;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@Getter
@NoArgsConstructor
@Schema(
    title = "Create or update an asset",
    description = "Upserts an asset for the current tenant with the provided ID, type, display name, description, and metadata."
)
@Plugin(
    examples = {
        @Example(
            title = "Upsert an asset with some metadata.",
            full = true,
            code = """
                id: asset_set
                namespace: company.team

                tasks:
                  - id: upsert_asset
                    type: io.kestra.plugin.kestra.ee.assets.Set
                    namespace: "assets.data"
                    assetId: "customers_by_country"
                    assetType: "TABLE"
                    displayName: "Customers by Country"
                    assetDescription: "A table showing the number of customers by country."
                    metadata:
                        system: "{{ outputs.task.system }}"
                        database: "my-project"
                        schema: "analytics"
                        name: "customers_by_country"
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Namespace for the asset"
    )
    private Property<String> namespace;

    @NotNull
    @Schema(
        title = "Asset ID"
    )
    private Property<String> assetId;

    @NotNull
    @Schema(
        title = "Asset type"
    )
    private Property<String> assetType;

    @Schema(
        title = "Asset display name"
    )
    private Property<String> displayName;

    @Schema(
        title = "Asset description"
    )
    private Property<String> assetDescription;

    @Schema(
        title = "Asset metadata"
    )
    private Property<Map<String, Object>> metadata;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);

        Asset asset = new Asset()
            .namespace(runContext.render(namespace).as(String.class).orElse(null))
            .id(runContext.render(assetId).as(String.class).orElseThrow())
            .type(runContext.render(assetType).as(String.class).orElseThrow())
            .displayName(runContext.render(displayName).as(String.class).orElse(null))
            .description(runContext.render(assetDescription).as(String.class).orElse(null))
            .metadata(runContext.render(metadata).asMap(String.class, Object.class));

        kestraClient.assets().createAsset(
            runContext.flowInfo().tenantId(),
            JacksonMapper.ofYaml().writeValueAsString(asset)
        );

        return null;
    }
}
