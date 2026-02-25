package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.NotImplementedException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Purges assets from the catalog based on retention policies.",
    description = "By default, the task purges assets, asset usage events (execution view), and asset lineage events (for asset exporters) matching the filters. You can configure it to only purge specific types of records. Asset lineage event purging currently does not support filtering by asset ID, type or metadata. Asset usage event purging currently does not support filtering by asset type or metadata."
)
@Plugin(
    examples = {
        @Example(
            title = "Purge old staging tables",
            full = true,
            code = """
                id: purge_staging_assets
                namespace: company.data

                tasks:
                  - id: cleanup_staging
                    type: io.kestra.plugin.ee.assets.PurgeAssets
                    namespace: company.data
                    assetType:
                      - io.kestra.plugin.ee.assets.Table
                    metadata:
                      model_layer: staging
                    endDate: "{{ now() | dateAdd(-90, 'DAYS') }}"
                """
        ),
        @Example(
            title = "Scheduled retention policy",
            full = true,
            code = """
                id: asset_retention_policy
                namespace: company.infra

                triggers:
                - id: monthly_cleanup
                 type: io.kestra.plugin.core.trigger.Schedule
                 cron: "0 0 1 * *"

                tasks:
                - id: purge_old_vms
                 type: io.kestra.plugin.ee.assets.PurgeAssets
                 assetType:
                   - io.kestra.plugin.ee.assets.VM
                 endDate: "{{ now() | dateAdd(-180, 'DAYS') }}"
                """
        )
    }
)
public class PurgeAssets extends AbstractKestraTask implements RunnableTask<PurgeAssets.PurgeOutput> {
    @Nullable
    @Schema(
        title = "Filter assets by namespace",
        description = "Exact match or prefix with .*"
    )
    private Property<String> namespace;

    @Nullable
    @Schema(
        title = "Filter assets by id"
    )
    private Property<String> assetId;

    @Nullable
    @Schema(
        title = "Filter by asset types"
    )
    private Property<List<String>> assetType;

    @Nullable
    @Schema(
        title = "Metadata filters"
    )
    private Property<java.util.List<FieldQuery>> metadataQuery;

    @NotNull
    @Schema(
        title = "Purge assets created or updated before this date (ISO 8601)"
    )
    private Property<Instant> endDate;

    @NotNull
    @Schema(
        title = "Whether to purge assets matching the filters"
    )
    @Builder.Default
    private Property<Boolean> purgeAssets = Property.ofValue(true);

    @NotNull
    @Schema(
        title = "Whether to purge asset usage events (execution view) matching the filters"
    )
    @Builder.Default
    private Property<Boolean> purgeAssetUsages = Property.ofValue(true);

    @NotNull
    @Schema(
        title = "Whether to purge asset lineage events (for asset exporters) matching the filters"
    )
    @Builder.Default
    private Property<Boolean> purgeAssetLineages = Property.ofValue(true);

    @Override
    public PurgeOutput run(RunContext runContext) throws Exception {
        Boolean rPurgeAssetLineages = runContext.render(purgeAssetLineages).as(Boolean.class).orElse(true);
        Boolean rPurgeAssetUsages = runContext.render(purgeAssetUsages).as(Boolean.class).orElse(true);

        String rAssetId = runContext.render(assetId).as(String.class).orElse(null);
        List<String> rAssetType = runContext.render(assetType).asList(String.class);
        List<FieldQuery> rMetadataQuery = runContext.render(metadataQuery).asList(FieldQuery.class);

        // TODO Remove once asset lineage events support filtering by asset ID, type and metadata (https://github.com/kestra-io/kestra-ee/issues/6851)
        if (rPurgeAssetLineages) {
            if (rAssetId != null) {
                throw new NotImplementedException("Asset ID filtering is not yet implemented for lineage events");
            }

            if (!rAssetType.isEmpty()) {
                throw new NotImplementedException("Asset type filtering is not yet implemented for lineage events");
            }

            if (!rMetadataQuery.isEmpty()) {
                throw new NotImplementedException("Asset metadata filtering is not yet implemented for lineage events");
            }
        }

        // TODO Remove once asset usage events support filtering by type and metadata (https://github.com/kestra-io/kestra-ee/issues/6851)
        if (rPurgeAssetUsages) {
            if (!rAssetType.isEmpty()) {
                throw new NotImplementedException("Asset type filtering is not yet implemented for usage events");
            }

            if (!rMetadataQuery.isEmpty()) {
                throw new NotImplementedException("Asset metadata filtering is not yet implemented for usage events");
            }
        }

        var kestraClient = kestraClient(runContext);
        PurgeOutput.PurgeOutputBuilder outputBuilder = PurgeOutput.builder();

        String rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        Instant rEndDate = runContext.render(endDate).as(Instant.class).orElse(null);
        if (runContext.render(purgeAssets).as(Boolean.class).orElse(true)) {
            outputBuilder.purgedAssetsCount(kestraClient.assets().deleteAssetsByQuery(
                this.toQueryFilters(
                    rAssetId,
                    rNamespace,
                    rAssetType,
                    rMetadataQuery,
                    rEndDate,
                    QueryFilterField.ID,
                    QueryFilterField.UPDATED
                ),
                true,
                runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
            ).getCount());
        }

        if (rPurgeAssetUsages) {
            outputBuilder.purgedAssetUsagesCount(kestraClient.assets().deleteAssetUsagesByQuery(
                this.toQueryFilters(
                    rAssetId,
                    rNamespace,
                    rAssetType,
                    rMetadataQuery,
                    rEndDate,
                    QueryFilterField.ASSET_ID,
                    QueryFilterField.CREATED
                ),
                runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
            ).getCount());
        }

        if (rPurgeAssetLineages) {
            outputBuilder.purgedAssetLineagesCount(kestraClient.assets().deleteAssetLineageEventsByQuery(
                this.toQueryFilters(
                    rAssetId,
                    rNamespace,
                    rAssetType,
                    rMetadataQuery,
                    rEndDate,
                    QueryFilterField.ASSET_ID,
                    QueryFilterField.CREATED
                ),
                runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
            ).getCount());
        }

        return outputBuilder.build();
    }

    private java.util.List<QueryFilter> toQueryFilters(
        String assetId,
        String namespace,
        java.util.List<String> typesFilter,
        java.util.List<FieldQuery> metadataQuery,
        Instant endDate,
        QueryFilterField idField,
        QueryFilterField dateField
    ) {
        java.util.List<QueryFilter> queryFilters = new ArrayList<>();

        if (assetId != null) {
            queryFilters.add(new QueryFilter()
                .field(idField)
                .operation(QueryFilterOp.EQUALS)
                .value(assetId));
        }

        if (namespace != null) {
            queryFilters.add(new QueryFilter()
                .field(QueryFilterField.NAMESPACE)
                .operation(QueryFilterOp.PREFIX)
                .value(namespace));
        }

        if (!typesFilter.isEmpty()) {
            queryFilters.add(new QueryFilter()
                .field(QueryFilterField.TYPE)
                .operation(QueryFilterOp.IN)
                .value(typesFilter));
        }

        if (!metadataQuery.isEmpty()) {
            Map<QueryType, java.util.List<FieldQuery>> byOpType = metadataQuery.stream().collect(Collectors.groupingBy(FieldQuery::type));
            byOpType.forEach(throwBiConsumer((key, value) -> {
                Map<String, String> metadataMap = value.stream().collect(Collectors.toMap(
                    FieldQuery::field,
                    FieldQuery::value
                ));

                queryFilters.add(new QueryFilter()
                    .field(QueryFilterField.METADATA)
                    .operation(key.toQueryFilterOp())
                    .value(metadataMap));
            }));
        }

        if (endDate != null) {
            queryFilters.add(new QueryFilter()
                .field(dateField)
                .operation(QueryFilterOp.LESS_THAN_OR_EQUAL_TO)
                .value(endDate)
            );
        }

        return queryFilters;
    }

    @Getter
    @Builder
    public static class PurgeOutput implements Output {
        @Schema(
            title = "Number of assets purged"
        )
        private Integer purgedAssetsCount;

        @Schema(
            title = "Number of asset usages purged"
        )
        private Integer purgedAssetUsagesCount;

        @Schema(
            title = "Number of asset lineages purged"
        )
        private Integer purgedAssetLineagesCount;
    }
}
