package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;
import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "List assets with filters",
    description = "Searches assets for the current tenant with optional namespace, type, and metadata filters. Supports pagination (size defaults to 100) or fetches all pages when no page is set."
)
@Plugin(
    examples = {
        @Example(
            title = "List assets matching the filters.",
            full = true,
            code = """
                id: asset_list
                namespace: company.team

                tasks:
                  - id: list_assets
                    type: io.kestra.plugin.kestra.ee.assets.List
                    namespace: "assets.data"
                    types:
                        - TABLE
                    metadataQuery:
                        - field: system
                          type: EQUAL_TO
                          value: "{{ outputs.task.system }}"
                        - field: database
                          type: EQUAL_TO
                          value: "my-project"
                        - field: schema
                          type: NOT_EQUAL_TO
                          value: "public"
                        - field: name
                          type: EQUAL_TO
                          value: "customers_by_country"
                    fetchType: FETCH # or STORE to get an ION file in output with the list of assets
                """
        )
    }
)
public class List extends AbstractKestraTask implements RunnableTask<List.Output> {
    @Nullable
    @Schema(title = "Page number",
        description = "If omitted, all pages are iterated. Set to 1+ to fetch a single page with `size` items.")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(title = "Page size",
        description = "Number of assets per page; defaults to 100.")
    private Property<Integer> size = Property.ofValue(100);

    @Nullable
    @Schema(
        title = "Namespace filter"
    )
    private Property<String> namespace;

    @Nullable
    @Schema(
        title = "Asset types to match"
    )
    private Property<java.util.List<String>> types;

    @Nullable
    @Schema(
        title = "Metadata filters"
    )
    private Property<java.util.List<FieldQuery>> metadataQuery;

    @Schema(
        title = "Output fetch type",
        description = "Defines how results are returned (e.g., `FETCH` for direct output, `STORE` to persist as a file)."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        FetchType renderedFetchType = runContext.render(this.fetchType).as(FetchType.class).orElseThrow();


        Integer rPage = runContext.render(this.page).as(Integer.class).orElse(null);
        Integer rSize = runContext.render(this.size).as(Integer.class).orElse(100);

        var kestraClient = kestraClient(runContext);

        java.util.List<AssetsControllerApiAsset> fetchedAssets;
        if (rPage != null) {
            fetchedAssets = kestraClient.assets().searchAssets(
                rPage,
                rSize,
                toQueryFilters(runContext.render(namespace).as(String.class).orElse(null), runContext.render(types).asList(String.class), runContext.render(metadataQuery).asList(FieldQuery.class)),
                runContext.flowInfo().tenantId(),
                null
            ).getResults();
        } else {
            fetchedAssets = new ArrayList<>();

            int currentPage = 1;
            long total;
            do {
                PagedResultsAssetsControllerApiAsset results = kestraClient.assets().searchAssets(
                    currentPage,
                    rSize,
                    toQueryFilters(runContext.render(namespace).as(String.class).orElse(null), runContext.render(types).asList(String.class), runContext.render(metadataQuery).asList(FieldQuery.class)),
                    runContext.flowInfo().tenantId(),
                    null
                );
                fetchedAssets.addAll(results.getResults());
                total = results.getTotal();
            } while ((long) currentPage++ * rSize < total);
        }

        Output.OutputBuilder outputBuilder = Output.builder();
        switch(renderedFetchType) {
            case FETCH_ONE -> outputBuilder
                .asset(fetchedAssets.getFirst())
                .size(1L);
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                    fetchedAssets.forEach(throwConsumer(asset -> {
                        final String s = JacksonMapper.ofIon().writeValueAsString(asset);
                        fileWriter.write(s);
                        fileWriter.write("\n");
                    }));
                }
                outputBuilder
                    .uri(runContext.storage().putFile(tempFile))
                    .size((long) fetchedAssets.size());
            }
            case FETCH -> outputBuilder
                .assets(fetchedAssets)
                .size((long) fetchedAssets.size());
            case NONE -> runContext.logger().info("fetchType is set to NONE, no output will be returned");
        }

        return outputBuilder.build();
    }

    private java.util.List<QueryFilter> toQueryFilters(String namespace, java.util.List<String> typesFilter, java.util.List<FieldQuery> metadataQuery) {
        java.util.List<QueryFilter> queryFilters = new ArrayList<>();

        if (namespace != null) {
            queryFilters.add(new QueryFilter()
                .field(QueryFilterField.NAMESPACE)
                .operation(QueryFilterOp.EQUALS)
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

        return queryFilters;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List containing the fetched assets.",
            description = "Only populated if using `fetchType=FETCH`."
        )
        private java.util.List<AssetsControllerApiAsset> assets;

        @Schema(
            title = "First fetched asset.",
            description = "Only populated if using `fetchType=FETCH_ONE`."
        )
        private AssetsControllerApiAsset asset;

        @Schema(
            title = "Kestra's internal storage URI of the stored assets.",
            description = "Only populated if using `fetchType=STORE`."
        )
        private URI uri;

        @Schema(
            title = "The number of fetched assets. Only populated if FetchType != NONE"
        )
        private Long size;
    }
}
