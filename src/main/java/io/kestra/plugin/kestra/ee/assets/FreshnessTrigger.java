package io.kestra.plugin.kestra.ee.assets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTrigger;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin
public class FreshnessTrigger extends AbstractKestraTrigger implements PollingTriggerInterface, TriggerOutput<FreshnessTrigger.Output> {
    private Property<String> assetId;

    private Property<String> namespace;

    private Property<String> assetType;

    @Schema(
        title = "Maximum allowed time since last update (e.g., PT24H, P1D)"
    )
    @NotNull
    private Property<Duration> maxStaleness;

    @Schema(
        title = "How often the trigger should check for stale assets. Default is 1 hour."
    )
    @Builder.Default
    private final Duration interval = Duration.ofHours(1);

    private Property<List<FieldQuery>> metadataQuery;

    @Hidden
    @Getter(AccessLevel.NONE)
    @Builder.Default
    Clock clock = Clock.systemDefaultZone();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws IllegalVariableEvaluationException {
        RunContext runContext = conditionContext.getRunContext();
        var kestraClient = kestraClient(runContext);

        List<AssetsControllerApiAsset> fetchedAssets = new ArrayList<>();
        int currentPage = 1;
        long total;
        int size = 100;
        Instant now = clock.instant();
        String tenantId = runContext.render(this.tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        do {
            PagedResultsAssetsControllerApiAsset results = kestraClient.assets().searchAssets(
                currentPage,
                size,
                toQueryFilters(
                    runContext.render(assetId).as(String.class).orElse(null),
                    runContext.render(namespace).as(String.class).orElse(null),
                    runContext.render(assetType).as(String.class).orElse(null),
                    runContext.render(metadataQuery).asList(FieldQuery.class),
                    now.minus(runContext.render(maxStaleness).as(Duration.class).orElseThrow())
                ),
                tenantId,
                null
            );
            fetchedAssets.addAll(results.getResults());
            total = results.getTotal();
        } while ((long) currentPage++ * size < total);

        if (fetchedAssets.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, Output.builder()
            .assets(fetchedAssets.stream()
                .map(fetchedAsset -> new AssetWithStaleInfo(
                    tenantId,
                    fetchedAsset.getNamespace(),
                    fetchedAsset.getId(),
                    fetchedAsset.getType(),
                    fetchedAsset.getDisplayName(),
                    fetchedAsset.getDescription(),
                    fetchedAsset.getMetadata(),
                    Optional.ofNullable(fetchedAsset.getCreated()).map(OffsetDateTime::toInstant).orElse(null),
                    Optional.ofNullable(fetchedAsset.getUpdated()).map(OffsetDateTime::toInstant).orElse(null),
                    false,
                    Duration.between(fetchedAsset.getUpdated().toInstant(), now),
                    now
                )).toList()
            ).build()));
    }

    private List<QueryFilter> toQueryFilters(String assetId, String namespace, String typeFilter, List<FieldQuery> metadataQuery, Instant updatedBefore) {
        List<QueryFilter> queryFilters = new ArrayList<>();

        if (assetId != null) {
            queryFilters.add(new QueryFilter()
                .field(QueryFilterField.ID)
                .operation(QueryFilterOp.EQUALS)
                .value(assetId));
        }

        if (namespace != null) {
            queryFilters.add(new QueryFilter()
                .field(QueryFilterField.NAMESPACE)
                .operation(QueryFilterOp.EQUALS)
                .value(namespace));
        }

        if (typeFilter != null) {
            queryFilters.add(new QueryFilter()
                .field(QueryFilterField.TYPE)
                .operation(QueryFilterOp.EQUALS)
                .value(typeFilter));
        }

        if (!metadataQuery.isEmpty()) {
            Map<QueryType, List<FieldQuery>> byOpType = metadataQuery.stream().collect(Collectors.groupingBy(FieldQuery::type));
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

        queryFilters.add(new QueryFilter()
            .field(QueryFilterField.UPDATED)
            .operation(QueryFilterOp.LESS_THAN_OR_EQUAL_TO)
            .value(updatedBefore)
        );

        return queryFilters;
    }

    @Getter
    public static class AssetWithStaleInfo extends Asset {
        private Duration staleDuration;
        private Instant checkTime;

        public AssetWithStaleInfo(String tenantId, String namespace, String id, String type, String displayName, String description, Map<String, Object> metadata, Instant created, Instant updated, boolean deleted, Duration staleDuration, Instant checkTime) {
            super(tenantId, namespace, id, type, displayName, description, metadata, created, updated, deleted);
            this.staleDuration = staleDuration;
            this.checkTime = checkTime;
        }

        @JsonProperty("lastUpdated")
        @Override
        public Instant getUpdated() {
            return super.getUpdated();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of stale assets"
        )
        List<AssetWithStaleInfo> assets;
    }
}
