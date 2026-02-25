package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.StateType;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class PurgeAssetsTest extends AbstractKestraEeContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.assets.purge";

    @Test
    void purgeCombinationsShouldReturnExpectedCounts() throws Exception {
        for (boolean purgeAssets : List.of(false, true)) {
            for (boolean purgeUsages : List.of(false, true)) {
                for (boolean purgeLineages : List.of(false, true)) {
                    // Produces: 2 assets, 2 asset usages, 1 lineage event
                    TestData data = createAssetsAndExecutionData(NAMESPACE + ".matrix", Map.of("env", "test"));

                    RunContext runContext = runContextFactory.of();

                    PurgeAssets task = baseTaskBuilder()
                        .namespace(Property.ofValue(data.namespace()))
                        .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
                        .purgeAssets(Property.ofValue(purgeAssets))
                        .purgeAssetUsages(Property.ofValue(purgeUsages))
                        .purgeAssetLineages(Property.ofValue(purgeLineages))
                        .build();

                    PurgeAssets.PurgeOutput output = task.run(runContext);

                    assertThat(output, is(notNullValue()));
                    if (purgeAssets) {
                        assertThat(output.getPurgedAssetsCount(), is(2));
                    } else {
                        assertThat(output.getPurgedAssetsCount(), nullValue());
                    }

                    if (purgeUsages) {
                        assertThat(output.getPurgedAssetUsagesCount(), is(2));
                    } else {
                        assertThat(output.getPurgedAssetUsagesCount(), nullValue());
                    }

                    if (purgeLineages) {
                        assertThat(output.getPurgedAssetLineagesCount(), is(1));
                    } else {
                        assertThat(output.getPurgedAssetLineagesCount(), nullValue());
                    }

                    // Validate side-effect for assets only (usages/lineage list APIs might not be exposed in SDK).
                    if (purgeAssets) {
                        assertThat("Input asset should be deleted", kestraTestDataUtils.assetExists(data.inputAssetId()), is(false));
                        assertThat("Output asset should be deleted", kestraTestDataUtils.assetExists(data.outputAssetId()), is(false));
                    } else {
                        assertThat("Input asset should still exist", kestraTestDataUtils.assetExists(data.inputAssetId()), is(true));
                        assertThat("Output asset should still exist", kestraTestDataUtils.assetExists(data.outputAssetId()), is(true));
                    }

                    // Clean up remaining assets between combinations (avoid cross-run coupling).
                    purgeAllAssetsInNamespace(data.namespace());
                }
            }
        }
    }

    @Test
    void shouldFilterByNamespaceForAssetsAndUsages() throws Exception {
        TestData matching = createAssetsAndExecutionData(NAMESPACE + ".ns.matching", Map.of("env", "test"));
        TestData other = createAssetsAndExecutionData(NAMESPACE + ".ns.other", Map.of("env", "test"));

        RunContext runContext = runContextFactory.of();

        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(matching.namespace()))
            .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(true))
            .purgeAssetLineages(Property.ofValue(false))
            .build();

        PurgeAssets.PurgeOutput output = task.run(runContext);
        assertThat(output.getPurgedAssetsCount(), is(2));
        assertThat(output.getPurgedAssetUsagesCount(), is(2));

        assertThat(kestraTestDataUtils.assetExists(matching.inputAssetId()), is(false));
        assertThat(kestraTestDataUtils.assetExists(matching.outputAssetId()), is(false));
        assertThat(kestraTestDataUtils.assetExists(other.inputAssetId()), is(true));
        assertThat(kestraTestDataUtils.assetExists(other.outputAssetId()), is(true));

        purgeAllAssetsInNamespace(other.namespace());
    }

    @Test
    void shouldFilterByAssetIdForAssetsAndUsages() throws Exception {
        TestData data = createAssetsAndExecutionData(NAMESPACE + ".assetid", Map.of("env", "test"));

        RunContext runContext = runContextFactory.of();

        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(data.namespace()))
            .assetId(Property.ofValue(data.inputAssetId()))
            .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(true))
            .purgeAssetLineages(Property.ofValue(false))
            .build();

        PurgeAssets.PurgeOutput output = task.run(runContext);
        assertThat(output.getPurgedAssetsCount(), is(1));
        assertThat(output.getPurgedAssetUsagesCount(), is(1));

        assertThat(kestraTestDataUtils.assetExists(data.inputAssetId()), is(false));
        assertThat(kestraTestDataUtils.assetExists(data.outputAssetId()), is(true));

        purgeAllAssetsInNamespace(data.namespace());
    }

    @Test
    void shouldFilterByAssetTypeForAssetsOnly() throws Exception {
        String namespace = NAMESPACE + ".type";

        String inputAssetId = IdUtils.create();
        String outputAssetId = IdUtils.create();

        // Create two assets with different types, then generate usage/lineage via execution.
        kestraTestDataUtils.createAsset(namespace, inputAssetId, "TABLE", Map.of("env", "test"));
        kestraTestDataUtils.createAsset(namespace, outputAssetId, "TOPIC", Map.of("env", "test"));
        createExecutionProducingUsageAndLineage(namespace, inputAssetId, outputAssetId, "TABLE");

        RunContext runContext = runContextFactory.of();

        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(namespace))
            .assetType(Property.ofValue(List.of("TABLE")))
            .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(false))
            .purgeAssetLineages(Property.ofValue(false))
            .build();

        PurgeAssets.PurgeOutput output = task.run(runContext);
        assertThat(output.getPurgedAssetsCount(), is(1));
        assertThat(output.getPurgedAssetUsagesCount(), nullValue());
        assertThat(output.getPurgedAssetLineagesCount(), nullValue());

        assertThat(kestraTestDataUtils.assetExists(inputAssetId), is(false));
        assertThat(kestraTestDataUtils.assetExists(outputAssetId), is(true));

        purgeAllAssetsInNamespace(namespace);
    }

    @Test
    void shouldFilterByMetadataForAssetsOnly() throws Exception {
        String namespace = NAMESPACE + ".metadata";

        String matchingId = IdUtils.create();
        String nonMatchingId = IdUtils.create();

        kestraTestDataUtils.createAsset(namespace, matchingId, "TABLE", Map.of("env", "test", "team", "a"));
        kestraTestDataUtils.createAsset(namespace, nonMatchingId, "TABLE", Map.of("env", "test", "team", "b"));

        createExecutionProducingUsageAndLineage(namespace, matchingId, matchingId, "TABLE");
        createExecutionProducingUsageAndLineage(namespace, nonMatchingId, nonMatchingId, "TABLE");

        RunContext runContext = runContextFactory.of();

        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(namespace))
            .metadataQuery(Property.ofValue(java.util.List.of(
                new FieldQuery("team", QueryType.EQUAL_TO, "a")
            )))
            .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(false))
            .purgeAssetLineages(Property.ofValue(false))
            .build();

        PurgeAssets.PurgeOutput output = task.run(runContext);
        assertThat(output.getPurgedAssetsCount(), is(1));
        assertThat(output.getPurgedAssetUsagesCount(), nullValue());
        assertThat(output.getPurgedAssetLineagesCount(), nullValue());

        assertThat(kestraTestDataUtils.assetExists(matchingId), is(false));
        assertThat(kestraTestDataUtils.assetExists(nonMatchingId), is(true));

        purgeAllAssetsInNamespace(namespace);
    }

    @Test
    void endDateShouldPreventPurgingNewAssets() throws Exception {
        TestData data = createAssetsAndExecutionData(NAMESPACE + ".enddate", Map.of("env", "test"));

        RunContext runContext = runContextFactory.of();

        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(data.namespace()))
            .endDate(Property.ofValue(Instant.now().minus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(true))
            .purgeAssetLineages(Property.ofValue(true))
            .build();

        PurgeAssets.PurgeOutput output = task.run(runContext);
        assertThat(output.getPurgedAssetsCount(), is(0));
        assertThat(output.getPurgedAssetUsagesCount(), is(0));
        assertThat(output.getPurgedAssetLineagesCount(), is(0));

        assertThat(kestraTestDataUtils.assetExists(data.inputAssetId()), is(true));
        assertThat(kestraTestDataUtils.assetExists(data.outputAssetId()), is(true));

        purgeAllAssetsInNamespace(data.namespace());
    }

    @Test
    void lineageFilteringAssetIdOrTypeOrMetadataShouldThrowNotImplemented() {
        RunContext runContext = runContextFactory.of();

        PurgeAssets base = baseTaskBuilder()
            .namespace(Property.ofValue(NAMESPACE + ".lineage.notimpl"))
            .endDate(Property.ofValue(Instant.now()))
            .purgeAssets(Property.ofValue(false))
            .purgeAssetUsages(Property.ofValue(false))
            .purgeAssetLineages(Property.ofValue(true))
            .build();

        assertThrows(NotImplementedException.class, () -> base.toBuilder()
            .assetId(Property.ofValue("asset"))
            .build()
            .run(runContext));

        assertThrows(NotImplementedException.class, () -> base.toBuilder()
            .assetType(Property.ofValue(List.of("TABLE")))
            .build()
            .run(runContext));

        assertThrows(NotImplementedException.class, () -> base.toBuilder()
            .metadataQuery(Property.ofValue(java.util.List.of(new FieldQuery("k", QueryType.EQUAL_TO, "v"))))
            .build()
            .run(runContext));
    }

    @Test
    void usageFilteringOnAssetTypeOrMetadataShouldThrowNotImplemented() {
        RunContext runContext = runContextFactory.of();

        PurgeAssets base = baseTaskBuilder()
            .namespace(Property.ofValue(NAMESPACE + ".usage.notimpl"))
            .endDate(Property.ofValue(Instant.now()))
            .purgeAssets(Property.ofValue(false))
            .purgeAssetUsages(Property.ofValue(true))
            .purgeAssetLineages(Property.ofValue(false))
            .build();

        assertThrows(NotImplementedException.class, () -> base.toBuilder()
            .assetType(Property.ofValue(List.of("TABLE")))
            .build()
            .run(runContext));

        assertThrows(NotImplementedException.class, () -> base.toBuilder()
            .metadataQuery(Property.ofValue(java.util.List.of(new FieldQuery("k", QueryType.EQUAL_TO, "v"))))
            .build()
            .run(runContext));
    }

    @Test
    void shouldFilterByNamespacePrefixForAssetsAndUsages() throws Exception {
        String prefix = NAMESPACE + ".prefix";
        String subNamespace = prefix + ".sub";

        // Create assets/events in a sub-namespace and ensure purging with the prefix matches it.
        TestData inSubNamespace = createAssetsAndExecutionData(subNamespace, Map.of("env", "test"));

        // Control group: assets in a sibling namespace should not be purged.
        TestData sibling = createAssetsAndExecutionData(NAMESPACE + ".prefix_other", Map.of("env", "test"));

        RunContext runContext = runContextFactory.of();

        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(prefix))
            .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(true))
            .purgeAssetLineages(Property.ofValue(false))
            .build();

        PurgeAssets.PurgeOutput output = task.run(runContext);
        assertThat(output.getPurgedAssetsCount(), is(2));
        assertThat(output.getPurgedAssetUsagesCount(), is(2));

        assertThat("Assets in sub-namespace should be purged", kestraTestDataUtils.assetExists(inSubNamespace.inputAssetId()), is(false));
        assertThat("Assets in sub-namespace should be purged", kestraTestDataUtils.assetExists(inSubNamespace.outputAssetId()), is(false));

        assertThat("Assets in sibling namespace should not be purged", kestraTestDataUtils.assetExists(sibling.inputAssetId()), is(true));
        assertThat("Assets in sibling namespace should not be purged", kestraTestDataUtils.assetExists(sibling.outputAssetId()), is(true));

        purgeAllAssetsInNamespace(sibling.namespace());
    }

    // -------- helpers --------

    private PurgeAssets.PurgeAssetsBuilder<?, ?> baseTaskBuilder() {
        return PurgeAssets.builder()
            .id("purge_assets")
            .type(PurgeAssets.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build())
            .tenantId(Property.ofValue(TENANT_ID));
    }

    private void purgeAllAssetsInNamespace(String namespace) throws Exception {
        RunContext runContext = runContextFactory.of();
        PurgeAssets task = baseTaskBuilder()
            .namespace(Property.ofValue(namespace))
            .endDate(Property.ofValue(Instant.now().plus(Duration.ofDays(1))))
            .purgeAssets(Property.ofValue(true))
            .purgeAssetUsages(Property.ofValue(true))
            .purgeAssetLineages(Property.ofValue(true))
            .build();

        task.run(runContext);
    }

    private record TestData(String namespace, String inputAssetId, String outputAssetId) {
    }

    private TestData createAssetsAndExecutionData(String namespace, Map<String, String> metadata) throws Exception {
        String inputAssetId = IdUtils.create();
        String outputAssetId = IdUtils.create();

        kestraTestDataUtils.createAsset(namespace, inputAssetId, "TABLE", metadata);

        createExecutionProducingUsageAndLineage(namespace, inputAssetId, outputAssetId, "TABLE");

        return new TestData(namespace, inputAssetId, outputAssetId);
    }

    private void createExecutionProducingUsageAndLineage(String namespace, String inputAssetId, String outputAssetId, String outputType) throws Exception {
        String flowId = "asset_events_" + IdUtils.create().replace("-", "_");

        String flow = """
            id: "%s"
            namespace: "%s"
            tasks:
              - id: hello
                type: io.kestra.plugin.core.log.Log
                message: Hello World! 🚀
                assets:
                  inputs:
                    - id: %s
                  outputs:
                    - id: %s
                      namespace: %s
                      type: %s
            """.formatted(flowId, namespace, inputAssetId, outputAssetId, namespace, outputType);

        int assetUsagesBeforeExecution = kestraTestDataUtils.getAssetUsagesForNamespace(namespace).getTotal().intValue();
        int assetLineagesBeforeExecution = kestraTestDataUtils.getAssetLineagesForNamespace(namespace).getTotal().intValue();

        FlowWithSource created = kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flow);
        kestraTestDataUtils.createRandomizedExecution(created.getId(), created.getNamespace());

        Execution execution = queryExecution(created.getId(), namespace);
        Awaitility.await().until(() -> kestraTestDataUtils.getExecution(execution.getId()).getState().getCurrent() == StateType.SUCCESS);
        Awaitility.await().until(() -> kestraTestDataUtils.assetExists(inputAssetId) && kestraTestDataUtils.assetExists(outputAssetId));
        Awaitility.await().until(() -> {
            try {
                // One for input & one for output
                return kestraTestDataUtils.getAssetUsagesForNamespace(namespace).getTotal().equals(assetUsagesBeforeExecution + 2L);
            } catch (ApiException ignored) {
                // Not present yet
                return false;
            }
        });
        Awaitility.await().until(() -> {
            try {
                // One lineage event for the execution (covers both input & output assets)
                return kestraTestDataUtils.getAssetLineagesForNamespace(namespace).getTotal().equals(assetLineagesBeforeExecution + 1L);
            } catch (ApiException ignored) {
                // Not present yet
                return false;
            }
        });
    }

    private Execution queryExecution(String flowId, String namespace) throws Exception {
        RunContext runContext = runContextFactory.of();

        return Await.until(
            () -> {
                try {
                    Query searchTask = Query.builder()
                        .kestraUrl(Property.ofValue(KESTRA_URL))
                        .auth(AbstractKestraTask.Auth.builder()
                            .username(Property.ofValue(USERNAME))
                            .password(Property.ofValue(PASSWORD))
                            .build())
                        .tenantId(Property.ofValue(TENANT_ID))
                        .namespace(Property.ofValue(namespace))
                        .flowId(Property.ofValue(flowId))
                        .size(Property.ofValue(10))
                        .fetchType(Property.ofValue(FetchType.FETCH))
                        .build();

                    var output = searchTask.run(runContext);
                    if (output.getRows() == null || output.getRows().isEmpty()) {
                        return null;
                    }

                    Object row = output.getRows().getFirst();
                    if (row instanceof java.util.ArrayList<?> arrayList && !arrayList.isEmpty()) {
                        return (Execution) arrayList.getFirst();
                    }

                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            Duration.ofMillis(200),
            Duration.ofSeconds(5)
        );
    }
}
