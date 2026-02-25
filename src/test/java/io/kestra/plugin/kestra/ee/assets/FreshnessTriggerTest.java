package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTrigger;
import io.kestra.sdk.model.AssetsControllerApiAsset;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ExtendWith(MockitoExtension.class)
@MicronautTest
public class FreshnessTriggerTest extends AbstractKestraEeContainerTest {
    @Mock
    private Clock clock;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void defaultClock() {
        FreshnessTrigger trigger = FreshnessTrigger.builder().build();
        assertThat(trigger.clock, is(Clock.systemDefaultZone()));
    }

    @Test
    void minimumParameters() throws Exception {
        FreshnessTrigger freshnessTrigger = FreshnessTrigger.builder()
            .id("freshness-trigger")
            .type(FreshnessTrigger.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .maxStaleness(Property.ofValue(Duration.ofHours(1)))
            .clock(clock)
            .build();

        AssetsControllerApiAsset asset = kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "ANY_TYPE", Map.of("a", "b"));
        AssetsControllerApiAsset secondAsset = kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "ANOTHER_TYPE", Map.of("a", "c"));
        // It's been more than an hour since the last update, so the trigger should fire
        Instant triggerTime = Instant.now().plus(Duration.ofHours(1));
        Mockito.doReturn(triggerTime).when(clock).instant();
        // This one should not be returned by the trigger since it's update date is less than an hour ago
        kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "ANY_TYPE", Map.of(
            "a", "d"
        ));

        Map.Entry<ConditionContext, Trigger> conditionContextTriggerEntry = TestsUtils.mockTrigger(runContextFactory, freshnessTrigger);
        Optional<Execution> evaluate = freshnessTrigger.evaluate(conditionContextTriggerEntry.getKey(), conditionContextTriggerEntry.getValue());

        assertThat(evaluate.isPresent(), is(true));
        List<Map<String, Object>> triggerOutputAssets = (List<Map<String, Object>>) evaluate.get().getTrigger().getVariables().get("assets");
        assertThat(
            triggerOutputAssets,
            hasItems(
                Matchers.<Map<?, ?>>allOf(
                    hasEntry("namespace", asset.getNamespace()),
                    hasEntry("id", asset.getId()),
                    hasEntry("type", asset.getType()),
                    hasEntry("metadata", Map.of("a", "b"))
                ),
                Matchers.<Map<?, ?>>allOf(
                    hasEntry("namespace", secondAsset.getNamespace()),
                    hasEntry("id", secondAsset.getId()),
                    hasEntry("type", secondAsset.getType()),
                    hasEntry("metadata", Map.of("a", "c"))
                )
            )
        );

        // The following assertions can't be done above as we get truncated asset update dates from the asset sdk so we need to compare two truncated dates to avoid false negatives
        Map<String, Object> firstAssetMap = triggerOutputAssets.stream().filter(m -> m.get("id").equals(asset.getId())).findFirst().get();
        assertThat(
            Instant.parse((String) firstAssetMap.get("lastUpdated")).truncatedTo(ChronoUnit.MICROS),
            is(asset.getUpdated().toInstant().truncatedTo(ChronoUnit.MICROS))
        );
        assertThat(
            Instant.parse(((String) firstAssetMap.get("checkTime"))).truncatedTo(ChronoUnit.MICROS),
            is(triggerTime.truncatedTo(ChronoUnit.MICROS))
        );
        assertThat(
            JacksonMapper.ofJson().convertValue(firstAssetMap.get("staleDuration").toString(), Duration.class).toMillis(),
            is(Duration.between(asset.getUpdated().toInstant(), triggerTime).toMillis())
        );

        Map<String, Object> secondAssetMap = triggerOutputAssets.stream().filter(m -> m.get("id").equals(secondAsset.getId())).findFirst().get();
        assertThat(
            Instant.parse((String) secondAssetMap.get("lastUpdated")).truncatedTo(ChronoUnit.MICROS),
            is(secondAsset.getUpdated().toInstant().truncatedTo(ChronoUnit.MICROS))
        );
        assertThat(
            Instant.parse((String) secondAssetMap.get("checkTime")).truncatedTo(ChronoUnit.MICROS),
            is(triggerTime.truncatedTo(ChronoUnit.MICROS))
        );
        assertThat(
            JacksonMapper.ofJson().convertValue(secondAssetMap.get("staleDuration").toString(), Duration.class).toMillis(),
            is(Duration.between(secondAsset.getUpdated().toInstant(), triggerTime).toMillis())
        );
    }

    @Test
    void namespaceFilter() throws Exception {
        String wantedNamespace = TestsUtils.randomNamespace();

        AssetsControllerApiAsset matching = kestraTestDataUtils.createAsset(wantedNamespace, TestsUtils.randomString(), "TYPE_A", Map.of("a", "b"));
        // another asset in different namespace should not be returned
        kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "TYPE_A", Map.of("a", "b"));

        FreshnessTrigger freshnessTrigger = FreshnessTrigger.builder()
            .id("freshness-namespace-filter")
            .type(FreshnessTrigger.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .maxStaleness(Property.ofValue(Duration.ofHours(1)))
            .namespace(Property.ofValue(wantedNamespace))
            .clock(clock)
            .build();

        Instant triggerTime = Instant.now().plus(Duration.ofHours(1));
        Mockito.doReturn(triggerTime).when(clock).instant();

        Map.Entry<ConditionContext, Trigger> conditionContextTriggerEntry = TestsUtils.mockTrigger(runContextFactory, freshnessTrigger);
        Optional<Execution> evaluate = freshnessTrigger.evaluate(conditionContextTriggerEntry.getKey(), conditionContextTriggerEntry.getValue());

        assertThat(evaluate.isPresent(), is(true));
        List<Map<String, Object>> triggerOutputAssets = (List<Map<String, Object>>) evaluate.get().getTrigger().getVariables().get("assets");
        assertThat(triggerOutputAssets, hasSize(1));
        Map<String, Object> assetMap = triggerOutputAssets.get(0);
        assertThat(assetMap.get("namespace"), is(matching.getNamespace()));
        assertThat(assetMap.get("id"), is(matching.getId()));
    }

    @Test
    void typeFilter() throws Exception {
        String wantedType = "ONLY_TYPE_" + TestsUtils.randomString();

        AssetsControllerApiAsset matching = kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), wantedType, Map.of("a", "b"));
        // another asset with different type should not be returned
        kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "OTHER_TYPE", Map.of("a", "b"));

        FreshnessTrigger freshnessTrigger = FreshnessTrigger.builder()
            .id("freshness-type-filter")
            .type(FreshnessTrigger.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .maxStaleness(Property.ofValue(Duration.ofHours(1)))
            .assetType(Property.ofValue(wantedType))
            .clock(clock)
            .build();

        Instant triggerTime = Instant.now().plus(Duration.ofHours(1));
        Mockito.doReturn(triggerTime).when(clock).instant();

        Map.Entry<ConditionContext, Trigger> conditionContextTriggerEntry = TestsUtils.mockTrigger(runContextFactory, freshnessTrigger);
        Optional<Execution> evaluate = freshnessTrigger.evaluate(conditionContextTriggerEntry.getKey(), conditionContextTriggerEntry.getValue());

        assertThat(evaluate.isPresent(), is(true));
        List<Map<String, Object>> triggerOutputAssets = (List<Map<String, Object>>) evaluate.get().getTrigger().getVariables().get("assets");
        assertThat(triggerOutputAssets, hasSize(1));
        Map<String, Object> assetMap = triggerOutputAssets.get(0);
        assertThat(assetMap.get("type"), is(matching.getType()));
        assertThat(assetMap.get("id"), is(matching.getId()));
    }

    @Test
    void metadataFilter() throws Exception {
        // create assets with different metadata values
        String namespace = TestsUtils.randomNamespace();
        AssetsControllerApiAsset kv1Asset = kestraTestDataUtils.createAsset(namespace, TestsUtils.randomString(), "TYPE_X", Map.of("k", "v1"));
        AssetsControllerApiAsset kv2Asset = kestraTestDataUtils.createAsset(namespace, TestsUtils.randomString(), "TYPE_X", Map.of("k", "v2"));

        // EQUAL
        FreshnessTrigger freshnessTrigger = FreshnessTrigger.builder()
            .id("freshness-metadata-filter")
            .type(FreshnessTrigger.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .maxStaleness(Property.ofValue(Duration.ofHours(1)))
            .namespace(Property.ofValue(namespace))
            .metadataQuery(Property.ofValue(List.of(new FieldQuery("k", QueryType.EQUAL_TO, "v1"))))
            .clock(clock)
            .build();

        Instant triggerTime = Instant.now().plus(Duration.ofHours(1));
        Mockito.doReturn(triggerTime).when(clock).instant();

        Map.Entry<ConditionContext, Trigger> conditionContextTriggerEntry = TestsUtils.mockTrigger(runContextFactory, freshnessTrigger);
        Optional<Execution> evaluate = freshnessTrigger.evaluate(conditionContextTriggerEntry.getKey(), conditionContextTriggerEntry.getValue());

        assertThat(evaluate.isPresent(), is(true));
        List<Map<String, Object>> triggerOutputAssets = (List<Map<String, Object>>) evaluate.get().getTrigger().getVariables().get("assets");
        assertThat(triggerOutputAssets, hasSize(1));
        Map<String, Object> assetMap = triggerOutputAssets.get(0);
        assertThat(((Map<String, Object>) assetMap.get("metadata")).get("k"), is("v1"));
        assertThat(assetMap.get("id"), is(kv1Asset.getId()));

        // NOT EQUAL
        freshnessTrigger = FreshnessTrigger.builder()
            .id("freshness-metadata-filter")
            .type(FreshnessTrigger.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .maxStaleness(Property.ofValue(Duration.ofHours(1)))
            .namespace(Property.ofValue(namespace))
            .metadataQuery(Property.ofValue(List.of(new FieldQuery("k", QueryType.NOT_EQUAL_TO, "v1"))))
            .clock(clock)
            .build();

        evaluate = freshnessTrigger.evaluate(conditionContextTriggerEntry.getKey(), conditionContextTriggerEntry.getValue());

        assertThat(evaluate.isPresent(), is(true));
        triggerOutputAssets = (List<Map<String, Object>>) evaluate.get().getTrigger().getVariables().get("assets");
        assertThat(triggerOutputAssets, hasSize(1));
        assetMap = triggerOutputAssets.get(0);
        assertThat(((Map<String, Object>) assetMap.get("metadata")).get("k"), is("v2"));
        assertThat(assetMap.get("id"), is(kv2Asset.getId()));
    }

    @Test
    void assetIdFilter() throws Exception {
        // create two assets and assert that only the one with the specific id is returned
        AssetsControllerApiAsset wanted = kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "TYPE_ID", Map.of("a", "b"));
        kestraTestDataUtils.createAsset(TestsUtils.randomNamespace(), TestsUtils.randomString(), "TYPE_ID", Map.of("a", "b"));

        FreshnessTrigger freshnessTrigger = FreshnessTrigger.builder()
            .id("freshness-assetid-filter")
            .type(FreshnessTrigger.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .maxStaleness(Property.ofValue(Duration.ofHours(1)))
            .assetId(Property.ofValue(wanted.getId()))
            .clock(clock)
            .build();

        Instant triggerTime = Instant.now().plus(Duration.ofHours(1));
        Mockito.doReturn(triggerTime).when(clock).instant();

        Map.Entry<ConditionContext, Trigger> conditionContextTriggerEntry = TestsUtils.mockTrigger(runContextFactory, freshnessTrigger);
        Optional<Execution> evaluate = freshnessTrigger.evaluate(conditionContextTriggerEntry.getKey(), conditionContextTriggerEntry.getValue());

        assertThat(evaluate.isPresent(), is(true));
        List<Map<String, Object>> triggerOutputAssets = (List<Map<String, Object>>) evaluate.get().getTrigger().getVariables().get("assets");
        assertThat(triggerOutputAssets, hasSize(1));
        Map<String, Object> assetMap = triggerOutputAssets.get(0);
        assertThat(assetMap.get("id"), is(wanted.getId()));
    }


    private AbstractKestraTrigger.Auth basicAuth() {
        return AbstractKestraTrigger.Auth.builder()
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .build();
    }
}
