package io.kestra.plugin.kestra.ee.cases;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerEvaluationResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.scheduler.model.TriggerState;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.AbstractKestraTrigger;
import io.kestra.sdk.model.CaseSeverity;

import jakarta.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CaseEventTest extends AbstractKestraEeContainerTest {
    // Server-side kestra.ee.cases.event-visibility-grace defaults to 1s: an event only shows once it is older than that.
    private static final Duration VISIBILITY_GRACE = Duration.ofMillis(1500);

    @Inject
    TestRunContextFactory runContextFactory;

    private static String uniqueNamespace() {
        return "kestra.tests.cases.caseevent." + IdUtils.create().toLowerCase();
    }

    private String createCase(String namespace, CaseSeverity severity) throws Exception {
        CreateCase createCase = CreateCase.builder()
            .id("open_case_" + IdUtils.create())
            .type(CreateCase.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace))
            .title(Property.ofValue("Case for CaseEvent test " + IdUtils.create()))
            .severity(Property.ofValue(severity))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, createCase, Map.of());
        return createCase.run(runContext).getCaseId();
    }

    private CaseEvent.CaseEventBuilder<?, ?> trigger(String id) {
        return CaseEvent.builder()
            .id(id)
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTrigger.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .events(Property.ofValue(List.of("CREATED")))
            .interval(Duration.ofMillis(200));
    }

    private static String stateKey(TriggerContext triggerContext) {
        return CaseEventCursor.defaultKey(triggerContext.getNamespace(), triggerContext.getFlowId(), triggerContext.getTriggerId());
    }

    private static CompletableFuture<List<TriggerEvaluationResult>> collect(CaseEvent trigger, Map.Entry<ConditionContext, TriggerState> context, int count) throws Exception {
        return Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(count)
            .doFinally(signal -> trigger.stop())
            .collectList()
            .toFuture();
    }

    private static void awaitCursorPersisted(Map.Entry<ConditionContext, TriggerState> context) throws Exception {
        RunContext runContext = context.getKey().getRunContext();
        String key = stateKey(context.getValue().context());
        Await.until(() -> {
            try {
                return CaseEventCursor.read(runContext, key).isPresent();
            } catch (Exception e) {
                return false;
            }
        }, Duration.ofMillis(50), Duration.ofSeconds(10));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> caseOf(TriggerEvaluationResult result) {
        return (Map<String, Object>) result.trigger().getVariables().get("case");
    }

    private static List<Object> caseIds(List<TriggerEvaluationResult> results) {
        return results.stream().map(result -> caseOf(result).get("id")).toList();
    }

    @Test
    void shouldEmitOneExecutionPerEventCreatedAfterTheTriggerStarted() throws Exception {
        String namespace = uniqueNamespace();
        createCase(namespace, CaseSeverity.LOW);

        CaseEvent trigger = trigger("case-event-trigger-" + IdUtils.create())
            .namespace(Property.ofValue(namespace))
            .build();
        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var future = collect(trigger, context, 2);
        awaitCursorPersisted(context);

        String first = createCase(namespace, CaseSeverity.LOW);
        String second = createCase(namespace, CaseSeverity.HIGH);

        List<TriggerEvaluationResult> results = future.get(30, TimeUnit.SECONDS);
        assertThat(caseIds(results), contains(first, second));
        assertThat(results.getFirst().trigger().getVariables().get("event"), is("CREATED"));
        assertThat(caseOf(results.getFirst()).containsKey("ticket"), is(true));
        assertThat(caseOf(results.getFirst()).get("ticket"), is(nullValue()));
    }

    @Test
    void shouldOnlyEmitEventsMatchingTheSeverityAndNamespaceFilters() throws Exception {
        String namespace = uniqueNamespace();

        CaseEvent trigger = trigger("case-event-filters-" + IdUtils.create())
            .severities(Property.ofValue(List.of(CaseSeverity.CRITICAL)))
            .namespace(Property.ofValue(namespace))
            .build();
        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var future = collect(trigger, context, 1);
        awaitCursorPersisted(context);

        createCase(namespace, CaseSeverity.LOW);
        createCase(uniqueNamespace(), CaseSeverity.CRITICAL);
        String matching = createCase(namespace, CaseSeverity.CRITICAL);

        assertThat(caseIds(future.get(30, TimeUnit.SECONDS)), contains(matching));
    }

    @Test
    void shouldResumeFromThePersistedCursorAfterARestart() throws Exception {
        String namespace = uniqueNamespace();
        String triggerId = "case-event-restart-" + IdUtils.create();

        CaseEvent firstTrigger = trigger(triggerId).namespace(Property.ofValue(namespace)).build();
        Map.Entry<ConditionContext, TriggerState> firstContext = TestsUtils.mockTrigger(runContextFactory, firstTrigger);
        var firstFuture = collect(firstTrigger, firstContext, 1);
        awaitCursorPersisted(firstContext);
        String beforeRestart = createCase(namespace, CaseSeverity.LOW);
        assertThat(caseIds(firstFuture.get(30, TimeUnit.SECONDS)), contains(beforeRestart));

        String whileStopped = createCase(namespace, CaseSeverity.LOW);

        CaseEvent secondTrigger = trigger(triggerId).namespace(Property.ofValue(namespace)).build();
        Map.Entry<ConditionContext, TriggerState> secondContext = TestsUtils.mockTrigger(runContextFactory, secondTrigger);
        assertThat(stateKey(secondContext.getValue().context()), is(stateKey(firstContext.getValue().context())));

        assertThat(caseIds(collect(secondTrigger, secondContext, 1).get(30, TimeUnit.SECONDS)), contains(whileStopped));
    }

    @Test
    void shouldPollAgainImmediatelyWhenTheServerReportsMoreEventsAreAvailable() throws Exception {
        String namespace = uniqueNamespace();
        // An interval no sleep could fit in: three one-event pages only arrive in time if hasMore skips the sleep.
        CaseEvent trigger = trigger("case-event-has-more-" + IdUtils.create())
            .namespace(Property.ofValue(namespace))
            .size(Property.ofValue(1))
            .interval(Duration.ofSeconds(30))
            .build();
        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        RunContext runContext = context.getKey().getRunContext();
        CaseEventCursor.write(runContext, stateKey(context.getValue().context()), new CaseEventCursor(Instant.now(), ""), null);

        List<String> created = List.of(
            createCase(namespace, CaseSeverity.LOW),
            createCase(namespace, CaseSeverity.LOW),
            createCase(namespace, CaseSeverity.LOW)
        );
        Thread.sleep(VISIBILITY_GRACE.toMillis());

        assertThat(caseIds(collect(trigger, context, 3).get(15, TimeUnit.SECONDS)), contains(created.toArray()));
    }

    @Test
    void shouldFailInsteadOfResettingTheCursorWhenTheStoredCursorIsUnreadable() throws Exception {
        CaseEvent trigger = trigger("case-event-corrupt-cursor-" + IdUtils.create()).build();
        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var triggerContext = context.getValue().context();
        String stateKey = stateKey(triggerContext);
        RunContext runContext = context.getKey().getRunContext();
        runContext.namespaceKv(runContext.flowInfo().namespace())
            .put(stateKey, new KVValueAndMetadata(null, "not-a-cursor"));

        Exception exception = assertThrows(
            Exception.class,
            () -> Flux.from(trigger.eval(context.getKey(), triggerContext))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signal -> trigger.stop())
                .blockLast(Duration.ofSeconds(10))
        );

        assertThat(exception.getMessage(), containsString("unreadable"));
        assertThat(runContext.namespaceKv(runContext.flowInfo().namespace()).getValue(stateKey).get().value(), is("not-a-cursor"));
    }

    @Test
    void shouldTerminateTheStreamOnAnUnrecoverableClientError() throws Exception {
        CaseEvent wrongPassword = trigger("case-event-unauthorized-" + IdUtils.create())
            .interval(Duration.ofMillis(50))
            .auth(
                AbstractKestraTrigger.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue("not-the-password"))
                    .build()
            )
            .build();
        assertStreamTerminatesOn(wrongPassword, 401);

        CaseEvent unknownEventType = trigger("case-event-bad-request-" + IdUtils.create())
            .interval(Duration.ofMillis(50))
            .events(Property.ofValue(List.of("NOT_A_TYPE")))
            .build();
        assertStreamTerminatesOn(unknownEventType, 400);
    }

    private void assertStreamTerminatesOn(CaseEvent trigger, int status) throws Exception {
        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        long start = System.nanoTime();
        Exception exception = assertThrows(
            Exception.class,
            () -> Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signal -> trigger.stop())
                .blockLast(Duration.ofSeconds(10))
        );

        assertThat(exception.getMessage(), containsString(String.valueOf(status)));
        assertThat(Duration.ofNanos(System.nanoTime() - start), greaterThanOrEqualTo(Duration.ofMillis(300)));
    }

    @Test
    void shouldNotRewriteTheCursorToKvWhenNothingChanged() throws Exception {
        // No other test produces TICKET_UNLINKED events, so the cursor has nothing to advance to.
        CaseEvent trigger = trigger("case-event-idle-" + IdUtils.create())
            .events(Property.ofValue(List.of("TICKET_UNLINKED")))
            .interval(Duration.ofMillis(50))
            // A TTL gives the persisted cursor an expirationDate that moves forward on every real write.
            .stateTtl(Property.ofValue(Duration.ofHours(1)))
            .build();
        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        RunContext runContext = context.getKey().getRunContext();
        String stateKey = stateKey(context.getValue().context());

        var subscription = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe();

        try {
            awaitCursorPersisted(context);
            Instant firstExpiration = readExpirationDate(runContext, stateKey);

            Thread.sleep(500);
            Instant secondExpiration = readExpirationDate(runContext, stateKey);

            assertThat(secondExpiration, is(firstExpiration));
        } finally {
            trigger.stop();
            subscription.dispose();
        }
    }

    private static Instant readExpirationDate(RunContext runContext, String stateKey) {
        return runContext.namespaceKv(runContext.flowInfo().namespace())
            .findMetadataAndValue(stateKey)
            .map(KVValueAndMetadata::metadata)
            .map(KVMetadata::getExpirationDate)
            .orElseThrow();
    }
}
