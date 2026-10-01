package io.kestra.plugin.kestra.ee.cases;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerEvaluationResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.scheduler.model.TriggerState;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraTrigger;

import jakarta.inject.Inject;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CaseEventTest {
    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static final String THREE_EVENTS_JSON = """
        {
          "results": [
            {
              "event": {"type": "CREATED", "created": "2024-01-01T00:00:00Z"},
              "case": {"id": "case-1", "title": "First case", "description": "desc-1", "severity": "CRITICAL", "status": "OPEN", "namespace": "company.team", "url": "http://localhost/cases/case-1", "ticket": null}
            },
            {
              "event": {"type": "ASSIGNED", "created": "2024-01-01T00:00:01Z"},
              "case": {"id": "case-2", "title": "Second case", "description": "desc-2", "severity": "HIGH", "status": "OPEN", "namespace": "company.team", "url": "http://localhost/cases/case-2", "ticket": null}
            },
            {
              "event": {"type": "COMMENT", "created": "2024-01-01T00:00:02Z"},
              "case": {"id": "case-3", "title": "Third case", "description": "desc-3", "severity": "LOW", "status": "OPEN", "namespace": "company.team", "url": "http://localhost/cases/case-3", "ticket": {"system": "jira", "key": "OPS-1", "url": "http://jira/OPS-1"}}
            }
          ],
          "nextSinceCreated": "2024-01-01T00:00:02Z",
          "nextSinceId": "event-3"
        }
        """;

    // Once the trigger has advanced past the first response, the endpoint has nothing left to say;
    // held at the same cursor, exactly like the real endpoint holds it when a page is empty.
    private static final String EMPTY_RESULTS_JSON = """
        {"results": [], "nextSinceCreated": "2024-01-01T00:00:02Z", "nextSinceId": "event-3"}
        """;

    /**
     * The trigger now always sends a cursor (never a bare, cursor-less request), so a freshly-initialized
     * one is told apart from an advanced one by its empty {@code sinceId}. That request gets all three
     * events in one response; any later request (a real, non-empty {@code sinceId}) gets none. A
     * one-execution-per-poll implementation could only reach 3 emitted items by making a second or third
     * request, which this stub starves - only a single response emitting all three events can satisfy
     * {@code take(3)}.
     */
    private static String cursorAwareResponse(String query) {
        return hasEmptySinceId(query) ? THREE_EVENTS_JSON : EMPTY_RESULTS_JSON;
    }

    private static boolean hasEmptySinceId(String query) {
        return query != null && (query.contains("sinceId=&") || query.endsWith("sinceId="));
    }

    private String startStub(Function<String, String> responseForQuery) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/main/cases/events/search", exchange -> {
            byte[] body = responseForQuery.apply(exchange.getRequestURI().getQuery()).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        return "http://localhost:" + server.getAddress().getPort();
    }

    @Test
    void shouldEmitOneExecutionPerEvent() throws Exception {
        // given a stub returning three events in one response
        String stubUrl = startStub(CaseEventTest::cursorAwareResponse);

        CaseEvent trigger = CaseEvent.builder()
            .id("case-event-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        List<TriggerEvaluationResult> results = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(3)
            .doFinally(signal -> trigger.stop())
            .collectList()
            .block(Duration.ofSeconds(10));

        assertThat(results, hasSize(3));
    }

    @Test
    void shouldSendEventAndNamespaceFiltersToTheApi() throws Exception {
        AtomicReference<String> capturedQuery = new AtomicReference<>();
        String stubUrl = startStub(query -> {
            capturedQuery.compareAndSet(null, query);
            return cursorAwareResponse(query);
        });

        CaseEvent trigger = CaseEvent.builder()
            .id("case-event-trigger-filters-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .events(Property.ofValue(List.of("CREATED")))
            .namespace(Property.ofValue("company.team"))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        List<TriggerEvaluationResult> results = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(3)
            .doFinally(signal -> trigger.stop())
            .collectList()
            .block(Duration.ofSeconds(10));

        assertThat(results, hasSize(3));
        assertThat(capturedQuery.get(), containsString("events=CREATED"));
        assertThat(capturedQuery.get(), containsString("namespace=company.team"));
    }

    private static final String NO_NEW_EVENTS_JSON = """
        {"results": [], "nextSinceCreated": "2099-01-01T00:00:00Z", "nextSinceId": "none"}
        """;

    private static String preExistingEventJson(Instant created) {
        return """
            {
              "results": [
                {
                  "event": {"type": "CREATED", "created": "%s"},
                  "case": {"id": "pre-existing-case", "title": "Pre-existing", "description": "d", "severity": "LOW", "status": "OPEN", "namespace": "company.team", "url": "http://localhost/cases/pre-existing-case", "ticket": null}
                }
              ],
              "nextSinceCreated": "%s",
              "nextSinceId": "pre-existing-event"
            }
            """.formatted(created, created);
    }

    // com.sun.net.httpserver.HttpExchange#getRequestURI decodes the query string, so no further decoding is needed here.
    private static Instant extractSinceCreated(String query) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("sinceCreated=")) {
                String value = param.substring("sinceCreated=".length());
                return value.isEmpty() ? null : Instant.parse(value);
            }
        }
        return null;
    }

    @Test
    void shouldNotEmitPreExistingEventsWhenNoCursorIsPersisted() throws Exception {
        // given a stub that behaves like a real cursor-respecting backend: it only returns the
        // pre-existing event when asked for everything since the beginning, which a trigger that
        // correctly initializes its cursor to "now" must never do
        Instant preExisting = Instant.now().minus(Duration.ofDays(1));
        AtomicReference<String> firstQuery = new AtomicReference<>();

        String stubUrl = startStub(query -> {
            firstQuery.compareAndSet(null, query);
            Instant requestedSince = extractSinceCreated(query);
            boolean shouldReturnPreExistingEvent = requestedSince == null || !requestedSince.isAfter(preExisting);
            return shouldReturnPreExistingEvent ? preExistingEventJson(preExisting) : NO_NEW_EVENTS_JSON;
        });

        CaseEvent trigger = CaseEvent.builder()
            .id("fresh-cursor-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        List<TriggerEvaluationResult> results = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(Duration.ofMillis(300))
            .doFinally(signal -> trigger.stop())
            .collectList()
            .block(Duration.ofSeconds(10));

        assertThat(results, is(empty()));
        assertThat(firstQuery.get(), containsString("sinceCreated="));
    }

    private static final String EVENTS_4_TO_5_JSON = """
        {
          "results": [
            {
              "event": {"type": "CREATED", "created": "2024-01-01T00:00:03Z"},
              "case": {"id": "case-4", "title": "Fourth case", "description": "desc-4", "severity": "HIGH", "status": "OPEN", "namespace": "company.team", "url": "http://localhost/cases/case-4", "ticket": null}
            },
            {
              "event": {"type": "CREATED", "created": "2024-01-01T00:00:04Z"},
              "case": {"id": "case-5", "title": "Fifth case", "description": "desc-5", "severity": "MEDIUM", "status": "OPEN", "namespace": "company.team", "url": "http://localhost/cases/case-5", "ticket": null}
            }
          ],
          "nextSinceCreated": "2024-01-01T00:00:04Z",
          "nextSinceId": "event-5"
        }
        """;

    private static final String HELD_AT_EVENT_5_JSON = """
        {"results": [], "nextSinceCreated": "2024-01-01T00:00:04Z", "nextSinceId": "event-5"}
        """;

    @Test
    void shouldResumeFromThePersistedCursorAfterARestart() throws Exception {
        // unique per run: /tmp/unittest backs the KV store on real disk, so a fixed id would collide
        // with the cursor a previous run of this same test left behind and never advance past it
        String triggerId = "restart-trigger-" + IdUtils.create();
        AtomicReference<String> resumedQuery = new AtomicReference<>();
        String stubUrl = startStub(query -> {
            if (query != null && query.contains("sinceId=event-3")) {
                resumedQuery.compareAndSet(null, query);
                return EVENTS_4_TO_5_JSON;
            }
            if (query != null && query.contains("sinceId=event-5")) {
                return HELD_AT_EVENT_5_JSON;
            }
            return THREE_EVENTS_JSON;
        });

        // first trigger instance consumes events 1-3 and persists the cursor
        CaseEvent firstTrigger = CaseEvent.builder()
            .id(triggerId)
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .build();
        Map.Entry<ConditionContext, TriggerState> firstContext = TestsUtils.mockTrigger(runContextFactory, firstTrigger);

        List<TriggerEvaluationResult> firstResults = Flux.from(firstTrigger.eval(firstContext.getKey(), firstContext.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(3)
            .doFinally(signal -> firstTrigger.stop())
            .collectList()
            .block(Duration.ofSeconds(10));
        assertThat(firstResults, hasSize(3));
        // guards against a false pass: without this, a broken read() that always reports "no cursor"
        // would still leave secondTrigger emitting 2 items via the stub's cursor-less fallback
        assertThat(resumedQuery.get(), is(nullValue()));

        var firstTriggerContext = firstContext.getValue().context();
        String stateKey = CaseEventCursor.defaultKey(firstTriggerContext.getNamespace(), firstTriggerContext.getFlowId(), firstTriggerContext.getTriggerId());
        RunContext firstRunContext = firstContext.getKey().getRunContext();
        assertThat(firstRunContext.namespaceKv(firstRunContext.flowInfo().namespace()).getValue(stateKey).isPresent(), is(true));

        // a second instance, built fresh from the same flow/trigger id, must request sinceId=<event 3>
        // and emit only events 4-5 - neither replaying 1-3 nor skipping 4
        CaseEvent secondTrigger = CaseEvent.builder()
            .id(triggerId)
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .build();
        Map.Entry<ConditionContext, TriggerState> secondContext = TestsUtils.mockTrigger(runContextFactory, secondTrigger);

        var secondTriggerContext = secondContext.getValue().context();
        assertThat(
            CaseEventCursor.defaultKey(secondTriggerContext.getNamespace(), secondTriggerContext.getFlowId(), secondTriggerContext.getTriggerId()),
            is(stateKey)
        );

        List<TriggerEvaluationResult> secondResults = Flux.from(secondTrigger.eval(secondContext.getKey(), secondContext.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(2)
            .doFinally(signal -> secondTrigger.stop())
            .collectList()
            .block(Duration.ofSeconds(10));

        assertThat(secondResults, hasSize(2));
        assertThat(resumedQuery.get(), containsString("sinceId=event-3"));
        assertThat(resumedQuery.get(), containsString("sinceCreated=2024-01-01T00:00:02Z"));
    }

    @Test
    void shouldKeepTicketAsAnExplicitNullInsteadOfDroppingItFromTheOutput() throws Exception {
        // THREE_EVENTS_JSON's first case has "ticket": null - the exact shape workstream E's runIf guard depends on.
        String stubUrl = startStub(CaseEventTest::cursorAwareResponse);

        CaseEvent trigger = CaseEvent.builder()
            .id("ticket-null-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        List<TriggerEvaluationResult> results = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(1)
            .doFinally(signal -> trigger.stop())
            .collectList()
            .block(Duration.ofSeconds(10));

        assertThat(results, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> caseMap = (Map<String, Object>) results.getFirst().trigger().getVariables().get("case");
        assertThat(caseMap.containsKey("ticket"), is(true));
        assertThat(caseMap.get("ticket"), is(nullValue()));
    }

    private static final String HAS_MORE_STEP_1_JSON = """
        {"results": [{"event": {"type": "CREATED", "created": "2024-02-01T00:00:00Z"}, "case": {"id": "c1", "title": "t", "description": "d", "severity": "LOW", "status": "OPEN", "namespace": "company.team", "url": "http://x/c1", "ticket": null}}], "nextSinceCreated": "2024-02-01T00:00:00Z", "nextSinceId": "step-1", "hasMore": true}
        """;

    private static final String HAS_MORE_STEP_2_JSON = """
        {"results": [{"event": {"type": "CREATED", "created": "2024-02-01T00:00:01Z"}, "case": {"id": "c2", "title": "t", "description": "d", "severity": "LOW", "status": "OPEN", "namespace": "company.team", "url": "http://x/c2", "ticket": null}}], "nextSinceCreated": "2024-02-01T00:00:01Z", "nextSinceId": "step-2", "hasMore": true}
        """;

    private static final String HAS_MORE_STEP_3_JSON = """
        {"results": [{"event": {"type": "CREATED", "created": "2024-02-01T00:00:02Z"}, "case": {"id": "c3", "title": "t", "description": "d", "severity": "LOW", "status": "OPEN", "namespace": "company.team", "url": "http://x/c3", "ticket": null}}], "nextSinceCreated": "2024-02-01T00:00:02Z", "nextSinceId": "step-3", "hasMore": false}
        """;

    @Test
    void shouldPollAgainImmediatelyWhenTheServerReportsMoreEventsAreAvailable() throws Exception {
        // A long interval that a sleep between any of the three polls would blow straight through -
        // the only way to collect 3 events within the tight block() deadline below is to never sleep while hasMore is true.
        String stubUrl = startStub(query -> {
            if (query != null && query.contains("sinceId=step-2")) {
                return HAS_MORE_STEP_3_JSON;
            }
            if (query != null && query.contains("sinceId=step-1")) {
                return HAS_MORE_STEP_2_JSON;
            }
            return HAS_MORE_STEP_1_JSON;
        });

        CaseEvent trigger = CaseEvent.builder()
            .id("has-more-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofSeconds(5))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        List<TriggerEvaluationResult> results = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .take(3)
            .doFinally(signal -> trigger.stop())
            .collectList()
            .block(Duration.ofSeconds(3));

        assertThat(results, hasSize(3));
    }

    private static final String HAS_MORE_STUCK_JSON = """
        {"results": [], "nextSinceCreated": "2024-03-01T00:00:00Z", "nextSinceId": "stuck", "hasMore": true}
        """;

    @Test
    void shouldStillSleepWhenHasMoreIsTrueButTheCursorNeverAdvances() throws Exception {
        // A misbehaving server always reporting hasMore=true without ever advancing past its own fixed
        // cursor - hasMore alone must not be trusted to skip the sleep, or the trigger spins in a tight
        // loop of HTTP requests instead of honoring the configured interval.
        AtomicInteger requestCount = new AtomicInteger();
        String stubUrl = startStub(query -> {
            requestCount.incrementAndGet();
            return HAS_MORE_STUCK_JSON;
        });

        CaseEvent trigger = CaseEvent.builder()
            .id("stuck-has-more-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofSeconds(2))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Disposable subscription = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe();

        try {
            Thread.sleep(300);
            // Without the cursor-advanced guard this would be in the hundreds (a tight, sleep-free loop);
            // with it: one request that genuinely advances the fresh cursor, one more that finds it
            // already stuck, then a 2s sleep - at most 2 within this window.
            assertThat(requestCount.get(), lessThanOrEqualTo(2));
        } finally {
            trigger.stop();
            subscription.dispose();
        }
    }

    @Test
    void shouldTerminateTheStreamOnAnUnrecoverableBadRequest() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/main/cases/events/search", exchange -> {
            byte[] body = "Invalid 'events' filter value 'NOT_A_TYPE'".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(400, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        String stubUrl = "http://localhost:" + server.getAddress().getPort();

        CaseEvent trigger = CaseEvent.builder()
            .id("bad-request-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Exception exception = assertThrows(
            Exception.class,
            () -> Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signal -> trigger.stop())
                .blockLast(Duration.ofSeconds(10))
        );

        assertThat(exception.getMessage(), containsString("400"));
    }

    @Test
    void shouldNotRewriteTheCursorToKvWhenNothingChanged() throws Exception {
        // A stub that never advances past its own fixed cursor, like a real backend drained of new events.
        String stubUrl = startStub(query -> EMPTY_RESULTS_JSON);

        CaseEvent trigger = CaseEvent.builder()
            .id("idle-trigger-" + IdUtils.create())
            .type(CaseEvent.class.getName())
            .kestraUrl(Property.ofValue(stubUrl))
            .auth(AbstractKestraTrigger.Auth.builder().apiToken(Property.ofValue("token")).build())
            .tenantId(Property.ofValue("main"))
            .interval(Duration.ofMillis(50))
            // A TTL gives the persisted cursor an expirationDate that moves forward on every real write -
            // the one observable signal, short of instrumenting the KV store, that a write happened at all.
            .stateTtl(Property.ofValue(Duration.ofHours(1)))
            .build();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        RunContext runContext = context.getKey().getRunContext();
        var triggerContext = context.getValue().context();
        String stateKey = CaseEventCursor.defaultKey(triggerContext.getNamespace(), triggerContext.getFlowId(), triggerContext.getTriggerId());

        Disposable subscription = Flux.from(trigger.eval(context.getKey(), context.getValue().context()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe();

        try {
            // Long enough for the bootstrap write and the one genuine advance to the stub's fixed cursor to settle.
            Thread.sleep(300);
            Instant firstExpiration = readExpirationDate(runContext, stateKey);

            // Several more idle polls at the (short) configured interval, all against the same fixed cursor.
            Thread.sleep(300);
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
