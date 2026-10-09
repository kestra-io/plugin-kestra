package io.kestra.plugin.kestra.ee.cases;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerEvaluationResult;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTrigger;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.CaseSeverity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on Kestra case events",
    description = "Streams Kestra EE case events (created, assigned, commented on, etc.) and emits one execution per event, filtered server-side by event type, severity, namespace and assignee."
)
@Plugin(
    examples = {
        @Example(
            title = "Open an external ticket for critical cases.",
            full = true,
            code = """
                id: open_ticket_for_critical_cases
                namespace: system

                triggers:
                  - id: on_case_created
                    type: io.kestra.plugin.kestra.ee.cases.CaseEvent
                    auth:
                      apiToken: "{{ secret('KESTRA_API_TOKEN') }}"
                    events: [CREATED]
                    severities: [CRITICAL]
                    namespace: company

                tasks:
                  - id: open_issue
                    type: io.kestra.plugin.github.issues.Create
                    oauthToken: "{{ secret('GITHUB_TOKEN') }}"
                    repository: acme/ops
                    title: "{{ trigger.case.title }}"
                    body: |
                      {{ trigger.case.description is defined ? trigger.case.description : '' }}

                      Severity: {{ trigger.case.severity }}
                      Kestra case: {{ trigger.case.url }}
                """
        ),
        @Example(
            title = "Start the triage agent when a case is assigned to it.",
            full = true,
            code = """
                id: start_triage_agent_on_assignment
                namespace: system

                triggers:
                  - id: on_case_assigned
                    type: io.kestra.plugin.kestra.ee.cases.CaseEvent
                    auth:
                      apiToken: "{{ secret('KESTRA_API_TOKEN') }}"
                    events: [ASSIGNED]
                    assignees:
                      - triage-agent@kestra.io

                tasks:
                  - id: run_triage
                    type: io.kestra.plugin.core.log.Log
                    message: "Case {{ trigger.case.id }} assigned: {{ trigger.case.title }}"
                """
        )
    }
)
public class CaseEvent extends AbstractKestraTrigger implements RealtimeTriggerInterface, TriggerOutput<CaseEvent.Output> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_SIZE = 100;
    private static final long SLEEP_CHUNK_MILLIS = 100;

    @Schema(title = "Event types to react to", description = "Any of the CaseEventType values (e.g. CREATED, ASSIGNED, COMMENT); all types when empty.")
    private Property<List<String>> events;

    @Schema(title = "Case severities to keep", description = "Any of CRITICAL, HIGH, MEDIUM, LOW; all severities when empty.")
    private Property<List<CaseSeverity>> severities;

    @Schema(title = "Namespace filter", description = "Prefix match on the case's namespace, as with the Flow trigger; null scans all namespaces.")
    private Property<String> namespace;

    @Schema(title = "Assignee filter", description = "Keep cases assigned to any of these user or group ids; all assignees when empty.")
    private Property<List<String>> assignees;

    @Schema(
        title = "Polling interval",
        description = "How often to poll for new case events once the previous page came back short of a full page. Defaults to PT10S."
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(10);

    @Schema(title = "Page size", description = "How many events to examine per poll. Defaults to 100.")
    @Builder.Default
    private Property<Integer> size = Property.ofValue(DEFAULT_SIZE);

    @Schema(
        title = "Cursor state key",
        description = "Namespace KV key backing this trigger's persisted cursor. Defaults to a key derived from the namespace, flow id and trigger id, so two triggers on the same flow keep separate cursors; set explicitly to have two triggers share one."
    )
    private Property<String> stateKey;

    @Schema(
        title = "Cursor state time-to-live",
        description = "How long the persisted cursor lives in the namespace KV store, counted from the last time the cursor actually advanced rather than from the last poll attempt. Unset by default: once it expires, the next poll cannot tell an expired cursor from one that was never written, so it reinitializes to now and silently skips any event created during the gap, rather than replaying from the beginning. Only set this if that bounded-skip trade-off is acceptable, and account for stretches where the cursor legitimately does not advance (no matching events)."
    )
    private Property<Duration> stateTtl;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Override
    public Publisher<TriggerEvaluationResult> eval(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        return Flux.create(emitter -> {
            emitter.onDispose(waitForTermination::countDown);
            AtomicReference<Throwable> error = new AtomicReference<>();
            try {
                var client = kestraClient(runContext);
                var tenantId = runContext.render(this.tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
                var rEvents = runContext.render(events).asList(String.class);
                var rSeverities = runContext.render(severities).asList(CaseSeverity.class);
                var rNamespace = runContext.render(namespace).as(String.class).orElse(null);
                var rAssignees = runContext.render(assignees).asList(String.class);
                var rSize = runContext.render(size).as(Integer.class).orElse(DEFAULT_SIZE);

                var stateKeyResolved = runContext.render(stateKey).as(String.class)
                    .orElseGet(() -> CaseEventCursor.defaultKey(context.getNamespace(), context.getFlowId(), context.getTriggerId()));
                var stateTtlResolved = runContext.render(stateTtl).as(Duration.class).orElse(null);

                var cursor = CaseEventCursor.read(runContext, stateKeyResolved).orElseGet(() -> {
                    var fresh = new CaseEventCursor(Instant.now(), "");
                    CaseEventCursor.write(runContext, stateKeyResolved, fresh, stateTtlResolved);
                    return fresh;
                });
                String sinceCreated = cursor.created().toString();
                String sinceId = cursor.id();
                var lastPersistedCursor = cursor;

                while (isActive.get() && !emitter.isCancelled()) {
                    try {
                        SearchResponse searchResponse;
                        try {
                            var body = client.cases().searchCaseEvents(tenantId, sinceCreated, sinceId, rEvents, rSeverities, rNamespace, rAssignees, rSize);
                            searchResponse = MAPPER.convertValue(body, SearchResponse.class);
                        } catch (ApiException e) {
                            if (e.getCode() == 400 || e.getCode() == 401 || e.getCode() == 422) {
                                throw new UnrecoverableCaseEventsException("Case events search returned HTTP %s: %s".formatted(e.getCode(), e.getResponseBody()));
                            }
                            throw new IllegalStateException("Case events search returned HTTP " + e.getCode() + ": " + e.getResponseBody(), e);
                        }
                        var results = searchResponse.results != null ? searchResponse.results : List.<EventWithCase>of();

                        for (var entry : results) {
                            var triggerCase = new TriggerCase(
                                entry.theCase.id,
                                entry.theCase.title,
                                entry.theCase.description,
                                entry.theCase.severity,
                                entry.theCase.status,
                                entry.theCase.namespace,
                                entry.theCase.url,
                                entry.theCase.ticket
                            );
                            var output = new Output(entry.event.type, Instant.parse(entry.event.created), triggerCase);
                            emitter.next(TriggerService.generateRealtimeEvaluationResult(this, conditionContext, output));
                        }

                        sinceCreated = searchResponse.nextSinceCreated;
                        sinceId = searchResponse.nextSinceId;
                        var newCursor = new CaseEventCursor(Instant.parse(sinceCreated), sinceId);
                        boolean cursorAdvanced = !newCursor.equals(lastPersistedCursor);
                        if (cursorAdvanced) {
                            // Persisted after emitting: a crash replays rather than skips, and the generated flow's runIf guard is what makes a replay safe.
                            CaseEventCursor.write(runContext, stateKeyResolved, newCursor, stateTtlResolved);
                            lastPersistedCursor = newCursor;
                        }

                        // hasMore alone would spin forever on a page held back entirely by the visibility grace
                        // (cursor unchanged, nothing to show yet); only skip the sleep when the cursor actually moved.
                        if (!searchResponse.hasMore || !cursorAdvanced) {
                            sleepInterval(emitter);
                        }
                    } catch (UnrecoverableCaseEventsException | IllegalArgumentException e) {
                        throw e;
                    } catch (Exception e) {
                        runContext.logger().warn("Case events poll failed, retrying in {}: {}", interval, e.getMessage());
                        sleepInterval(emitter);
                    }
                }
            } catch (Throwable e) {
                error.set(e);
            } finally {
                Throwable throwable = error.get();
                if (throwable != null) {
                    emitter.error(throwable);
                } else {
                    emitter.complete();
                }
            }
        });
    }

    /** Sleeps in short slices so {@link #kill()} does not block for a whole (possibly long) interval. */
    private void sleepInterval(reactor.core.publisher.FluxSink<TriggerEvaluationResult> emitter) {
        long remainingMillis = interval.toMillis();
        while (remainingMillis > 0 && isActive.get() && !emitter.isCancelled()) {
            long chunk = Math.min(SLEEP_CHUNK_MILLIS, remainingMillis);
            try {
                Thread.sleep(chunk);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isActive.set(false);
                return;
            }
            remainingMillis -= chunk;
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class UnrecoverableCaseEventsException extends RuntimeException {
        UnrecoverableCaseEventsException(String message) {
            super(message);
        }
    }

    // --- Raw JSON models mirroring CasesController's ApiCaseEventSearchResult / ApiCaseEventWithCase / ApiTriggerCase ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class SearchResponse {
        public List<EventWithCase> results;
        public String nextSinceCreated;
        public String nextSinceId;
        public boolean hasMore;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class EventWithCase {
        public EventDef event;
        @JsonProperty("case")
        public CaseDef theCase;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class EventDef {
        public String type;
        public String created;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class CaseDef {
        public String id;
        public String title;
        public String description;
        public String severity;
        public String status;
        public String namespace;
        public String url;
        public Map<String, String> ticket;
    }

    public record Output(String event, Instant created, @JsonProperty("case") TriggerCase theCase) implements io.kestra.core.models.tasks.Output {
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record TriggerCase(
        String id,
        String title,
        String description,
        String severity,
        String status,
        String namespace,
        String url,
        // present for the external-ticket replay guard: runIf: "{{ trigger.case.ticket is not defined }}"
        Map<String, String> ticket) {
    }
}
