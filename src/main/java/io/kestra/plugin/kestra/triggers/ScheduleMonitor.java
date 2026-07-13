package io.kestra.plugin.kestra.triggers;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.core.trigger.Schedule;
import io.kestra.plugin.kestra.AbstractKestraTrigger;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Detect unhealthy schedule triggers",
    description = "Finds SCHEDULE triggers that are overdue, missing next execution, or optionally disabled. Defaults: poll every 60s, allowedDelay 1 minute, disabled ignored unless includeDisabled is true."
)
@Plugin(
    examples = {
        @Example(
            title = "Detect stuck, unhealthy, or disabled schedule triggers",
            full = true,
            code = """
                id: detect_schedule_triggers
                namespace: company.team

                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data.size }} triggers detected"

                triggers:
                  - id: stuck_schedules
                    type: io.kestra.plugin.kestra.triggers.ScheduleMonitor
                    includeDisabled: true
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}" # Pass your Kestra username as a secret
                      password: "{{ secret('KESTRA_PASSWORD') }}" # Pass your Kestra password as secret
                    interval: PT2M
                """
        )
    }
)
public class ScheduleMonitor extends AbstractKestraTrigger implements TriggerOutput<ScheduleMonitor.Output>, PollingTriggerInterface {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Namespace filter", description = "Prefix match; null scans all namespaces.")
    @PluginProperty(group = "connection")
    private Property<String> namespace;

    @Schema(title = "Flow filter")
    @PluginProperty(group = "advanced")
    private Property<String> flowId;

    @Schema(title = "Include disabled schedules", description = "Defaults to false.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> includeDisabled = Property.ofValue(false);

    @Schema(
        title = "Allowed delay past next run",
        description = "Defaults to PT1M. If now is after next execution plus this delay, trigger is flagged."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Duration> allowedDelay = Property.ofValue(Duration.ofMinutes(1));

    @Schema(
        title = "Max idle interval between runs",
        description = "Flags schedules with no execution within this period."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxExecutionInterval;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var result = runChecks(runContext);

        if (result.getData().isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, result));
    }

    public Output runChecks(RunContext runContext) throws Exception {
        var baseUrl = resolveKestraUrl(runContext);
        var authHeader = resolveAuthorizationHeader(runContext);
        var httpClient = HttpClient.newHttpClient();

        var tenantId = runContext.render(this.tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        var rFlowId = runContext.render(flowId).as(String.class).orElse(null);
        var rIncludeDisabled = runContext.render(includeDisabled).as(Boolean.class).orElse(false);
        var rMaxExecutionInterval = runContext.render(maxExecutionInterval).as(Duration.class).orElse(null);
        var rAllowedDelay = runContext.render(allowedDelay).as(Duration.class).orElse(Duration.ofMinutes(1));

        List<TriggerInfo> detectedTriggers = new ArrayList<>();

        // Build filter query params in the SDK's format: filters[<camelCaseField>][<OP>]=value
        // The SDK converts NAMESPACE → "namespace", FLOW_ID → "flowId" via toCamelCaseFromFolder.
        var filterParams = new StringBuilder();
        if (rNamespace != null) {
            filterParams.append("&filters%5Bnamespace%5D%5BSTARTS_WITH%5D=").append(encode(rNamespace));
        }
        if (rFlowId != null) {
            filterParams.append("&filters%5BflowId%5D%5BEQUALS%5D=").append(encode(rFlowId));
        }

        int page = 1;
        int size = 100;
        long total = Long.MAX_VALUE;

        while ((long) (page - 1) * size < total) {
            var url = baseUrl + "/api/v1/" + tenantId + "/triggers/search?page=" + page + "&size=" + size + filterParams;
            var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET();

            if (authHeader != null) {
                requestBuilder.header("Authorization", authHeader);
            }

            var response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Triggers search returned HTTP " + response.statusCode() + ": " + response.body());
            }

            var searchResponse = MAPPER.readValue(response.body(), TriggerSearchResponse.class);

            total = searchResponse.total;
            var results = searchResponse.results;

            if (results == null || results.isEmpty()) {
                break;
            }

            for (var entry : results) {
                if (entry.trigger == null || !Schedule.class.getName().equals(entry.trigger.type)) {
                    continue;
                }

                var state = entry.state;
                if (state == null) {
                    continue;
                }

                // v1.3: disabled flag is only on the trigger definition; v2.0 also surfaces it on state
                var isDisabled = Boolean.TRUE.equals(entry.trigger.disabled) || Boolean.TRUE.equals(state.disabled);

                var now = Instant.now();
                var lastExec = state.evaluatedAt != null ? Instant.parse(state.evaluatedAt) : null;
                var nextExec = state.nextEvaluationDate != null ? Instant.parse(state.nextEvaluationDate) : null;

                var info = TriggerInfo.builder()
                    .namespace(state.namespace)
                    .flowId(state.flowId)
                    .triggerId(state.triggerId)
                    .lastExecution(lastExec)
                    .expectedNext(nextExec)
                    .build();

                if (isDisabled) {
                    if (rIncludeDisabled) {
                        detectedTriggers.add(info);
                    }
                    continue;
                }

                if (state.backfill != null) {
                    continue;
                }

                if (rMaxExecutionInterval != null && lastExec != null) {
                    if (Duration.between(lastExec, now).compareTo(rMaxExecutionInterval) > 0) {
                        detectedTriggers.add(info);
                        continue;
                    }
                }

                if (nextExec == null) {
                    detectedTriggers.add(info);
                    continue;
                }

                if (now.isAfter(nextExec.plus(rAllowedDelay))) {
                    detectedTriggers.add(info);
                }
            }

            page++;
        }

        return Output.builder()
            .data(detectedTriggers)
            .build();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    // --- Raw JSON models with @JsonAlias to support both v1.3 and v2.0 field names ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TriggerSearchResponse {
        public long total;
        public List<TriggerEntry> results;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TriggerEntry {
        // v2.0: "trigger", v1.3: "abstractTrigger"
        @JsonProperty("trigger")
        @JsonAlias("abstractTrigger")
        public TriggerDef trigger;

        // v2.0: "state", v1.3: "triggerContext"
        @JsonProperty("state")
        @JsonAlias("triggerContext")
        public TriggerStateDef state;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TriggerDef {
        public String id;
        public String type;
        public Boolean disabled;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TriggerStateDef {
        public String namespace;
        public String flowId;
        public String triggerId;

        // v2.0: "evaluatedAt", v1.3: "date"
        @JsonProperty("evaluatedAt")
        @JsonAlias("date")
        public String evaluatedAt;

        // v2.0: "nextEvaluationDate", v1.3: "nextExecutionDate"
        @JsonProperty("nextEvaluationDate")
        @JsonAlias("nextExecutionDate")
        public String nextEvaluationDate;

        public Boolean disabled;
        public Object backfill;
    }

    @Getter
    @Builder
    public static class TriggerInfo {
        String namespace;
        String flowId;
        String triggerId;
        Instant lastExecution;
        Instant expectedNext;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of unhealthy schedule triggers",
            description = """
                    Includes stuck or misconfigured schedule triggers.
                    Disabled triggers included only when includeDisabled = true.
                """
        )
        List<TriggerInfo> data;
    }
}
