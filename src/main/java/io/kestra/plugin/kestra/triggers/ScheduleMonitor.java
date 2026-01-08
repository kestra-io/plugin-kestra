package io.kestra.plugin.kestra.triggers;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.core.trigger.Schedule;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.*;
import io.kestra.sdk.model.Trigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Detect stuck or misconfigured Schedule triggers.",
    description = """
        Detects SCHEDULE triggers that are overdue (stuck), misconfigured,
        or (optionally) disabled.
        """
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
                      username: "{{ secret( KESTRA_USERNAME ) }}" # Pass your Kestra username as a secret
                      password: "{{ secret('KESTRA_PASSWORD') }}" # Pass your Kestra password as secret
                    interval: PT2M
                """
        )
    }
)
public class ScheduleMonitor extends AbstractTrigger implements TriggerOutput<ScheduleMonitor.Output>, PollingTriggerInterface {
    private static final String DEFAULT_KESTRA_URL = "http://localhost:8080";
    private static final String KESTRA_URL_TEMPLATE = "{{ kestra.url }}";

    @Schema(title = "Kestra API URL. If null, uses 'kestra.url' from configuration. If that is also null, defaults to 'http://localhost:8080'.")
    private Property<String> kestraUrl;

    @Schema(title = "Authentication information.")
    private AbstractKestraTask.Auth auth;

    @Schema(title = "The tenant ID to use for the request, defaults to current tenant.")
    @Setter
    protected Property<String> tenantId;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Limit the check to a given namespace.")
    private Property<String> namespace;

    @Schema(title = "Limit the check to a specific flow.")
    private Property<String> flowId;

    @Schema(title = "Whether to include disabled schedules in the check. Default: false.")
    @Builder.Default
    private Property<Boolean> includeDisabled = Property.ofValue(false);

    @Schema(
        title = "Maximum allowed delay for a schedule before being considered stuck.",
        description = "If the next execution time is older than now minus this delay, the trigger is marked as stuck."
    )
    @Builder.Default
    private Property<Duration> allowedDelay = Property.ofValue(Duration.ofMinutes(1));

    @Schema(
        title = "Maximum allowed execution duration",
        description = "If set, a schedule-triggered execution RUNNING longer than this duration is considered stuck."
    )
    private Property<Duration> maxExecutionDuration;

    @Schema(
        title = "Expected maximum interval between executions",
        description = "If no execution happened within this interval, the schedule is considered unhealthy."
    )
    private Property<Duration> maxExecutionInterval;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Output result = runChecks(runContext);

        if (result.getData().isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, result));
    }

    public Output runChecks(RunContext runContext) throws Exception {
        KestraClient client = kestraClient(runContext);

        String tenantId = runContext.flowInfo().tenantId();
        String rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        String rFlowId = runContext.render(flowId).as(String.class).orElse(null);
        boolean rIncludeDisabled = runContext.render(includeDisabled).as(Boolean.class).orElse(false);
        Duration rMaxExecutionDuration = runContext.render(maxExecutionDuration).as(Duration.class).orElse(null);
        Duration rMaxExecutionInterval = runContext.render(maxExecutionInterval).as(Duration.class).orElse(null);
        Duration rAllowedDelay = runContext.render(allowedDelay).as(Duration.class).orElse(Duration.ofMinutes(1));

        List<TriggerInfo> detectedTriggers = new ArrayList<>();

        int page = 1;
        int size = 100;
        long total = Long.MAX_VALUE;

        List<QueryFilter> filters = new ArrayList<>();

        if (rNamespace != null) {
            filters.add(new QueryFilter()
                .field(QueryFilterField.NAMESPACE)
                .operation(QueryFilterOp.STARTS_WITH)
                .value(rNamespace)
            );
        }

        if (rFlowId != null) {
            filters.add(new QueryFilter()
                .field(QueryFilterField.FLOW_ID)
                .operation(QueryFilterOp.EQUALS)
                .value(rFlowId)
            );
        }

        while ((long) page * size < total) {
            PagedResultsTriggerControllerTriggers response = client.triggers().searchTriggers(page, size, tenantId, null, filters);

            total = response.getTotal();
            List<TriggerControllerTriggers> results = response.getResults();

            if (results.isEmpty()) {
                break;
            }

            for (TriggerControllerTriggers t : results) {

                if (t.getAbstractTrigger() == null || !Schedule.class.getName().equals(t.getAbstractTrigger().getType())) {
                    continue;
                }

                Trigger triggerContext = t.getTriggerContext();

                if (triggerContext == null) continue;

                boolean isDisabled = Boolean.TRUE.equals(t.getAbstractTrigger().getDisabled()) || Boolean.TRUE.equals(triggerContext.getDisabled());

                Instant now = Instant.now();
                Instant lastExec = triggerContext.getDate() != null ? triggerContext.getDate().toInstant() : null;
                Instant nextExec = triggerContext.getNextExecutionDate() != null ? triggerContext.getNextExecutionDate().toInstant() : null;

                TriggerInfo info = TriggerInfo.builder()
                    .namespace(triggerContext.getNamespace())
                    .flowId(triggerContext.getFlowId())
                    .triggerId(triggerContext.getTriggerId())
                    .lastExecution(lastExec)
                    .expectedNext(nextExec)
                    .build();

                if (isDisabled) {
                    if (rIncludeDisabled) {
                        detectedTriggers.add(info);
                    }
                    continue;
                }

                if (triggerContext.getBackfill() != null) {
                    continue;
                }

                if (rMaxExecutionDuration != null && triggerContext.getExecutionId() != null) {
                    io.kestra.sdk.model.Execution exec = client.executions().execution(triggerContext.getExecutionId(), tenantId);

                    if (exec.getState() != null && exec.getState().getCurrent() == StateType.RUNNING && exec.getState().getStartDate() != null) {

                        Instant start = exec.getState().getStartDate().toInstant();
                        if (Duration.between(start, now).compareTo(rMaxExecutionDuration) > 0) {
                            detectedTriggers.add(info);
                            continue;
                        }
                    }
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

    protected KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        String rKestraUrl = runContext.render(kestraUrl).as(String.class)
            .orElseGet(() -> {
                try {
                    return runContext.render(KESTRA_URL_TEMPLATE);
                } catch (IllegalVariableEvaluationException e) {
                    return DEFAULT_KESTRA_URL;
                }
            });

        runContext.logger().info("Kestra URL: {}", rKestraUrl);

        String normalizedUrl = rKestraUrl.trim().replaceAll("/+$", "");

        var builder = KestraClient.builder();
        builder.url(normalizedUrl);
        if (auth != null) {
            if (auth.getApiToken() != null && (auth.getUsername() != null || auth.getPassword() != null)) {
                throw new IllegalArgumentException("Cannot use both API Token authentication and HTTP Basic authentication");
            }

            String rApiToken = runContext.render(auth.getApiToken()).as(String.class).orElse(null);
            if (rApiToken != null) {
                builder.tokenAuth(rApiToken);
                return builder.build();
            }

            Optional<String> maybeUsername = runContext.render(auth.getUsername()).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.getPassword()).as(String.class);
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                builder.basicAuth(maybeUsername.get(), maybePassword.get());
                return builder.build();
            }

            throw new IllegalArgumentException("Both username and password are required for HTTP Basic authentication");
        }
        return builder.build();
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
    public static class Auth {
        @Schema(title = "API token")
        private Property<String> apiToken;

        @Schema(title = "Username for HTTP basic authentication")
        private Property<String> username;

        @Schema(title = "Password for HTTP basic authentication")
        private Property<String> password;
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
