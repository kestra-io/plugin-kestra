package io.kestra.plugin.kestra.triggers;


import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.api.TriggersApi;
import io.kestra.sdk.model.PagedResultsTriggerControllerTriggers;
import io.kestra.sdk.model.TriggerControllerTriggers;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Detect stuck or disabled schedule triggers in Kestra.",
    description = """
        Detects schedule triggers that are overdue (stuck) or disabled using the Trigger API.
        Works at the tenant level, for a specific namespace, or for a single flow.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Detect stuck or disabled schedule triggers",
            full = true,
            code = """
                id: detect-schedules
                namespace: company.team

                tasks:
                  - id: detect_stuck_schedules
                    type: io.kestra.plugin.kestra.triggers.DetectStuckOrDisabledSchedules
                    namespace: "company.team"
                    threshold: "PT5M"   # ISO-8601 duration (5 minutes)
                    auth:
                      username: "admin"
                      password: "yourpassword"
                """
        )
    }
)
public class DetectStuckOrDisabledSchedules extends AbstractKestraTask
    implements RunnableTask<DetectStuckOrDisabledSchedules.Output> {

    @PluginProperty
    private String namespace;

    @Schema(title = "Threshold duration to consider a schedule overdue (default 1 minute).")
    @Builder.Default
    private Property<Duration> threshold = Property.ofValue(Duration.ofMinutes(1));

    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);
        TriggersApi triggersApi = kestraClient.triggers();

Duration thresholdDuration = runContext.render(this.threshold)
            .as(Duration.class)
            .orElse(Duration.ofMinutes(1));

        
        String tenantId = runContext.flowInfo().tenantId();
        String namespaceToUse = this.namespace != null ? this.namespace : runContext.flowInfo().namespace();

        runContext.logger().info(
            "Detecting stuck or disabled schedule triggers (tenant='{}', namespace='{}', threshold={} min)",
            tenantId, namespaceToUse, thresholdDuration
        );

        List<TriggerId> disabledTriggers = new ArrayList<>();
        List<TriggerId> stuckTriggers = new ArrayList<>();
        Instant now = Instant.now();

        int page = 1;
        int size = 100;
       


        while (true) {
            PagedResultsTriggerControllerTriggers triggersResponse = triggersApi.searchTriggers(
                page,
                size,
                tenantId,
                null,
                null,
                null,
                namespaceToUse,
                null,
                null
            );

            List<TriggerControllerTriggers> results = triggersResponse.getResults();


            if (results == null ||results.isEmpty()) {
                break;
            }

            for (TriggerControllerTriggers trigger : results) {

                if (trigger.getAbstractTrigger() == null ||
                    !trigger.getAbstractTrigger().getType().equals("io.kestra.plugin.core.trigger.Schedule")) {
                    continue;
                }
                var context = trigger.getTriggerContext();
                if (context == null) continue;

                TriggerId triggerId = new TriggerId(
                    tenantId,
                    context.getNamespace(),
                    context.getFlowId(),
                    context.getTriggerId()
                );
                boolean isDisabled = Boolean.TRUE.equals(trigger.getAbstractTrigger().getDisabled());
                Instant nextExec = context.getNextExecutionDate() != null
                    ? context.getNextExecutionDate().toInstant()
                    : null;


                if (isDisabled) {
                    disabledTriggers.add(triggerId);
                } else if (nextExec != null && now.isAfter(nextExec.plus(thresholdDuration))) {
                    stuckTriggers.add(triggerId);
                }
            }
        // Break if we have no more pages
            if (results.size() < size) {
                break;
            }
            
            page++;
        }

        int totalIssues = disabledTriggers.size() + stuckTriggers.size();

        runContext.logger().info("Detection complete: {} issues found", totalIssues);
        return new Output(disabledTriggers, stuckTriggers, totalIssues);
    }

    @Data
    public static class TriggerId {
        private String tenantId;
        private String namespace;
        private String flowId;
        private String triggerId;

        public TriggerId(String tenantId, String namespace, String flowId, String triggerId) {
            this.tenantId = tenantId;
            this.namespace = namespace;
            this.flowId = flowId;
            this.triggerId = triggerId;
        }
    }


    @Data
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of disabled schedule triggers")
        private List<TriggerId> disabledTriggers;

        @Schema(title = "List of stuck schedule triggers")
        private List<TriggerId> stuckTriggers;

        @Schema(title = "Total number of issues found")
        private Integer totalFound;

       

        public Output(List<TriggerId> disabledTriggers, List<TriggerId> stuckTriggers, Integer totalFound) {
            this.disabledTriggers = disabledTriggers;
            this.stuckTriggers = stuckTriggers;
            this.totalFound = totalFound;
            
        }
    }
}
