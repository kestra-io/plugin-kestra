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

import java.time.*;
import java.util.*;

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
                    thresholdMinutes: 60
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

    @Schema(title = "Threshold in minutes to consider a schedule overdue.")
    @Builder.Default
    private Property<Integer> thresholdMinutes = Property.ofValue(60);

    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);
        TriggersApi triggersApi = kestraClient.triggers();


        int threshold = runContext.render(this.thresholdMinutes).as(Integer.class).orElse(60);
        String tenantId = runContext.flowInfo().tenantId();
        String namespaceToUse = this.namespace != null ? this.namespace : runContext.flowInfo().namespace();

        runContext.logger().info(
            "🔍 Detecting stuck or disabled schedule triggers (tenant='{}', namespace='{}', threshold={} min)",
            tenantId, namespaceToUse, threshold
        );

        List<String> issues = new ArrayList<>();
        Instant now = Instant.now();

        int page = 1;
        int size = 100;
        boolean more = true;


        while (more) {
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
            int returned = results.size();
            runContext.logger().info("📦 Trigger API returned {} triggers (page {})", returned, page);

            if (results.isEmpty()) {
                break;
            }

            for (TriggerControllerTriggers trigger : results) {

                if (trigger.getAbstractTrigger() == null ||
                    !trigger.getAbstractTrigger().getType().contains("Schedule")) {
                    continue;
                }
                var context = trigger.getTriggerContext();
                if (context == null) continue;

                String flowNamespace = context.getNamespace();
                String flowId = context.getFlowId();
                String triggerId = context.getTriggerId();
                boolean disabled = Boolean.TRUE.equals(context.getDisabled());
                Instant lastExec = context.getDate() != null ? context.getDate().toInstant() : null;

                String name = flowNamespace + "." + flowId + "#" + triggerId;

                // 🚫 Disabled triggers
                if (disabled) {
                    issues.add(name + " (🚫 Disabled trigger)");
                    continue;
                }

                if (lastExec != null) {
                    long minutesLate = Duration.between(lastExec, now).toMinutes();
                    if (minutesLate >= threshold) {
                        issues.add(name + " (⏰ Stuck: last execution was " + minutesLate + " min ago)");
                    }
                } else {
                    issues.add(name + " (⚠️ Missing last execution timestamp)");
                }


            }

            more = returned == size;
            page++;
        }


        if (!issues.isEmpty()) {
            runContext.logger().warn("⚠️ Problematic triggers found: {}", issues.size());
            for (String issue : issues) {
                runContext.logger().warn(" - {}", issue);
            }
        }
        runContext.logger().info("✅ Detection complete: {} issues found", issues.size());
        return new Output(issues, issues.size());
    }

    @Data
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of stuck or disabled schedule triggers")
        private List<String> issues;

        @Schema(title = "Total number of issues found")
        private Integer totalFound;

        public Output(List<String> issues, Integer totalFound) {
            this.issues = issues;
            this.totalFound = totalFound;
        }
    }
}
