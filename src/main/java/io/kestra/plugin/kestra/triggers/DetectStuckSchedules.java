package io.kestra.plugin.kestra.triggers;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.Trigger;
import io.kestra.sdk.model.Flow;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Detect stuck or misconfigured schedule triggers.",
    description = "This task checks for schedule triggers that are either not firing as expected or have invalid configuration.")
@Plugin(
    examples = {
        @Example(
            title = "Detect stuck or misconfigured schedule triggers",
            full = true,
            code = """
            id: detect-schedules
            namespace: company.team

            tasks:
              - id: detect_stuck_schedules
                type: io.kestra.plugin.kestra.triggers.DetectStuckSchedules
                thresholdMinutes: 60
                auth:
                  apiToken: "{{ secrets('KESTRA_API_TOKEN') }}"
            """)
    })
public class DetectStuckSchedules extends AbstractKestraTask implements RunnableTask<DetectStuckSchedules.Output> {

    @Schema(title = "Threshold in minutes to consider a schedule stuck")
    @Builder.Default
    private Property<Integer> thresholdMinutes = Property.ofValue(60);

    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);
        String rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        int threshold = runContext.render(this.thresholdMinutes).as(Integer.class).orElse(60);

        runContext.logger().info("Detecting stuck or misconfigured schedule triggers with threshold={} minutes", threshold);

        List<Flow> flows = kestraClient.flows().list(rTenantId);
        List<Trigger> triggers = new ArrayList<>();

        for (Flow flow : flows) {
            if (flow.getTriggers() != null) {
                triggers.addAll(flow.getTriggers());
            }
        }

        List<String> stuck = new ArrayList<>();
        List<String> misconfigured = new ArrayList<>();

        for (Trigger trigger : triggers) {
            if ("io.kestra.core.models.triggers.types.Schedule".equals(trigger.getType())) {
                try {
                    Instant lastExecution = trigger.getLastExecutionDate();
                    String cron = (String) trigger.getConfig().get("cron");
                    Instant now = Instant.now();

                    if (cron == null || cron.isBlank()) {
                        misconfigured.add(trigger.getId() + " (missing cron)");
                        continue;
                    }

                    // Simple stuck check: if no execution or too old
                    if (lastExecution == null || Duration.between(lastExecution, now).toMinutes() > threshold) {
                        stuck.add(trigger.getId());
                    }
                } catch (Exception e) {
                    misconfigured.add(trigger.getId() + " (error: " + e.getMessage() + ")");
                }
            }
        }

        runContext.logger().info("Found {} stuck triggers and {} misconfigured triggers", stuck.size(), misconfigured.size());

        return Output.builder()
            .stuckTriggers(stuck)
            .misconfiguredTriggers(misconfigured)
            .totalChecked(triggers.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements Output.OutputValues {
        @Schema(title = "List of stuck triggers")
        private final List<String> stuckTriggers;

        @Schema(title = "List of misconfigured triggers")
        private final List<String> misconfiguredTriggers;

        @Schema(title = "Total triggers checked")
        private final Integer totalChecked;

        @Override
        public Map<String, Object> toMap() {
            return Map.of(
                "stuckTriggers", stuckTriggers,
                "misconfiguredTriggers", misconfiguredTriggers,
                "totalChecked", totalChecked
            );
        }
    }
}
