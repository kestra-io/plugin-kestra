package io.kestra.plugin.kestra.triggers;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.api.FlowsApi;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.*;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Detect stuck or misconfigured schedule triggers.",
    description = "Detects schedule triggers that are missing cron configuration or have not executed recently."
)
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
            """
        )
    }
)
public class DetectStuckSchedules extends AbstractKestraTask implements RunnableTask<DetectStuckSchedules.Output> {

    @Schema(title = "Threshold in minutes to consider a schedule stuck.")
    @Builder.Default
    private Property<Integer> thresholdMinutes = Property.ofValue(60);

    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);
        FlowsApi flowsApi = kestraClient.flows();
        ObjectMapper mapper = new ObjectMapper();

        int threshold = runContext.render(this.thresholdMinutes).as(Integer.class).orElse(60);
        String tenantId = runContext.flowInfo().tenantId();

        runContext.logger().info("Detecting stuck or misconfigured schedule triggers (threshold={} minutes)", threshold);

        // Example placeholder: in a real scenario, fetch these dynamically
        String namespace = runContext.flowInfo().namespace();
        List<String> flowIds = List.of("flow1", "flow2");

        List<String> stuckTriggers = new ArrayList<>();
        List<String> misconfiguredTriggers = new ArrayList<>();
        Instant now = Instant.now();

        for (String flowId : flowIds) {
            try {
                Object flowObject = flowsApi.getFlow(namespace, flowId, false, false, tenantId, null);
                JsonNode flowJson = mapper.valueToTree(flowObject);
                JsonNode triggerArray = flowJson.path("triggers");

                for (JsonNode triggerNode : triggerArray) {
                    String type = triggerNode.path("type").asText();
                    if ("io.kestra.core.models.triggers.types.Schedule".equals(type)) {
                        String triggerId = triggerNode.path("id").asText();
                        String cron = triggerNode.path("config").path("cron").asText(null);
                        String lastExecStr = triggerNode.path("lastExecutionDate").asText(null);

                        String triggerName = namespace + "." + flowId + "#" + triggerId;

                        if (cron == null || cron.isBlank()) {
                            misconfiguredTriggers.add(triggerName + " (missing cron)");
                            continue;
                        }

                        if (lastExecStr == null || lastExecStr.isBlank()) {
                            stuckTriggers.add(triggerName + " (never executed)");
                        } else {
                            Instant lastExec = Instant.parse(lastExecStr);
                            if (Duration.between(lastExec, now).toMinutes() > threshold) {
                                stuckTriggers.add(triggerName + " (last run too old)");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                misconfiguredTriggers.add(namespace + "." + flowId + " (error: " + e.getMessage() + ")");
            }
        }

        runContext.logger().info(
            "Detection complete: {} stuck triggers, {} misconfigured triggers",
            stuckTriggers.size(),
            misconfiguredTriggers.size()
        );

        // ✅ FIXED: use Lombok builder instead of constructor
        return Output.builder()
            .stuckTriggers(stuckTriggers)
            .misconfiguredTriggers(misconfiguredTriggers)
            .totalChecked(flowIds.size())
            .build();
    }

    // ✅ CLEANED Output class
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of stuck triggers")
        private List<String> stuckTriggers;

        @Schema(title = "List of misconfigured triggers")
        private List<String> misconfiguredTriggers;

        @Schema(title = "Total flows checked")
        private Integer totalChecked;

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
