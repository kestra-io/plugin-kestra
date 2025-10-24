package io.kestra.plugin.kestra.triggers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.api.TriggersApi;
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

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

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

        try {
            while (more) {
                Object triggersResponse = triggersApi.searchTriggers(
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

                String json = mapper.writeValueAsString(triggersResponse);
                JsonNode root = mapper.readTree(json);
                JsonNode results = root.path("results");
                int returned = results == null ? 0 : results.size();
                runContext.logger().info("📦 Trigger API returned {} triggers (page {})", returned, page);

                if (results == null || returned == 0) break;

                for (JsonNode triggerNode : results) {
                    try {
                        String type = triggerNode.path("abstractTrigger").path("type").asText(null);
                        if (type == null || !type.contains("Schedule")) continue;

                        String flowNamespace = triggerNode.path("triggerContext").path("namespace").asText(null);
                        String flowId = triggerNode.path("triggerContext").path("flowId").asText(null);
                        String triggerId = triggerNode.path("triggerContext").path("triggerId").asText(null);
                        boolean disabled = triggerNode.path("triggerContext").path("disabled").asBoolean(false);
                        double epoch = triggerNode.path("triggerContext").path("date").asDouble(0);

                        String name = flowNamespace + "." + flowId + "#" + triggerId;
                        runContext.logger().info("Trigger: {} -> disabled={} lastExecEpoch={}", name, disabled, epoch);

                        // 🚫 Disabled triggers
                        if (disabled) {
                            issues.add(name + " (🚫 Disabled trigger)");
                            runContext.logger().warn("⚠️ Disabled: {}", name);
                            continue;
                        }

                        // ⏰ Stuck detection
                        if (epoch > 0) {
                            Instant lastExec = Instant.ofEpochSecond((long) epoch);
                            long minutesLate = Duration.between(lastExec, now).toMinutes();

                            if (minutesLate >= threshold) {
                                issues.add(name + " (⏰ Stuck: last execution was " + minutesLate + " min ago)");
                                runContext.logger().warn("⚠️ Stuck: {} ({} minutes late)", name, minutesLate);
                            } else {
                                runContext.logger().info("✅ {} OK | last execution {} min ago", name, minutesLate);
                            }
                        } else {
                            runContext.logger().warn("⚠️ Missing last execution timestamp for {}", name);
                        }

                    } catch (Exception e) {
                        runContext.logger().error("Error evaluating trigger node: {}", e.getMessage(), e);
                    }
                }

                more = returned == size;
                page++;
            }
        } catch (Exception e) {
            runContext.logger().error("❌ Error while checking triggers via Trigger API: {}", e.getMessage(), e);
        }

        runContext.logger().info("✅ Detection complete: {} issues found", issues.size());
        if (!issues.isEmpty()) {
            runContext.logger().warn("⚠️ Problematic triggers:\n - {}", String.join("\n - ", issues));
        }

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
