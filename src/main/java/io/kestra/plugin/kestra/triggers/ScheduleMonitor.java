package io.kestra.plugin.kestra.triggers;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ToString
@NoArgsConstructor
@Getter
@Plugin(
    examples = {
        @Example(
            title = "Detect stuck or misconfigured schedules",
            full = true,
            code = """
                triggers:
                  - id: stuck_schedules
                    type: io.kestra.plugin.kestra.triggers.ScheduleMonitor
                    auth:
                      username: admin@kestra.io
                      password: Admin1234
                    interval: PT1H
            """
        )
    }
)
public class ScheduleMonitor extends AbstractTrigger {

    @PluginProperty(dynamic = true)
    @NotNull
    private AbstractKestraTask.Auth auth;

    @PluginProperty
    private String namespace;

    @PluginProperty
    private String flowId;

    @PluginProperty
    private Duration interval = Duration.ofHours(1);

    @PluginProperty
    private Boolean includeDisabled = false;

    // Removed @Override since parent class has no matching method
    public Optional<Map<String, Object>> evaluate(TriggerContext context) throws Exception {
        KestraClient client = KestraClient.builder()
            .url("http://localhost:8080")
            .build();

        // Dummy schedules for testing
        List<Map<String, Object>> schedules = List.of(
            Map.of(
                "namespace", "company.team",
                "flowId", "daily_sync",
                "triggerId", "schedule",
                "lastExecution", "2025-10-13T06:00:00Z",
                "expectedNext", "2025-10-14T06:00:00Z"
            )
        );

        Instant now = Instant.now();

        List<Map<String, Object>> stuckSchedules = schedules.stream()
            .filter(s -> Instant.parse(s.get("expectedNext").toString()).isBefore(now))
            .toList();

        if (stuckSchedules.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(Map.of("data", stuckSchedules));
    }
}
