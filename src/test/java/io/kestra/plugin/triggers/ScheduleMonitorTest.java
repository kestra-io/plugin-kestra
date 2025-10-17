package io.kestra.plugin.triggers;

import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.plugin.kestra.triggers.ScheduleMonitor;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ScheduleMonitorTest {

    @Test
    void testEvaluateReturnsData() throws Exception {
        // Arrange
        ScheduleMonitor trigger = new ScheduleMonitor();
        TriggerContext context = TriggerContext.builder()
            .namespace("company.team")
            .flowId("daily_sync")
            .build();

        // Act
        Optional<Map<String, Object>> result = trigger.evaluate(context);

        // Assert
        assertTrue(result.isPresent(), "Expected trigger to return data");
        assertNotNull(result.get().get("data"));
        System.out.println("✅ Trigger output: " + result.get());
    }
}
