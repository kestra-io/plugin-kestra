package io.kestra.plugin.kestra.triggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.sdk.KestraClient;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.core.models.property.Property;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class DetectStuckSchedulesTest extends AbstractKestraContainerTest {

    static {
        System.setProperty("KESRA_IMAGE", "ghcr.io/kestra-io/kestra:develop");
    }

    protected String kestraImage = "ghcr.io/kestra-io/kestra:develop";

    @Inject
    private RunContextFactory runContextFactory;

    private KestraClient buildClient() {
        return KestraClient.builder()
            .url(KESTRA_URL)
            .basicAuth(USERNAME, PASSWORD)
            .build();
    }

    @Test
    void shouldDetectDisabledAndStuckTriggers() throws Exception {
        RunContext runContext = runContextFactory.of();
        KestraClient client = buildClient();

        String namespace = "company.team.detect." + UUID.randomUUID().toString().substring(0, 6);

        // 1️⃣ Enabled trigger
        client.flows().createFlow(TENANT_ID, """
            id: flow_enabled
            namespace: %s
            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Enabled trigger test"
            triggers:
              - id: schedule_enabled
                type: io.kestra.plugin.core.trigger.Schedule
                cron: "* * * * *"
                disabled: false
        """.formatted(namespace));

        // 2️⃣ Disabled trigger
        client.flows().createFlow(TENANT_ID, """
            id: flow_disabled
            namespace: %s
            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Disabled trigger test"
            triggers:
              - id: schedule_disabled
                type: io.kestra.plugin.core.trigger.Schedule
                cron: "* * * * *"
                disabled: true
        """.formatted(namespace));

        // 3️⃣ Stuck trigger
        client.flows().createFlow(TENANT_ID, """
            id: flow_stuck
            namespace: %s
            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Stuck trigger test"
            triggers:
              - id: schedule_stuck
                type: io.kestra.plugin.core.trigger.Schedule
                cron: "* * * * *"
                disabled: false
        """.formatted(namespace));

        Thread.sleep(3000); // wait a little

        DetectStuckOrDisabledSchedules task = DetectStuckOrDisabledSchedules.builder()
            .namespace(Property.ofValue(namespace))
            .threshold(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        DetectStuckOrDisabledSchedules.Output output = task.run(runContext);

        assertThat(output, notNullValue());

        // Convert TriggerId -> String
        List<String> disabledIds = output.getDisabledTriggers().stream()
            .map(DetectStuckOrDisabledSchedules.TriggerId::getTriggerId)
            .toList();
        List<String> stuckIds = output.getStuckTriggers().stream()
            .map(DetectStuckOrDisabledSchedules.TriggerId::getTriggerId)
            .toList();

        assertThat(disabledIds, hasItem("schedule_disabled"));
        assertThat(stuckIds, hasItem("schedule_stuck"));
        assertThat(output.getTotalFound(), equalTo(2));
    }
}
