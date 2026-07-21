package io.kestra.plugin.kestra.triggers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kestra.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.ApiTriggerState;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ToggleTest extends AbstractKestraOssContainerTest {

    @Inject
    RunContextFactory runContextFactory;

    private static final String NAMESPACE = "kestra.tests.trigger.toggle";
    private static final String FLOW_ID = "toggle-flow";
    private static final String TRIGGER_ID = "schedule";
    // Scheduler registers the trigger state row at the next minute boundary; allow up to 5 min
    // so registration survives a scheduler backlog on the shared container.
    private static final Duration INITIAL_AWAIT_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration AWAIT_TIMEOUT = Duration.ofMinutes(2);

    private final List<String> createdFlowIds = new ArrayList<>();

    @AfterEach
    void cleanupFlows() {
        createdFlowIds.forEach(flowId -> kestraTestDataUtils.deleteFlow(NAMESPACE, flowId));
        createdFlowIds.clear();
    }

    @Test
    void shouldDisableAndEnableTrigger() throws Exception {
        var runContext = runContextFactory.of();
        var flowId = FLOW_ID + "-" + IdUtils.create();

        kestraTestDataUtils.createFlowWithSchedule(
            NAMESPACE,
            flowId,
            "* * * * *",
            false
        );
        createdFlowIds.add(flowId);

        // Toggle's disabledTriggersByQuery only updates trigger-state rows that already exist, so
        // wait for the scheduler to register the trigger before toggling it.
        Await.until(
            () -> findTrigger(flowId),
            Duration.ofSeconds(1),
            INITIAL_AWAIT_TIMEOUT
        );

        Toggle disable = Toggle.builder()
            .id("disable-" + IdUtils.create())
            .type(Toggle.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .flowId(Property.ofValue(flowId))
            .trigger(Property.ofValue(TRIGGER_ID))
            .enabled(Property.ofValue(false))
            .build();

        disable.run(runContext);

        var disabledTrigger = Await.until(
            () ->
            {
                var trigger = findTrigger(flowId);
                return Boolean.TRUE.equals(trigger != null ? trigger.getDisabled() : null) ? trigger : null;
            },
            Duration.ofSeconds(1),
            AWAIT_TIMEOUT
        );

        assertThat(disabledTrigger.getDisabled(), is(true));

        Toggle enable = Toggle.builder()
            .id("enable-" + IdUtils.create())
            .type(Toggle.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .flowId(Property.ofValue(flowId))
            .trigger(Property.ofValue(TRIGGER_ID))
            .enabled(Property.ofValue(true))
            .build();

        enable.run(runContext);

        var trigger = Await.until(
            () ->
            {
                var currentTrigger = findTrigger(flowId);
                return Boolean.FALSE.equals(currentTrigger != null ? currentTrigger.getDisabled() : null) ? currentTrigger : null;
            },
            Duration.ofSeconds(1),
            AWAIT_TIMEOUT
        );

        assertThat(trigger.getDisabled(), is(false));
    }

    private AbstractKestraTask.Auth basicAuth() {
        return AbstractKestraTask.Auth.builder()
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .build();
    }

    private ApiTriggerState findTrigger(String flowId) {
        return kestraTestDataUtils.getKestraClient()
            .triggers()
            .searchTriggersForFlow(TENANT_ID, NAMESPACE, flowId, 1, 1000, null, null)
            .getResults()
            .stream()
            .filter(trigger -> TRIGGER_ID.equals(trigger.getTriggerId()))
            .findFirst()
            .orElse(null);
    }
}
