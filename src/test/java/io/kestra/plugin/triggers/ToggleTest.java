package io.kestra.plugin.triggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.triggers.Toggle;
import io.kestra.sdk.model.FlowWithSource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ToggleTest extends AbstractKestraOssContainerTest {

    @Inject
    RunContextFactory runContextFactory;

    private static final String NAMESPACE = "kestra.tests.trigger.toggle";
    private static final String FLOW_ID = "toggle-flow";
    private static final String TRIGGER_ID = "schedule";

    @Test
    void shouldDisableAndEnableTrigger() throws Exception {
        RunContext runContext = runContextFactory.of();

        FlowWithSource flow = kestraTestDataUtils.createFlowWithSchedule(
            NAMESPACE,
            FLOW_ID,
            "* * * * *",
            false
        );

        Toggle disable = Toggle.builder()
            .id("disable-" + IdUtils.create())
            .type(Toggle.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .flowId(Property.ofValue(FLOW_ID))
            .trigger(Property.ofValue(TRIGGER_ID))
            .enabled(Property.ofValue(false))
            .build();

        disable.run(runContext);

        assertThat(kestraTestDataUtils.getKestraClient().triggers().searchTriggersForFlow(1, 1000, NAMESPACE, FLOW_ID, TENANT_ID, null, null).getResults().getFirst().getDisabled(), is(true));

        Toggle enable = Toggle.builder()
            .id("enable-" + IdUtils.create())
            .type(Toggle.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .flowId(Property.ofValue(FLOW_ID))
            .trigger(Property.ofValue(TRIGGER_ID))
            .enabled(Property.ofValue(true))
            .build();

        enable.run(runContext);

        assertThat(kestraTestDataUtils.getKestraClient().triggers().searchTriggersForFlow(1, 1000, NAMESPACE, FLOW_ID, TENANT_ID, null, null).getResults().getFirst().getDisabled(), is(false));
    }

    private AbstractKestraTask.Auth basicAuth() {
        return AbstractKestraTask.Auth.builder()
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .build();
    }
}
