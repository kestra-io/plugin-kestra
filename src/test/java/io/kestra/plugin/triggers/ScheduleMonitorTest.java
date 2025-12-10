package io.kestra.plugin.triggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.triggers.ScheduleMonitor;
import io.kestra.sdk.model.FlowWithSource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class ScheduleMonitorTest extends AbstractKestraOssContainerTest {

    @Inject RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.schedule.monitor";

    @Test
    public void shouldDetectDisabledScheduleOnlyWhenIncluded() throws Exception {
        FlowWithSource flow = kestraTestDataUtils.createFlowWithSchedule(
                    NAMESPACE,
            "myflow",
            "*/5 * * * *",
            true
        );

        ScheduleMonitor monitor1 = ScheduleMonitor.builder()
            .id(ScheduleMonitorTest.class.getSimpleName() + IdUtils.create())
            .type(ScheduleMonitorTest.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .includeDisabled(Property.ofValue(false))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, monitor1);
        Optional<Execution> execution = monitor1.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(false));

        ScheduleMonitor monitor2 = ScheduleMonitor.builder()
            .id(ScheduleMonitorTest.class.getSimpleName() + IdUtils.create())
            .type(ScheduleMonitorTest.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .includeDisabled(Property.ofValue(true))
            .build();

        context = TestsUtils.mockTrigger(runContextFactory, monitor2);
        execution = monitor2.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
    }

    private AbstractKestraTask.Auth basicAuth() {
        return AbstractKestraTask.Auth.builder()
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .build();
    }
}

