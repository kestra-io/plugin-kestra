package io.kestra.plugin.triggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.triggers.ScheduleMonitor;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class ScheduleMonitorTest extends AbstractKestraOssContainerTest {

    @Inject RunContextFactory runContextFactory;

    private static final String NAMESPACE_PREFIX = "kestra.tests.schedule.monitor";
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(60);

    @Test
    public void shouldDetectDisabledScheduleOnlyWhenIncluded() throws Exception {
        var namespace = randomNamespace();
        var flowId = "myflow-" + IdUtils.create();

        kestraTestDataUtils.createFlowWithSchedule(
            namespace,
            flowId,
            "*/5 * * * *",
            true
        );

        Await.until(
            () -> findTrigger(namespace, flowId),
            Duration.ofMillis(100),
            AWAIT_TIMEOUT
        );

        ScheduleMonitor monitor1 = ScheduleMonitor.builder()
            .id(ScheduleMonitorTest.class.getSimpleName() + IdUtils.create())
            .type(ScheduleMonitorTest.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace))
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
            .namespace(Property.ofValue(namespace))
            .includeDisabled(Property.ofValue(true))
            .build();

        var context2 = TestsUtils.mockTrigger(runContextFactory, monitor2);
        execution = Optional.ofNullable(Await.until(
            () -> evaluate(monitor2, context2.getKey(), context2.getValue()).orElse(null),
            Duration.ofMillis(100),
            AWAIT_TIMEOUT
        ));

        assertThat(execution.isPresent(), is(true));
    }

    @Test
    public void shouldDetectNoExecutionWithinInterval() throws Exception {
        var namespace = randomNamespace();
        var flowId = "intervalflow-" + IdUtils.create();

        kestraTestDataUtils.createFlowWithSchedule(
            namespace,
            flowId,
            "*/5 * * * *",
            false
        );

        Await.until(
            () -> {
                var trigger = findTrigger(namespace, flowId);
                return trigger != null && trigger.getDate() != null ? trigger : null;
            },
            Duration.ofMillis(100),
            AWAIT_TIMEOUT
        );

        ScheduleMonitor monitor = ScheduleMonitor.builder()
            .id(ScheduleMonitorTest.class.getSimpleName() + IdUtils.create())
            .type(ScheduleMonitorTest.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace))
            .maxExecutionInterval(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, monitor);

        var execution = Await.until(
            () -> evaluate(monitor, context.getKey(), context.getValue()).orElse(null),
            Duration.ofMillis(100),
            AWAIT_TIMEOUT
        );

        assertThat(execution, is(notNullValue()));
    }

    @Test
    public void shouldDetectDisabledSchedulesAcrossPages() throws Exception {
        var namespace = randomNamespace();
        var flowCount = 101;

        for (var i = 0; i < flowCount; i++) {
            kestraTestDataUtils.createFlowWithSchedule(
                namespace,
                "pageflow-" + i + "-" + IdUtils.create(),
                "*/5 * * * *",
                true
            );
        }

        Await.until(
            () -> {
                var response = kestraTestDataUtils.getKestraClient()
                    .triggers()
                    .searchTriggers(
                        1,
                        1,
                        TENANT_ID,
                        null,
                        List.of(
                            new QueryFilter()
                                .field(QueryFilterField.NAMESPACE)
                                .operation(QueryFilterOp.STARTS_WITH)
                                .value(namespace)
                        )
                    );
                return response.getTotal() >= flowCount ? response : null;
            },
            Duration.ofMillis(100),
            AWAIT_TIMEOUT
        );

        var monitor = ScheduleMonitor.builder()
            .id(ScheduleMonitorTest.class.getSimpleName() + IdUtils.create())
            .type(ScheduleMonitorTest.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(basicAuth())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(namespace))
            .includeDisabled(Property.ofValue(true))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, monitor);
        var output = runChecks(monitor, context.getKey());

        assertThat(output.getData(), hasSize(flowCount));
    }

    private AbstractKestraTask.Auth basicAuth() {
        return AbstractKestraTask.Auth.builder()
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .build();
    }

    private Optional<Execution> evaluate(
        ScheduleMonitor monitor,
        ConditionContext conditionContext,
        io.kestra.core.models.triggers.Trigger triggerContext
    ) {
        try {
            return monitor.evaluate(conditionContext, triggerContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ScheduleMonitor.Output runChecks(
        ScheduleMonitor monitor,
        ConditionContext conditionContext
    ) {
        try {
            return monitor.runChecks(conditionContext.getRunContext());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private io.kestra.sdk.model.Trigger findTrigger(String namespace, String flowId) {
        return kestraTestDataUtils.getKestraClient()
            .triggers()
            .searchTriggersForFlow(1, 1000, namespace, flowId, TENANT_ID, null, null)
            .getResults()
            .stream()
            .filter(trigger -> "schedule".equals(trigger.getTriggerId()))
            .findFirst()
            .orElse(null);
    }

    private String randomNamespace() {
        return NAMESPACE_PREFIX + "." + IdUtils.create().toLowerCase();
    }
}
