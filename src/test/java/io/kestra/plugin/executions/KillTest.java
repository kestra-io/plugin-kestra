package io.kestra.plugin.executions;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Kill;
import io.kestra.sdk.model.FlowWithSource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class KillTest extends AbstractKestraContainerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.executions.kill";

    @Test
    public void shouldKillCurrentExecution() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow for testing
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Kill killTask = Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .propagateKill(Property.ofValue(false))
            .build();

        VoidOutput output = killTask.run(runContext);

        assertThat(output, is(nullValue()));
    }

    @Test
    public void shouldKillExecutionWithPropagation() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow for testing
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Kill killTask = Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .propagateKill(Property.ofValue(true))
            .build();

        VoidOutput output = killTask.run(runContext);

        assertThat(output, is(nullValue()));
    }

    @Test
    public void shouldKillSpecificExecution() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow for testing
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Kill killTask = Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue("test-execution-id"))
            .propagateKill(Property.ofValue(false))
            .build();

        VoidOutput output = killTask.run(runContext);

        assertThat(output, is(nullValue()));
    }
}
