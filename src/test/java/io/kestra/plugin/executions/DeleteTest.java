package io.kestra.plugin.executions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Delete;
import io.kestra.sdk.model.FlowWithSource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@KestraTest
public class DeleteTest extends AbstractKestraContainerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.executions.delete";

    @Test
    public void shouldDeleteCurrentExecution() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow for testing
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Delete deleteTask = Delete.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .deleteLogs(Property.ofValue(true))
            .deleteMetrics(Property.ofValue(true))
            .deleteStorage(Property.ofValue(true))
            .build();

        VoidOutput output = deleteTask.run(runContext);

        assertThat(output, is(nullValue()));
    }

    @Test
    public void shouldDeleteExecutionWithPropagation() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow for testing
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Delete deleteTask = Delete.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .deleteLogs(Property.ofValue(true))
            .deleteMetrics(Property.ofValue(true))
            .deleteStorage(Property.ofValue(true))
            .build();

        VoidOutput output = deleteTask.run(runContext);

        assertThat(output, is(nullValue()));
    }

    @Test
    public void shouldDeleteSpecificExecution() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow for testing
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Delete deleteTask = Delete.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue("test-execution-id"))
            .deleteLogs(Property.ofValue(true))
            .deleteMetrics(Property.ofValue(true))
            .deleteStorage(Property.ofValue(true))
            .build();

        VoidOutput output = deleteTask.run(runContext);

        assertThat(output, is(nullValue()));
    }
}
