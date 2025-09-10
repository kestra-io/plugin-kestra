package io.kestra.plugin.executions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Delete;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.StateType;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

@KestraTest
public class DeleteTest extends AbstractKestraContainerTest {
  @Inject protected RunContextFactory runContextFactory;

  protected static final String NAMESPACE = "kestra.tests.executions.delete";

  @Test
  public void shouldProvideDeleteExecutionId() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create a flow for testing
    FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

    // Query an execution for a given flow
    Execution execution = queryExecution(flow.getId());
    assertThat(execution.getId(), is(notNullValue()));

    // Delete the execution
    Delete deleteTask =
        Delete.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .deleteLogs(Property.ofValue(true))
            .deleteMetrics(Property.ofValue(true))
            .deleteStorage(Property.ofValue(true))
            .build();

    // Check the delete result
    NoSuchElementException exception =
        assertThrows(NoSuchElementException.class, () -> deleteTask.run(runContext));

    assertThat(exception.getMessage(), is("No value present"));
  }

  @Test
  public void failedDeleteNoneFinalExecution() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create a flow for testing
    FlowWithSource flow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

    // Query an execution for a given flow
    Thread.sleep(500);
    Execution execution = queryExecution(flow.getId());
    assertThat(execution.getId(), is(notNullValue()));

    // Delete the execution
    Delete deleteTask =
        Delete.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(execution.getId()))
            .deleteLogs(Property.ofValue(true))
            .deleteMetrics(Property.ofValue(true))
            .deleteStorage(Property.ofValue(true))
            .build();

    // Check the delete result
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> deleteTask.run(runContext));

    assertThat(
        exception.getMessage(),
        is(
            "Execution "
                + execution.getId()
                + " is not in a terminate state ("
                + execution.getState().getCurrent()
                + ")"));
  }

  @Test
  public void shouldDeleteExecution() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create a flow for testing
    FlowWithSource flow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

    // Query an execution for a given flow
    Execution beforeExecution = queryExecution(flow.getId());
    assertThat(beforeExecution.getState().getCurrent(), is(StateType.RUNNING));

    // Kill the execution
    Thread.sleep(500);
    kestraTestDataUtils.killExecution(beforeExecution.getId(), true);

    // Wait for the execution current state changed from KILLING to KILLED
    Thread.sleep(500);
    Execution afterExecution = queryExecution(flow.getId());
    assertThat(afterExecution.getState().getCurrent(), is(StateType.KILLED));

    // Delete the execution
    Delete deleteTask =
        Delete.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(afterExecution.getId()))
            .deleteLogs(Property.ofValue(true))
            .deleteMetrics(Property.ofValue(true))
            .deleteStorage(Property.ofValue(true))
            .build();

    // Check the delete result
    VoidOutput output = deleteTask.run(runContext);

    assertThat(output, is(nullValue()));
  }

  private Execution queryExecution(String flowId) throws Exception {
    RunContext runContext = runContextFactory.of();
    Query searchTask =
        Query.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .flowId(Property.ofValue(flowId))
            .size(Property.ofValue(10))
            .fetchType(Property.ofValue(io.kestra.core.models.tasks.common.FetchType.FETCH))
            .build();

    FetchOutput output = searchTask.run(runContext);

    assertThat(output.getRows().size(), is(1));

    Execution execution = null;
    var row = output.getRows().getFirst();
    if (row instanceof ArrayList<?> arrayList) {
      execution = (Execution) arrayList.getFirst();
    }

    assertThat(execution, is(notNullValue()));
    return execution;
  }
}
