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
import io.kestra.core.utils.Await;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Delete;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.StateType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@KestraTest
public class DeleteTest extends AbstractKestraOssContainerTest {
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

    // Query an execution for a given flow and wait for it to reach PAUSED
    Execution beforeExecution = queryExecution(flow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(checkExecutionState(beforeExecution.getId(), StateType.PAUSED));

    // Kill the execution
    kestraTestDataUtils.killExecution(beforeExecution.getId(), true);

    // Wait for the execution current state changed from KILLING to KILLED
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(checkExecutionState(beforeExecution.getId(), StateType.KILLED));
    Execution afterExecution = kestraTestDataUtils.getExecution(beforeExecution.getId());
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

    return Await.until(
        () -> {
          try {
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
            if (output.getRows().isEmpty()) {
              return null;
            }

            Execution execution = null;
            var row = output.getRows().getFirst();
            if (row instanceof ArrayList<?> arrayList && !arrayList.isEmpty()) {
              execution = (Execution) arrayList.getFirst();
            }

            return execution;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        Duration.ofMillis(200),
        Duration.ofSeconds(5));
  }

  private Callable<Boolean> checkExecutionState(String executionId, StateType stateType) {
    return () -> kestraTestDataUtils.getExecution(executionId).getState().getCurrent() == stateType;
  }
}
