package io.kestra.plugin.executions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Kill;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.StateType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@KestraTest
class KillTest extends AbstractKestraContainerTest {
  @Inject protected RunContextFactory runContextFactory;

  protected static final String NAMESPACE = "kestra.tests.executions.kill";

  @Test
  void shouldKillCurrentExecution() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create a flow for testing
    FlowWithSource flow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

    // Query the execution
    Execution beforeExecution = queryExecution(flow.getId());
    Awaitility.await().until(checkExecutionState(beforeExecution.getId(), StateType.PAUSED));

    Kill killTask =
        Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(beforeExecution.getId()))
            .propagateKill(Property.ofValue(false))
            .build();

    VoidOutput output = killTask.run(runContext);

    assertThat(output, is(nullValue()));

    // Check the execution is killed
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(beforeExecution.getId(), StateType.KILLED));

    Execution afterExecution = kestraTestDataUtils.getExecution(beforeExecution.getId());

    assertThat(
        afterExecution.getState().getCurrent(),
        in(new StateType[] {StateType.RESTARTED, StateType.RUNNING, StateType.KILLED}));
  }

  @Test
  void shouldKillExecutionWithPropagation() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create sub flow for testing
    FlowWithSource subFlow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

    FlowWithSource parentFlow =
        kestraTestDataUtils.createRandomizedSubFlow(NAMESPACE, subFlow.getId());

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(parentFlow.getId(), parentFlow.getNamespace());

    // Query the executions
    Execution parentExecution = queryExecution(parentFlow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(parentExecution.getId(), StateType.RUNNING));
    Execution subExecution = queryExecution(subFlow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(subExecution.getId(), StateType.PAUSED));

    // Kill the parent execution without propagate the kill to sub execution
    Kill killTask =
        Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(parentExecution.getId()))
            .propagateKill(Property.ofValue(true))
            .build();

    VoidOutput output = killTask.run(runContext);

    assertThat(output, is(nullValue()));

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(parentExecution.getId(), StateType.KILLED));
    Execution afterSubExecution = kestraTestDataUtils.getExecution(subExecution.getId());

    assertThat(afterSubExecution.getState().getCurrent(), is(StateType.KILLED));
  }

  @Test
  void shouldKillExecutionWithoutPropagation() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create sub flow for testing
    FlowWithSource subFlow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

    FlowWithSource parentFlow =
        kestraTestDataUtils.createRandomizedSubFlow(NAMESPACE, subFlow.getId());

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(parentFlow.getId(), parentFlow.getNamespace());

    // Query the executions
    Execution parentExecution = queryExecution(parentFlow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(parentExecution.getId(), StateType.RUNNING));
    Execution subExecution = queryExecution(subFlow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(subExecution.getId(), StateType.PAUSED));

    // Kill the parent execution without propagate the kill to sub execution
    Kill killTask =
        Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(parentExecution.getId()))
            .propagateKill(Property.ofValue(false))
            .build();

    VoidOutput output = killTask.run(runContext);

    assertThat(output, is(nullValue()));

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(parentExecution.getId(), StateType.KILLED));

    // With the propagateKill is false, after kill the parent execution the sub execution is still
    // paused and can be resumed to continue the execution.
    kestraTestDataUtils.resumeExecution(subExecution.getId());
    Execution resumeExecution = kestraTestDataUtils.getExecution(subExecution.getId());
    assertThat(
        resumeExecution.getState().getCurrent(),
        in(new StateType[] {StateType.RESTARTED, StateType.RUNNING, StateType.SUCCESS}));
  }

  @Test
  void shouldKillSpecificExecution() throws Exception {
    RunContext runContext = runContextFactory.of();

    // Create sub flow for testing
    FlowWithSource subFlow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

    FlowWithSource parentFlow =
        kestraTestDataUtils.createRandomizedSubFlow(NAMESPACE, subFlow.getId());

    // Create an execution for a given flow
    kestraTestDataUtils.createRandomizedExecution(parentFlow.getId(), parentFlow.getNamespace());

    // Query the executions
    Execution parentExecution = queryExecution(parentFlow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(parentExecution.getId(), StateType.RUNNING));
    Execution subExecution = queryExecution(subFlow.getId());
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(checkExecutionState(subExecution.getId(), StateType.PAUSED));

    // Kill the sub execution
    Kill killTask =
        Kill.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(subExecution.getId()))
            .propagateKill(Property.ofValue(true))
            .build();

    VoidOutput output = killTask.run(runContext);

    assertThat(output, is(nullValue()));

    // After kill the sub execution, the parent execution is killed too.
    Awaitility.await()
        .during(Duration.ofSeconds(2))
        .until(checkExecutionState(subExecution.getId(), StateType.KILLED));
    Execution afterParentExecution = kestraTestDataUtils.getExecution(parentExecution.getId());

    assertThat(afterParentExecution.getState().getCurrent(), is(StateType.KILLED));
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

  private Callable<Boolean> checkExecutionState(String executionId, StateType stateType) {
    return () -> kestraTestDataUtils.getExecution(executionId).getState().getCurrent() == stateType;
  }
}
