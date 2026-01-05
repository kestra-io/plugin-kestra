package io.kestra.plugin.executions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.plugin.kestra.executions.Resume;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.StateType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Map;

@KestraTest
public class ResumeTest extends AbstractKestraOssContainerTest {
    @Inject protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.executions.resume";

    @Test
    public void shouldResumePausedExecution() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create a flow that pauses automatically
        FlowWithSource flow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);

        // 2. Trigger an execution
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

        // 3. Wait for it to reach PAUSED state
        Thread.sleep(1000);
        Execution pausedExecution = queryExecution(flow.getId());
        assertThat(pausedExecution.getState().getCurrent(), is(StateType.PAUSED));

        // 4. Run RESUME Task
        Resume resumeTask = createResumeTask(pausedExecution.getId(), null);
        resumeTask.run(runContext);

        // 5. Verify State Changed
        Thread.sleep(1000);
        Execution resumedExecution = queryExecution(flow.getId());
        assertThat(resumedExecution.getState().getCurrent(), not(StateType.PAUSED));
    }

    @Test
    public void shouldResumeWithInputs() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create a flow that pauses
        FlowWithSource flow = kestraTestDataUtils.createRandomizedPauseFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

        Thread.sleep(1000);
        Execution pausedExecution = queryExecution(flow.getId());

        // 2. Resume with INPUTS
        Map<String, Object> inputs = Map.of("comment", "Approved by Unit Test", "status", "OK");

        Resume resumeTask = createResumeTask(pausedExecution.getId(), inputs);
        resumeTask.run(runContext);

        Thread.sleep(1000);
        Execution resumedExecution = queryExecution(flow.getId());

        // 3. Verify execution is running/success
        assertThat(resumedExecution.getState().getCurrent(), not(StateType.PAUSED));
    }

    @Test
    public void shouldFailToResumeFinishedExecution() throws Exception {
        RunContext runContext = runContextFactory.of();
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

        Thread.sleep(1000);
        Execution finishedExecution = queryExecution(flow.getId());
        assertThat(finishedExecution.getState().getCurrent(), is(StateType.SUCCESS));

        Resume resumeTask = createResumeTask(finishedExecution.getId(), null);

        assertThrows(Exception.class, () -> resumeTask.run(runContext));
    }

    @Test
    public void shouldFailToResumeNonExistentExecution() {
        RunContext runContext = runContextFactory.of();
        Resume resumeTask = createResumeTask("fake-execution-id-12345", null);

        assertThrows(Exception.class, () -> resumeTask.run(runContext));
    }

    // --- Helpers ---

    private Resume createResumeTask(String executionId, Map<String, Object> inputs) {
        var builder = Resume.builder()
            .kestraUrl(Property.of(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.of(USERNAME))
                .password(Property.of(PASSWORD))
                .build())
            .tenantId(Property.of(TENANT_ID))
            .executionId(Property.of(executionId));

        if (inputs != null) {
            builder.inputs(Property.of(inputs));
        }

        return builder.build();
    }

    private Execution queryExecution(String flowId) throws Exception {
        RunContext runContext = runContextFactory.of();
        Query searchTask = Query.builder()
            .kestraUrl(Property.of(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.of(USERNAME))
                .password(Property.of(PASSWORD))
                .build())
            .tenantId(Property.of(TENANT_ID))
            .namespace(Property.of(NAMESPACE))
            .flowId(Property.of(flowId))
            .size(Property.of(1))
            .fetchType(Property.of(io.kestra.core.models.tasks.common.FetchType.FETCH))
            .build();

        FetchOutput output = searchTask.run(runContext);

        if (output.getRows().isEmpty()) {
            throw new RuntimeException("No execution found for flow " + flowId);
        }

        var row = output.getRows().getFirst();
        if (row instanceof ArrayList<?> arrayList) {
            return (Execution) arrayList.getFirst();
        } else if (row instanceof Execution execution) {
            return execution;
        }

        throw new RuntimeException("Unexpected execution row format");
    }
}