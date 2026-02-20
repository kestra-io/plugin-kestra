package io.kestra.plugin.kestra.logs;

import static org.assertj.core.api.Assertions.assertThat;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.model.Execution;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.ArrayList;

@KestraTest
public class FetchTest extends AbstractKestraOssContainerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.logs.fetch";

    @Test
    public void fetch() throws Exception {
        RunContext runContext = runContextFactory.of();

        String flowYaml = """
            id: get-log
            namespace: %s

            tasks:
              - id: log-1
                type: io.kestra.core.tasks.log.Log
                message: "Log number 1"
                level: INFO
              - id: log-2
                type: io.kestra.core.tasks.log.Log
                message: "Log number 2"
                level: INFO
              - id: log-3
                type: io.kestra.core.tasks.log.Log
                message: "Log number 3"
                level: INFO
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log", NAMESPACE);

        Thread.sleep(2000);

        Execution execution = queryExecution("get-log");

        Fetch fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .level(Property.ofValue(Level.INFO))
            .executionId(Property.ofValue(execution.getId()))
            .build();

        Fetch.Output output = fetchTask.run(runContext);

        assertThat(output.getSize()).isEqualTo(3);
    }

    @Test
    public void fetchWithTaskId() throws Exception {
        RunContext runContext = runContextFactory.of();

        String flowYaml = """
            id: get-log-taskid
            namespace: %s

            tasks:
              - id: specific-task
                type: io.kestra.core.tasks.log.Log
                message: "Target Log"
                level: INFO
              - id: other-task
                type: io.kestra.core.tasks.log.Log
                message: "Ignored Log"
                level: INFO
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log-taskid", NAMESPACE);

        Thread.sleep(2000);

        Execution execution = queryExecution("get-log-taskid");

        Fetch fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .level(Property.ofValue(Level.INFO))
            .executionId(Property.ofValue(execution.getId()))
            .tasksId(Property.ofValue(java.util.List.of("specific-task")))
            .build();

        Fetch.Output output = fetchTask.run(runContext);

        assertThat(output.getSize()).isEqualTo(1);
    }

    @Test
    public void fetchWithExecutionId() throws Exception {
        RunContext runContext = runContextFactory.of();

        String flowYaml = """
            id: get-log-executionid
            namespace: %s

            tasks:
              - id: log-1
                type: io.kestra.core.tasks.log.Log
                message: "Log 1"
                level: INFO
              - id: log-2
                type: io.kestra.core.tasks.log.Log
                message: "Log 2"
                level: INFO
              - id: log-3
                type: io.kestra.core.tasks.log.Log
                message: "Log 3"
                level: INFO
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log-executionid", NAMESPACE);

        Thread.sleep(2000);

        Execution execution = queryExecution("get-log-executionid");

        Fetch fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .level(Property.ofValue(Level.INFO))
            .executionId(Property.ofValue(execution.getId()))
            .build();

        Fetch.Output output = fetchTask.run(runContext);

        assertThat(output.getSize()).isEqualTo(3);
    }

    private Execution queryExecution(String flowId) throws Exception {
        RunContext runContext = runContextFactory.of();

        Query searchTask = Query.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build())
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .flowId(Property.ofValue(flowId))
            .size(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        FetchOutput output = searchTask.run(runContext);

        assertThat(output.getRows()).isNotEmpty();

        var row = output.getRows().getFirst();
        if (row instanceof ArrayList<?> arrayList) {
            return (Execution) arrayList.getFirst();
        }

        throw new RuntimeException("Could not extract execution from query result");
    }
}
