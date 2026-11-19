package io.kestra.plugin.kestra.logs;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Await;
import io.kestra.plugin.kestra.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.model.ApiLightExecution;
import io.kestra.sdk.model.LogEntry;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

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
                type: io.kestra.plugin.core.log.Log
                message: "Log number 1"
                level: INFO
              - id: log-2
                type: io.kestra.plugin.core.log.Log
                message: "Log number 2"
                level: INFO
              - id: log-3
                type: io.kestra.plugin.core.log.Log
                message: "Log number 3"
                level: INFO
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log", NAMESPACE);

        Thread.sleep(2000);

        var execution = queryExecution("get-log");

        Fetch fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
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
                type: io.kestra.plugin.core.log.Log
                message: "Target Log"
                level: INFO
              - id: other-task
                type: io.kestra.plugin.core.log.Log
                message: "Ignored Log"
                level: INFO
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log-taskid", NAMESPACE);

        Thread.sleep(2000);

        var execution = queryExecution("get-log-taskid");

        Fetch fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .level(Property.ofValue(Level.INFO))
            .executionId(Property.ofValue(execution.getId()))
            .tasksId(Property.ofValue(java.util.List.of("specific-task")))
            .build();

        Fetch.Output output = fetchTask.run(runContext);

        assertThat(output.getSize()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void fetchWithExecutionId() throws Exception {
        RunContext runContext = runContextFactory.of();

        String flowYaml = """
            id: get-log-executionid
            namespace: %s

            tasks:
              - id: log-1
                type: io.kestra.plugin.core.log.Log
                message: "Log 1"
                level: INFO
              - id: log-2
                type: io.kestra.plugin.core.log.Log
                message: "Log 2"
                level: INFO
              - id: log-3
                type: io.kestra.plugin.core.log.Log
                message: "Log 3"
                level: INFO
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log-executionid", NAMESPACE);

        Thread.sleep(2000);

        var execution = queryExecution("get-log-executionid");

        Fetch fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .level(Property.ofValue(Level.INFO))
            .executionId(Property.ofValue(execution.getId()))
            .build();

        Fetch.Output output = fetchTask.run(runContext);

        assertThat(output.getSize()).isEqualTo(3);
    }

    @Test
    public void fetchWithLevel() throws Exception {
        String flowYaml = """
            id: get-log-level
            namespace: %s

            tasks:
              - id: debug-log
                type: io.kestra.plugin.core.log.Log
                message: "Debug log"
                level: DEBUG
              - id: info-log
                type: io.kestra.plugin.core.log.Log
                message: "Info log"
                level: INFO
              - id: warn-log
                type: io.kestra.plugin.core.log.Log
                message: "Warn log"
                level: WARN
              - id: error-log
                type: io.kestra.plugin.core.log.Log
                message: "Error log"
                level: ERROR
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log-level", NAMESPACE);

        String executionId = awaitExecutionId("get-log-level");
        awaitLogs(executionId, "Debug log", "Info log", "Warn log", "Error log");

        // level defaults to INFO
        assertThat(fetchMessages(executionId, null, null))
            .containsExactlyInAnyOrder("Info log", "Warn log", "Error log");
        assertThat(fetchMessages(executionId, Level.WARN, null))
            .containsExactlyInAnyOrder("Warn log", "Error log");
        assertThat(fetchMessages(executionId, Level.ERROR, null))
            .containsExactly("Error log");
        // DEBUG may also return Kestra's own debug lines, so only check that nothing above it is dropped
        assertThat(fetchMessages(executionId, Level.DEBUG, null))
            .contains("Debug log", "Info log", "Warn log", "Error log");
    }

    @Test
    public void fetchWithLevelAndTaskIds() throws Exception {
        String flowYaml = """
            id: get-log-level-taskids
            namespace: %s

            tasks:
              - id: info-log
                type: io.kestra.plugin.core.log.Log
                message: "Info log"
                level: INFO
              - id: warn-log
                type: io.kestra.plugin.core.log.Log
                message: "Warn log"
                level: WARN
              - id: error-log
                type: io.kestra.plugin.core.log.Log
                message: "Error log"
                level: ERROR
            """.formatted(NAMESPACE);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flowYaml);

        kestraTestDataUtils.createRandomizedExecution("get-log-level-taskids", NAMESPACE);

        String executionId = awaitExecutionId("get-log-level-taskids");
        awaitLogs(executionId, "Info log", "Warn log", "Error log");

        assertThat(fetchMessages(executionId, Level.WARN, List.of("warn-log")))
            .containsExactly("Warn log");
        // the task only logs below the requested level
        assertThat(fetchMessages(executionId, Level.WARN, List.of("info-log")))
            .isEmpty();
        assertThat(fetchMessages(executionId, Level.INFO, List.of("info-log", "error-log")))
            .containsExactlyInAnyOrder("Info log", "Error log");
        assertThat(fetchMessages(executionId, Level.WARN, List.of("info-log", "error-log")))
            .containsExactly("Error log");
        assertThat(fetchMessages(executionId, Level.INFO, List.of("unknown-task")))
            .isEmpty();
    }

    private String awaitExecutionId(String flowId) throws Exception {
        return Await.until(
            () ->
            {
                try {
                    return queryExecution(flowId).getId();
                } catch (Exception | AssertionError e) {
                    return null;
                }
            },
            Duration.ofMillis(200),
            Duration.ofSeconds(30)
        );
    }

    private void awaitLogs(String executionId, String... messages) throws Exception {
        Await.until(
            () ->
            {
                try {
                    return kestraTestDataUtils.getKestraClient().logs()
                        .listLogsFromExecution(executionId, TENANT_ID, List.of())
                        .stream()
                        .map(LogEntry::getMessage)
                        .toList()
                        .containsAll(List.of(messages));
                } catch (Exception e) {
                    return false;
                }
            },
            Duration.ofMillis(200),
            Duration.ofSeconds(30)
        );
    }

    private List<String> fetchMessages(String executionId, Level level, List<String> tasksId) throws Exception {
        RunContext runContext = runContextFactory.of();

        var fetchTask = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .executionId(Property.ofValue(executionId));
        if (level != null) {
            fetchTask.level(Property.ofValue(level));
        }
        if (tasksId != null) {
            fetchTask.tasksId(Property.ofValue(tasksId));
        }

        Fetch.Output output = fetchTask.build().run(runContext);

        List<String> messages;
        try (InputStream is = runContext.storage().getFile(output.getUri())) {
            messages = FileSerde.readAll(is)
                .map(row -> (String) ((Map<?, ?>) row).get("message"))
                .collectList()
                .block();
        }

        assertThat(output.getSize()).isEqualTo(messages.size());
        return messages;
    }

    private ApiLightExecution queryExecution(String flowId) throws Exception {
        RunContext runContext = runContextFactory.of();

        Query searchTask = Query.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
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
            return (ApiLightExecution) arrayList.getFirst();
        }

        throw new RuntimeException("Could not extract execution from query result");
    }
}
