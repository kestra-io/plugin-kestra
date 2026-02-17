package io.kestra.plugin.kestra.logs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch execution logs to storage",
    description = "Downloads logs for an execution using optional task filters and minimum level. Defaults to the current execution and level INFO. Stores results as an ION file in internal storage and returns its URI and row count."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fetch_logs_flow
                namespace: company.team

                tasks:
                  - id: my_task
                    type: io.kestra.plugin.scripts.shell.Commands
                    commands:
                      - echo "Processing data"
                      - echo "Task completed"

                  - id: fetch_logs
                    type: io.kestra.plugin.kestra.logs.Fetch
                    level: INFO
                    executionId: "{{ execution.id }}"

                  - id: log_count
                    type: io.kestra.plugin.core.log.Log
                    message: "Fetched {{ outputs.fetch_logs.size }} log entries"
                """
        ),
        @Example(
            code = {
                "level: WARN",
                "executionId: \"{{ execution.id }}\"",
                "tasksId: ",
                "  - \"previous_task_id\""
            }
        )
    }
)
public class Fetch extends AbstractKestraTask implements RunnableTask<Fetch.Output> {
    @Schema(title = "Execution namespace", description = "Used when targeting another execution; defaults to the current flow namespace.")
    private Property<String> namespace;

    @Schema(title = "Execution flow id", description = "Required when fetching a different flow's execution without a fully qualified ID.")
    private Property<String> flowId;

    @Schema(
        title = "Execution id to fetch",
        description = """
            Defaults to the current execution ID. Provide namespace and flowId when referencing an execution from another flow."""
    )
    private Property<String> executionId;

    @Schema(title = "Task ids filter", description = "Limit logs to these task ids; empty list fetches all tasks.")
    private Property<List<String>> tasksId;

    @Schema(title = "Minimum log level", description = "Defaults to INFO; lower levels are excluded.")
    @Builder.Default
    private Property<Level> level = Property.of(Level.INFO);

    @SuppressWarnings("unchecked")
    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);
        var executionInfo = PluginUtilsService.executionFromTaskParameters(
            runContext,
            runContext.render(this.namespace).as(String.class).orElse(null),
            runContext.render(this.flowId).as(String.class).orElse(null),
            runContext.render(this.executionId).as(String.class).orElse(null)
        );

        String targetTenantId = runContext.render(this.tenantId).as(String.class)
            .orElse(runContext.flowInfo().tenantId());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (OutputStream output = new FileOutputStream(tempFile)) {
            io.kestra.sdk.model.Level sdkLogLevel = io.kestra.sdk.model.Level.fromValue(
                runContext.render(this.level).as(Level.class).orElse(Level.INFO).name()
            );

            List<String> taskIds = runContext.render(this.tasksId).asList(String.class);

            if (!taskIds.isEmpty()) {
                for (String taskId : taskIds) {
                    var logs = kestraClient.logs().listLogsFromExecution(
                        executionInfo.id(),
                        targetTenantId,
                        sdkLogLevel,
                        null,
                        taskId,
                        null
                    );

                    if (logs != null) {
                        logs.forEach(throwConsumer(log -> {
                            count.incrementAndGet();
                            FileSerde.write(output, log);
                        }));
                    }
                }
            } else {
                var logs = kestraClient.logs().listLogsFromExecution(
                    executionInfo.id(),
                    targetTenantId,
                    sdkLogLevel,
                    null,
                    null,
                    null
                );

                if (logs != null) {
                    logs.forEach(throwConsumer(log -> {
                        count.incrementAndGet();
                        FileSerde.write(output, log);
                    }));
                }
            }
        }

        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .size(count.get())
            .build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The number of rows fetched")
        private Long size;

        @Schema(
            title = "Internal storage URI of stored results",
            description = "Logs are stored as an Amazon ION file, one record per row."
        )
        private URI uri;
    }
}
