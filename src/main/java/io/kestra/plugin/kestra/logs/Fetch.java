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
    title = "Fetch execution logs and store them in a file.",
    description = "This task is useful to automate moving logs between various systems and environments."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "level: INFO",
                "executionId: \"{{ trigger.executionId }}\""
            }
        ),
        @Example(
            code = {
                "level: WARN",
                "executionId: \"{{ execution.id }}\"",
                "tasksId: ",
                "  - \"previous_task_id\""
            }
        )
    },
    aliases = "io.kestra.core.tasks.log.Fetch"
)
public class Fetch extends AbstractKestraTask implements RunnableTask<Fetch.Output> {
    @Schema(title = "Filter for a specific namespace in case `executionId` is set.")
    private Property<String> namespace;

    @Schema(title = "Filter for a specific flow identifier in case `executionId` is set.")
    private Property<String> flowId;

    @Schema(
        title = "Filter for a specific execution.",
        description = """
            If not set, the task will use the ID of the current execution.
            If set, it will try to locate the execution on the current flow unless the `namespace` and `flowId` properties are set."""
    )
    private Property<String> executionId;

    @Schema(title = "Filter for one or more task(s).")
    private Property<List<String>> tasksId;

    @Schema(title = "The lowest log level that you want to fetch")
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

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (OutputStream output = new FileOutputStream(tempFile)) {
            io.kestra.sdk.model.Level sdkLogLevel = io.kestra.sdk.model.Level.fromValue(
                runContext.render(this.level).as(Level.class).orElse(Level.INFO).name()
            );
            
            List<String> taskIds = runContext.render(this.tasksId).asList(String.class);
            
            if (taskIds != null && !taskIds.isEmpty()) {
                for (String taskId : taskIds) {
                    var logs = kestraClient.logs().listLogsFromExecution(
                        executionInfo.id(),
                        executionInfo.tenantId(),
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
                    executionInfo.tenantId(),
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
            description = "Stored as Amazon ION file in a row per row format."
        )
        private URI uri;
    }
}
