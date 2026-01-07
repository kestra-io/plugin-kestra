package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Resume a paused execution",
    description = "This task uses the Kestra API to resume an execution that is in a PAUSED state."
)
@Plugin(
    examples = {
        @Example(
            title = "Resume an execution",
            code = {
                "executionId: \"{{ trigger.executionId }}\""
            }
        ),
        @Example(
            title = "Resume an execution with inputs",
            code = {
                "executionId: \"{{ trigger.executionId }}\"",
                "inputs:",
                "  comment: \"Approved by automated process\"",
                "  status: \"OK\""
            }
        )
    }
)
public class Resume extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "The Execution ID to resume",
        description = "If not provided, defaults to the current execution ID."
    )
    private Property<String> executionId;

    @Schema(title = "Map of inputs to send to the paused execution")
    private Property<Map<String, Object>> inputs;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String rExecutionId = this.executionId != null ?
            runContext.render(this.executionId).as(String.class).orElse(null) :
            null;

        if (rExecutionId == null) {
            rExecutionId = runContext.render("{{ execution.id }}");
        }

        String rTenant = runContext.render(this.tenantId).as(String.class)
            .orElse(runContext.flowInfo().tenantId());

        Map<String, String> finalInputs = new HashMap<>();
        if (this.inputs != null) {
            Map<String, Object> rawInputs = runContext.render(this.inputs).asMap(String.class, Object.class);
            if (rawInputs != null) {
                for (Map.Entry<String, Object> entry : rawInputs.entrySet()) {
                    if (entry.getValue() != null) {
                        finalInputs.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                }
            }
        }

        runContext.logger().info("Resuming execution {}", rExecutionId);
        this.kestraClient(runContext).executions()
            .resumeExecution(rExecutionId, rTenant, finalInputs);

        runContext.logger().debug("Successfully resumed execution {}", rExecutionId);

        return null;
    }
}