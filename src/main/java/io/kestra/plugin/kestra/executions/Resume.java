package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
        )
    }
)
public class Resume extends AbstractKestraTask implements RunnableTask<Resume.Output> {

    @Schema(title = "The Execution ID to resume")
    private Property<String> executionId;

    @Schema(title = "Map of inputs to send to the paused execution")
    private Property<Map<String, Object>> inputs;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // 1. Render Execution ID
        String rExecutionId = runContext.render(this.executionId).as(String.class).orElseThrow();

        // 2. Resolve Tenant
        // Priority 1: Use the 'tenantId' property explicitly set on the task
        String rTenant = runContext.render(this.tenantId).as(String.class).orElse(null);

        // Priority 2: If not set, fallback to the current context's tenant
        if (rTenant == null) {
            try {
                // We use render() to avoid calling the deprecated runContext.tenantId() method.
                // We wrap this in try/catch because in some test environments,
                // the 'tenantId' variable might not exist, causing an exception.
                rTenant = runContext.render("{{ tenantId }}");
            } catch (Exception e) {
                // If the variable is missing, we safely default to null
                rTenant = null;
            }
        }

        // 3. Get Client
        var client = this.kestraClient(runContext);

        // 4. Call the API
        client.executions().resumeExecution(rExecutionId, rTenant);

        return Output.builder()
            .executionId(rExecutionId)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The execution ID that was resumed")
        private String executionId;
    }
}