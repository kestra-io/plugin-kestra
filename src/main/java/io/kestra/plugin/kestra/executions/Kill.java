package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Kill an execution",
    description = "This task will kill the current execution and optionally propagate the kill to child executions."
)
@Plugin(
    examples = {
        @Example(
            title = "Kill the current execution with propagation to child executions",
            full = true,
            code = """
                id: conditional-kill-flow
                namespace: company.team

                inputs:
                  - id: shouldKill
                    type: boolean
                    defaults: false

                tasks:
                  - id: subflow
                    type: io.kestra.plugin.core.flow.Subflow
                    flowId: child
                    namespace: demo
                    wait: false
                  - id: kill
                    type: io.kestra.plugin.kestra.executions.Kill
                    runIf: "{{ inputs.shouldKill == true }}"
                    propagateKill: true
                """
        )
    }
)
public class Kill extends AbstractKestraTask implements RunnableTask<Kill.Output> {
    @Schema(title = "Propagate kill to child executions",
        description = "Whether to also kill the child executions (subflows) when this execution is killed."
    )
    @Builder.Default
    private Property<Boolean> propagateKill = Property.ofValue(false);

    @Override
    public Kill.Output run(RunContext runContext) throws Exception {
        boolean shouldPropagateKill = runContext.render(this.propagateKill).as(Boolean.class).orElse(false);
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String executionId = runContext.render("{{ execution.id }}");

        runContext.logger().info("Killing execution {} with propagateKill={}", executionId, shouldPropagateKill);
        KestraClient kestraClient = kestraClient(runContext);

        try {
            kestraClient.executions().killExecution(executionId, shouldPropagateKill, tId);
            runContext.logger().info("Successfully killed execution {}", executionId);

            return Output.builder()
                .executionId(executionId)
                .killed(true)
                .propagateKill(shouldPropagateKill)
                .build();
        } catch (Exception e) {
            runContext.logger().error("Failed to kill execution {}: {}", executionId, e.getMessage());
            throw new RuntimeException("Failed to kill execution: " + e.getMessage(), e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The execution ID that was killed")
        private String executionId;

        @Schema(title = "Whether the execution was successfully killed")
        private boolean killed;

        @Schema(title = "Whether the kill was propagated to child executions")
        private boolean propagateKill;
    }
}
