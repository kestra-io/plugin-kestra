package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
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
    description = "This task will kill an execution and optionally propagate the kill to child executions."
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
        ),
        @Example(
            title = "Kill a specific execution by ID",
            full = true,
            code = """
                id: kill-specific-execution
                namespace: company.team

                tasks:
                  - id: kill_execution
                    type: io.kestra.plugin.kestra.executions.Kill
                    executionId: "{{ vars.targetExecutionId }}"
                    propagateKill: false
                """
        )
    }
)
public class Kill extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @Schema(title = "The execution ID to kill",
        description = "The ID of the execution to kill. If null, will kill the current execution."
    )
    private Property<String> executionId;

    @Schema(title = "Propagate kill to child executions",
        description = "Whether to also kill the child executions (subflows) when this execution is killed."
    )
    @Builder.Default
    private Property<Boolean> propagateKill = Property.ofValue(true);

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        boolean rPropagateKill = runContext.render(this.propagateKill).as(Boolean.class).orElse(true);
        String rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rExecutionId = runContext.render(this.executionId).as(String.class).orElse(runContext.render("{{ execution.id }}"));

        runContext.logger().info("Killing execution {} with propagateKill={}", rExecutionId, rPropagateKill);
        KestraClient kestraClient = kestraClient(runContext);

        kestraClient.executions().killExecution(rExecutionId, rPropagateKill, rTenantId);
        runContext.logger().info("Successfully killed execution {}", rExecutionId);

        return null;
    }
}
