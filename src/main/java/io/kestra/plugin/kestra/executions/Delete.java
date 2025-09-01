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
    title = "Delete an execution",
    description = "This task will delete an execution and optionally propagate the delete to execution logs, metrics and files in the internal storage."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a specific execution by ID",
            full = true,
            code = """
                id: delete-specific-execution
                namespace: company.team

                tasks:
                  - id: delete_execution
                    type: io.kestra.plugin.kestra.executions.Delete
                    executionId: "{{ vars.targetExecutionId }}"
                    deleteLogs: true
                    deleteMetrics: true
                    deleteStorage: true
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @Schema(title = "The execution ID to delete",
        description = "The ID of the execution to delete. If null, will delete the current execution."
    )
    private Property<String> executionId;

    @Schema(title = "Whether to delete execution logs")
    @Builder.Default
    private Property<Boolean> deleteLogs = Property.ofValue(true);

    @Schema(title = "Whether to delete execution metrics")
    @Builder.Default
    private Property<Boolean> deleteMetrics = Property.ofValue(true);

    @Schema(title = "Whether to delete execution files in the internal storage")
    @Builder.Default
    private Property<Boolean> deleteStorage = Property.ofValue(true);

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        boolean rDeleteLogs = runContext.render(this.deleteLogs).as(Boolean.class).orElse(true);
        boolean rDeleteMetrics = runContext.render(this.deleteMetrics).as(Boolean.class).orElse(true);
        boolean rDeleteStorage = runContext.render(this.deleteStorage).as(Boolean.class).orElse(true);
        String rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rExecutionId = runContext.render(this.executionId).as(String.class).orElse(runContext.render("{{ execution.id }}"));

        runContext.logger().info("Deleteing execution {} with deleteLogs={},deleteMetrics={},deleteLogs={}", rExecutionId, rDeleteLogs,rDeleteMetrics, rDeleteStorage);
        KestraClient kestraClient = kestraClient(runContext);

        kestraClient.executions().deleteExecution(rExecutionId, rDeleteLogs,rDeleteMetrics,rDeleteStorage, rTenantId);
        runContext.logger().info("Successfully deleted execution {}", rExecutionId);

        return null;
    }
}
