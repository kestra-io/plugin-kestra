package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.StateType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete an execution",
    description =
        "This task will delete an execution and optionally propagate the delete to execution logs, metrics and files in the internal storage.")
@Plugin(
    examples = {
      @Example(
          title = "Delete a specific execution by ID",
          full = true,
          code =
              """
                id: delete-specific-execution
                namespace: company.team

                tasks:
                  - id: delete_execution
                    type: io.kestra.plugin.kestra.executions.Delete
                    executionId: "{{ vars.targetExecutionId }}"
                    deleteLogs: true
                    deleteMetrics: true
                    deleteStorage: true
                    auth:
                      apiToken: "{{ secrets('KESTRA_API_TOKEN') }}"
                """)
    })
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {
  @Schema(
      title = "The execution ID to delete",
      description =
          "The ID of the execution to delete. It's not allowed to delete the current execution.")
  @NotNull
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
  @SuppressWarnings("unchecked")
  public VoidOutput run(RunContext runContext) throws Exception {
    var currentExecution = (Map<String, Object>) runContext.getVariables().get("execution");
    var currentExecutionId = currentExecution != null ? (String) currentExecution.get("id") : "";

    boolean rDeleteLogs = runContext.render(this.deleteLogs).as(Boolean.class).orElse(true);
    boolean rDeleteMetrics = runContext.render(this.deleteMetrics).as(Boolean.class).orElse(true);
    boolean rDeleteStorage = runContext.render(this.deleteStorage).as(Boolean.class).orElse(true);
    String rTenantId =
        runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
    String rExecutionId = runContext.render(this.executionId).as(String.class).orElseThrow();

    if (rExecutionId.isBlank()) {
      throw new IllegalArgumentException("The execution id is required");
    }

    if (rExecutionId.equals(currentExecutionId)) {
      throw new IllegalArgumentException(
          "It's not allowed to delete the current execution " + rExecutionId);
    }

    runContext
        .logger()
        .info(
            "Deleting execution {} with deleteLogs={},deleteMetrics={},deleteLogs={}",
            rExecutionId,
            rDeleteLogs,
            rDeleteMetrics,
            rDeleteStorage);

    KestraClient kestraClient = kestraClient(runContext);
    Execution execution = kestraClient.executions().getExecution(rExecutionId, rTenantId);

    if (execution == null) {
      throw new IllegalArgumentException("Execution " + rExecutionId + " not found");
    } else {

      StateType state = execution.getState().getCurrent();
      boolean isTerminated = isTerminated(state);

      if (!isTerminated) {
        throw new IllegalArgumentException(
            "Execution " + rExecutionId + " is not in a terminate state (" + state + ")");
      }

      kestraClient
          .executions()
          .deleteExecution(rExecutionId, rDeleteLogs, rDeleteMetrics, rDeleteStorage, rTenantId);
      runContext.logger().debug("Successfully deleted execution {}", rExecutionId);
    }

    return null;
  }

  private boolean isTerminated(StateType stateType) {
    return stateType == StateType.FAILED
        || stateType == StateType.WARNING
        || stateType == StateType.SUCCESS
        || stateType == StateType.KILLED
        || stateType == StateType.CANCELLED
        || stateType == StateType.RETRIED
        || stateType == StateType.SKIPPED;
  }
}
