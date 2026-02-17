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
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Kill a running execution",
    description =
        "Terminates a non-finished execution and, by default, propagates the kill to child executions. Use on active states; already terminated executions are not affected."
)
@Plugin(
    examples = {
      @Example(
          title = "Kill the current execution with propagation to child executions",
          full = true,
          code =
              """
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
                    namespace: company.team
                    wait: false
                  - id: kill
                    type: io.kestra.plugin.kestra.executions.Kill
                    runIf: "{{ inputs.shouldKill == true }}"
                    executionId: "{{ execution.id }}"
                    propagateKill: true
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                """),
      @Example(
          title =
              "Kill a specific execution by ID. Use \"{{ execution.id }}\" to kill the current execution.",
          full = true,
          code =
              """
                  id: kill-specific-execution
                  namespace: company.team

                  tasks:
                    - id: kill_execution
                      type: io.kestra.plugin.kestra.executions.Kill
                      executionId: "{{ vars.targetExecutionId }}"
                      propagateKill: false
                      kestraUrl: http://localhost:8080
                      auth:
                        username: "{{ secret('KESTRA_USERNAME') }}"
                        password: "{{ secret('KESTRA_PASSWORD') }}"
                  """)
    })
public class Kill extends AbstractKestraTask implements RunnableTask<VoidOutput> {
  @Schema(
      title = "Execution ID to kill",
      description = "ID of the execution to kill; use `{{ execution.id }}` for the current one."
  )
  @NotNull
  private Property<String> executionId;

  @Schema(
      title = "Propagate kill to child executions",
      description = "Defaults to true. When true, also kills subflow executions."
  )
  @Builder.Default
  private Property<Boolean> propagateKill = Property.ofValue(true);

  @Override
  public VoidOutput run(RunContext runContext) throws Exception {
    boolean rPropagateKill = runContext.render(this.propagateKill).as(Boolean.class).orElse(true);
    String rTenantId =
        runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
    String rExecutionId = runContext.render(this.executionId).as(String.class).orElseThrow();

    runContext
        .logger()
        .info("Killing execution {} with propagateKill={}", rExecutionId, rPropagateKill);
    KestraClient kestraClient = kestraClient(runContext);

    kestraClient.executions().killExecution(rExecutionId, rPropagateKill, rTenantId);
    runContext.logger().debug("Successfully killed execution {}", rExecutionId);

    return null;
  }
}
