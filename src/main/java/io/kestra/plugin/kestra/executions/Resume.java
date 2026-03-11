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
    description = "Resumes a PAUSED execution via the Kestra API. Defaults to the current execution id when executionId is empty; optional inputs are forwarded to the resumed run."
)
@Plugin(
    examples = {
        @Example(
            title = "Resume an execution",
            full = true,
            code = 
                """
                id: resume_execution
                namespace: company.team

                tasks:
                  - id: resume_execution
                    type: io.kestra.plugin.kestra.executions.Resume
                    executionId: "{{ trigger.executionId }}"
                    kestraUrl: http://localhost:8080
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                      
                """
        ),
        @Example(
            title = "Resume an execution with inputs",
            full = true,
            code = 
                """
                id: resume_execution_with_inputs
                namespace: company.team

                tasks:
                  - id: resume_execution
                    type: io.kestra.plugin.kestra.executions.Resume
                    executionId: "{{ trigger.executionId }}"
                    kestraUrl: http://localhost:8080
                    inputs:
                      comment: "Approved by automated process"
                      status: "OK"
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                """
        )
    }
)
public class Resume extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Execution ID to resume",
        description = "Defaults to the current execution when not provided."
    )
    private Property<String> executionId;

    @Schema(title = "Inputs to send with resume")
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

        Map<String, Object> rInputs = runContext.render(this.inputs).asMap(String.class, Object.class);

        runContext.logger().info("Resuming execution {}", rExecutionId);
        this.kestraClient(runContext).executions()
            .resumeExecution(rExecutionId, rTenant, new HashMap<>(rInputs));

        runContext.logger().debug("Successfully resumed execution {}", rExecutionId);

        return null;
    }
}
