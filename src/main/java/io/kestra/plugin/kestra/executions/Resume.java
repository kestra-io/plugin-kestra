package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Resume a paused execution",
    description = "Resumes a PAUSED execution via the Kestra API. Defaults to the current execution id when executionId is empty."
)
@Plugin(
    examples = {
        @Example(
            title = "Resume an execution",
            full = true,
            code = """
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
        )
    }
)
public class Resume extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Execution ID to resume",
        description = "Defaults to the current execution when not provided."
    )
    @PluginProperty(group = "advanced")
    private Property<String> executionId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String rExecutionId = this.executionId != null ? runContext.render(this.executionId).as(String.class).orElse(null) : null;

        if (rExecutionId == null) {
            rExecutionId = runContext.render("{{ execution.id }}");
        }

        String rTenant = runContext.render(this.tenantId).as(String.class)
            .orElse(runContext.flowInfo().tenantId());

        runContext.logger().info("Resuming execution {}", rExecutionId);
        this.kestraClient(runContext).executions()
            .resumeExecution(rExecutionId, rTenant);

        runContext.logger().debug("Successfully resumed execution {}", rExecutionId);
        return null;
    }
}
