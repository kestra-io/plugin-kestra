package io.kestra.plugin.kestra.ee.iam.roles;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a role by ID",
    description = "Removes a role from the current tenant using its identifier."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a role.",
            full = true,
            code = """
                id: iam_role_delete
                namespace: company.team

                tasks:
                  - id: delete_role
                    type: io.kestra.plugin.kestra.ee.iam.roles.Delete
                    roleId: "{{ outputs.upsert_role.id }}"
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Role ID to delete")
    @PluginProperty(group = "main")
    private Property<String> roleId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.roles().deleteRole(
            runContext.render(roleId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
