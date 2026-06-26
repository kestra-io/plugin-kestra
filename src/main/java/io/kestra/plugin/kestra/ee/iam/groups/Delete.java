package io.kestra.plugin.kestra.ee.iam.groups;

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
    title = "Delete a group by ID",
    description = "Removes a group from the current tenant using its identifier."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a group.",
            full = true,
            code = """
                id: iam_group_delete
                namespace: company.team

                tasks:
                  - id: delete_group
                    type: io.kestra.plugin.kestra.ee.iam.groups.Delete
                    groupId: "{{ outputs.upsert_group.id }}"
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Group ID to delete")
    @PluginProperty(group = "main")
    private Property<String> groupId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.groups().deleteGroup(
            runContext.render(groupId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
