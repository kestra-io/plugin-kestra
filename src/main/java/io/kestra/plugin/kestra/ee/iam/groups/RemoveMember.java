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
    title = "Remove a user from a group",
    description = "Removes the specified user from the given group."
)
@Plugin(
    examples = {
        @Example(
            title = "Remove a user from a group.",
            full = true,
            code = """
                id: iam_group_remove_member
                namespace: company.team

                tasks:
                  - id: remove_member
                    type: io.kestra.plugin.kestra.ee.iam.groups.RemoveMember
                    groupId: "{{ outputs.upsert_group.id }}"
                    userId: "user-external-id"
                """
        )
    }
)
public class RemoveMember extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Group ID")
    @PluginProperty(group = "main")
    private Property<String> groupId;

    @NotNull
    @Schema(title = "User ID to remove")
    @PluginProperty(group = "main")
    private Property<String> userId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.groups().deleteUserFromGroup(
            runContext.render(groupId).as(String.class).orElseThrow(),
            runContext.render(userId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
