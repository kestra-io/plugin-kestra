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
    title = "Add a user to a group",
    description = "Adds the specified user as a member of the given group."
)
@Plugin(
    examples = {
        @Example(
            title = "Add a user to a group.",
            full = true,
            code = """
                id: iam_group_add_member
                namespace: company.team

                tasks:
                  - id: add_member
                    type: io.kestra.plugin.kestra.ee.iam.groups.AddMember
                    groupId: "{{ outputs.upsert_group.id }}"
                    userId: "user-external-id"
                """
        )
    }
)
public class AddMember extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Group ID")
    @PluginProperty(group = "main")
    private Property<String> groupId;

    @NotNull
    @Schema(title = "User ID to add")
    @PluginProperty(group = "main")
    private Property<String> userId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.groups().addUserToGroup(
            runContext.render(groupId).as(String.class).orElseThrow(),
            runContext.render(userId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
