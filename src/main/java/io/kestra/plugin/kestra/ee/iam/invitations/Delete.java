package io.kestra.plugin.kestra.ee.iam.invitations;

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
    title = "Delete an invitation",
    description = "Cancels and removes a pending invitation by its ID."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete an invitation.",
            full = true,
            code = """
                id: iam_invitation_delete
                namespace: company.team

                tasks:
                  - id: delete_invitation
                    type: io.kestra.plugin.kestra.ee.iam.invitations.Delete
                    invitationId: "{{ outputs.invite_user.invitationId }}"
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Invitation ID to delete")
    @PluginProperty(group = "main")
    private Property<String> invitationId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.invitations().deleteInvitation(
            runContext.render(invitationId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
