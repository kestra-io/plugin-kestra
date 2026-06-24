package io.kestra.plugin.kestra.ee.iam.invitations;

import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.IAMInvitationControllerApiInvitationCreateRequest;
import io.kestra.sdk.model.IAMInvitationControllerApiInvitationRole;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
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
    title = "Create an invitation",
    description = """
        Sends an invitation to a user by email, optionally assigning groups and roles.

        The target Kestra instance must have `kestra.url` configured so that invitation emails contain a valid \
        link for the recipient to accept the invitation. Without it the invitation is created but the email link \
        will be empty or broken.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Invite a user by email.",
            full = true,
            code = """
                id: iam_invitation_create
                namespace: company.team

                tasks:
                  - id: invite_user
                    type: io.kestra.plugin.kestra.ee.iam.invitations.Create
                    email: "user@example.com"
                    groupIds:
                      - "group-id-1"
                """
        )
    }
)
public class Create extends AbstractKestraTask implements RunnableTask<Create.Output> {

    @NotNull
    @Schema(title = "Email address of the user to invite")
    @PluginProperty(group = "main")
    private Property<String> email;

    @Schema(title = "Group IDs to assign to the invited user")
    @PluginProperty(group = "main")
    private Property<List<String>> groupIds;

    @Schema(title = "Roles to assign to the invited user")
    @PluginProperty(group = "main")
    private Property<List<IAMInvitationControllerApiInvitationRole>> roles;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rEmail = runContext.render(email).as(String.class).orElseThrow();
        var rGroupIds = runContext.render(groupIds).asList(String.class);
        var rRoles = runContext.render(roles).asList(IAMInvitationControllerApiInvitationRole.class);

        var request = new IAMInvitationControllerApiInvitationCreateRequest()
            .email(rEmail)
            .groups(rGroupIds.isEmpty() ? null : rGroupIds)
            .roles(rRoles.isEmpty() ? null : rRoles);

        var created = kestraClient.invitations().createInvitation(rTenant, request);
        return Output.builder().invitationId(created.getId()).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "ID of the created invitation")
        private String invitationId;
    }
}
