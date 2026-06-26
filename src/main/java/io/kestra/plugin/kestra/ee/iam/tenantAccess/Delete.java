package io.kestra.plugin.kestra.ee.iam.tenantAccess;

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
    title = "Revoke tenant access from a user",
    description = "Removes access to the current tenant for the user identified by their user ID."
)
@Plugin(
    examples = {
        @Example(
            title = "Revoke tenant access from a user.",
            full = true,
            code = """
                id: iam_tenant_access_delete
                namespace: company.team

                tasks:
                  - id: revoke_access
                    type: io.kestra.plugin.kestra.ee.iam.tenantAccess.Delete
                    userId: "user-external-id"
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "User ID whose tenant access should be revoked")
    @PluginProperty(group = "main")
    private Property<String> userId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.tenantAccess().deleteTenantAccess(
            runContext.render(userId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
