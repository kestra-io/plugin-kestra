package io.kestra.plugin.kestra.ee.iam.tenantAccess;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.IAMTenantAccessControllerApiCreateTenantAccessRequest;

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
    title = "Grant tenant access to a user",
    description = "Grants access to the current tenant for the user identified by the given email address."
)
@Plugin(
    examples = {
        @Example(
            title = "Grant tenant access to a user by email.",
            full = true,
            code = """
                id: iam_tenant_access_set
                namespace: company.team

                tasks:
                  - id: grant_access
                    type: io.kestra.plugin.kestra.ee.iam.tenantAccess.Set
                    email: "user@example.com"
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Email address of the user to grant access to")
    @PluginProperty(group = "main")
    private Property<String> email;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rEmail = runContext.render(email).as(String.class).orElseThrow();

        kestraClient.tenantAccess().createTenantAccess(
            rTenant,
            new IAMTenantAccessControllerApiCreateTenantAccessRequest().email(rEmail)
        );
        return null;
    }
}
