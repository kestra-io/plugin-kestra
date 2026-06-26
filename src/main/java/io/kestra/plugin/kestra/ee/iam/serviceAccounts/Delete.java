package io.kestra.plugin.kestra.ee.iam.serviceAccounts;

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
    title = "Delete a service account",
    description = "Removes a service account from the current tenant using its ID."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a service account.",
            full = true,
            code = """
                id: iam_service_account_delete
                namespace: company.team

                tasks:
                  - id: delete_service_account
                    type: io.kestra.plugin.kestra.ee.iam.serviceAccounts.Delete
                    serviceAccountId: "{{ outputs.upsert_service_account.id }}"
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {

    @NotNull
    @Schema(title = "Service account ID to delete")
    @PluginProperty(group = "main")
    private Property<String> serviceAccountId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        kestraClient.serviceAccount().deleteServiceAccountForTenant(
            runContext.render(serviceAccountId).as(String.class).orElseThrow(),
            runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId())
        );
        return null;
    }
}
