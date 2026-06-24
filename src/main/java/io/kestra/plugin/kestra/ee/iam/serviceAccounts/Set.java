package io.kestra.plugin.kestra.ee.iam.serviceAccounts;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.IAMServiceAccountControllerApiServiceAccountRequest;

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
    title = "Create or update a service account",
    description = """
        Upserts a service account by name: searches for an existing service account with the given name and updates it if found, or creates a new one otherwise.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Upsert a service account.",
            full = true,
            code = """
                id: iam_service_account_set
                namespace: company.team

                tasks:
                  - id: upsert_service_account
                    type: io.kestra.plugin.kestra.ee.iam.serviceAccounts.Set
                    name: "etl-pipeline-sa"
                    serviceAccountDescription: "Service account for the ETL pipeline"
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<Set.Output> {

    @NotNull
    @Schema(title = "Service account name")
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(title = "Service account description")
    @PluginProperty(group = "main")
    private Property<String> serviceAccountDescription;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var rDescription = runContext.render(serviceAccountDescription).as(String.class).orElse(null);

        var request = new IAMServiceAccountControllerApiServiceAccountRequest()
            .name(rName)
            .description(rDescription);

        // No name-based filter available for service accounts; retrieve and match in Java
        var existing = kestraClient.serviceAccount()
            .listServiceAccountsForTenant(rTenant, 1, 100, null, null)
            .getResults().stream()
            .filter(sa -> rName.equals(sa.getName()))
            .findFirst();

        String serviceAccountId;
        if (existing.isPresent()) {
            serviceAccountId = existing.get().getId();
            kestraClient.serviceAccount().updateServiceAccount(serviceAccountId, rTenant, request);
        } else {
            var created = kestraClient.serviceAccount().createServiceAccountForTenant(rTenant, request);
            serviceAccountId = created.getId();
        }

        return Output.builder().id(serviceAccountId).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "ID of the created or updated service account")
        private String id;
    }
}
