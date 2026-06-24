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
    @Schema(
        title = "Service account name",
        description = """
            Name of the service account to create or update. Must match `^(?=.{1,63}$)[a-z0-9]+(?:-[a-z0-9]+)*$`:
            lowercase alphanumeric characters and hyphens only, starting and ending with an alphanumeric character,
            maximum 63 characters.
            """
    )
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

        // No name-based filter available for service accounts; paginate and match in Java
        String existingId = findByName(kestraClient, rTenant, rName);

        String serviceAccountId;
        if (existingId != null) {
            serviceAccountId = existingId;
            kestraClient.serviceAccount().updateServiceAccount(serviceAccountId, rTenant, request);
        } else {
            var created = kestraClient.serviceAccount().createServiceAccountForTenant(rTenant, request);
            serviceAccountId = created.getId();
        }

        return Output.builder().id(serviceAccountId).build();
    }

    private String findByName(io.kestra.sdk.KestraClient kestraClient, String tenant, String name) throws io.kestra.sdk.internal.ApiException {
        int page = 1;
        int size = 100;
        long total = Long.MAX_VALUE;

        while ((long) (page - 1) * size < total) {
            var response = kestraClient.serviceAccount().listServiceAccountsForTenant(tenant, page, size, null, null);
            total = response.getTotal();

            var match = response.getResults().stream()
                .filter(sa -> name.equals(sa.getName()))
                .findFirst();

            if (match.isPresent()) {
                return match.get().getId();
            }

            if (response.getResults().isEmpty()) {
                break;
            }

            page++;
        }

        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "ID of the created or updated service account")
        private String id;
    }
}
