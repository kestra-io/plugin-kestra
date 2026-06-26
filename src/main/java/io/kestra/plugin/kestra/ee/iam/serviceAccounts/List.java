package io.kestra.plugin.kestra.ee.iam.serviceAccounts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.IAMServiceAccountControllerApiServiceAccountDetail;
import io.kestra.sdk.model.PagedResultsIAMServiceAccountControllerApiServiceAccountDetail;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List service accounts",
    description = """
        Lists service accounts for the current tenant. Supports pagination (size defaults to 100) or fetches all pages when no page is set.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "List all service accounts.",
            full = true,
            code = """
                id: iam_service_account_list
                namespace: company.team

                tasks:
                  - id: list_service_accounts
                    type: io.kestra.plugin.kestra.ee.iam.serviceAccounts.List
                    fetchType: FETCH
                """
        )
    }
)
public class List extends AbstractKestraTask implements RunnableTask<List.Output> {

    @Nullable
    @Schema(
        title = "Page number",
        description = "If omitted, all pages are fetched. Set to 1 or higher to retrieve a single page."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(
        title = "Page size",
        description = "Number of service accounts per page; defaults to 100."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> size = Property.ofValue(100);

    @Schema(
        title = "Output fetch type",
        description = "Defines how results are returned: `FETCH` for direct output, `STORE` to persist as an ION file."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElseThrow();
        var rPage = runContext.render(this.page).as(Integer.class).orElse(null);
        var rSize = runContext.render(this.size).as(Integer.class).orElse(100);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());

        var kestraClient = kestraClient(runContext);

        java.util.List<IAMServiceAccountControllerApiServiceAccountDetail> fetched;
        if (rPage != null) {
            fetched = kestraClient.serviceAccount().listServiceAccounts(rPage, rSize, null, null).getResults();
        } else {
            fetched = new ArrayList<>();
            int currentPage = 1;
            long total;
            do {
                PagedResultsIAMServiceAccountControllerApiServiceAccountDetail results = kestraClient.serviceAccount().listServiceAccounts(currentPage, rSize, null, null);
                fetched.addAll(results.getResults());
                total = results.getTotal();
            } while ((long) currentPage++ * rSize < total);
        }

        var outputBuilder = Output.builder();
        switch (rFetchType) {
            case FETCH_ONE -> outputBuilder.serviceAccounts(java.util.List.of(fetched.getFirst())).size(1L);
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                    fetched.forEach(throwConsumer(sa ->
                    {
                        fileWriter.write(JacksonMapper.ofIon().writeValueAsString(sa));
                        fileWriter.write("\n");
                    }));
                }
                outputBuilder.uri(runContext.storage().putFile(tempFile)).size((long) fetched.size());
            }
            case FETCH -> outputBuilder.serviceAccounts(fetched).size((long) fetched.size());
            case NONE -> runContext.logger().info("fetchType is NONE, no output returned");
        }

        return outputBuilder.build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of fetched service accounts",
            description = "Only populated when using `fetchType=FETCH` or `fetchType=FETCH_ONE`."
        )
        private java.util.List<IAMServiceAccountControllerApiServiceAccountDetail> serviceAccounts;

        @Schema(
            title = "Kestra internal storage URI of the stored service accounts",
            description = "Only populated when using `fetchType=STORE`."
        )
        private URI uri;

        @Schema(
            title = "Number of fetched service accounts",
            description = "Only populated if fetchType is not NONE."
        )
        private Long size;
    }
}
