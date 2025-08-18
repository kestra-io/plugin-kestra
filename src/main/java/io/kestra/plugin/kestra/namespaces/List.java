package io.kestra.plugin.kestra.namespaces;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.PagedResultsNamespaceWithDisabled;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Kestra Namespaces",
    description = "Retrieves a list of Kestra namespaces, offering options for filtering by prefix, pagination, and excluding non-existent namespaces."
)
@Plugin(
    examples = {
        @Example(
            title = "List all namespaces with pagination",
            full = true,
            code = """
                id: list_paginated_namespaces
                namespace: company.team

                tasks:
                  - id: list_namespaces_paged
                    type: io.kestra.plugin.kestra.namespaces.List
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    page: 1
                    size: 20
                """
        ),
        @Example(
            title = "List only existing namespaces starting with 'dev.'",
            full = true,
            code = """
                id: list_filtered_namespaces
                namespace: company.team

                tasks:
                  - id: list_dev_namespaces
                    type: io.kestra.plugin.kestra.namespaces.List
                    kestraUrl: https://cloud.kestra.io
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    tenantId: mytenant
                    prefix: dev.
                    existingOnly: true
                """
        ),
        @Example(
            title = "List all namespaces without pagination (fetch all pages)",
            full = true,
            code = """
                id: list_all_namespaces
                namespace: company.team

                tasks:
                  - id: fetch_all_namespaces
                    type: io.kestra.plugin.kestra.namespaces.List
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    # No 'page' or 'size' properties to fetch all
                """
        )
    }
)
public class List extends AbstractKestraTask implements RunnableTask<List.Output> {
    @Schema(title = "The namespace prefix, if null, all namespaces will be listed.")
    private Property<String> prefix;

    @Nullable
    @Schema(title = "If not provided, every pages are fetched",
        description = "For example, set to 1, it can be used to only fetch the first 10 results used with `size`.")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(title = "The number of namespaces to return per page.")
    private Property<Integer> size = Property.ofValue(10);

    @Builder.Default
    @Schema(title = "Return only existing namespace",
        description = "Set to true, namespaces that exists only because a flow is using it will not be returned.")
    private Property<Boolean> existingOnly = Property.ofValue(false);

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        Integer rPage = runContext.render(this.page).as(Integer.class).orElse(null);
        Integer rSize = runContext.render(this.size).as(Integer.class).orElse(10);
        String ns = runContext.render(prefix).as(String.class).orElse("");
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        Boolean rExistingOnly = runContext.render(existingOnly).as(Boolean.class).orElse(false);

        KestraClient kestraClient = kestraClient(runContext);
        java.util.List<String> allNamespaces = new ArrayList<String>();

        // If page is provided, fetch only that specific page
        if (rPage != null) {
            PagedResultsNamespaceWithDisabled results = kestraClient.namespaces()
                .searchNamespaces(
                    rPage,
                    rSize,
                    tId,
                    ns,
                    null,
                    rExistingOnly
                );
            results.getResults().forEach(namespace -> allNamespaces.add(namespace.getId()));
        } else {
            int currentPage = 1;
            long total;
            do {
                PagedResultsNamespaceWithDisabled results = kestraClient.namespaces()
                    .searchNamespaces(
                        currentPage,
                        rSize,
                        tId,
                        ns,
                        null,
                        rExistingOnly
                    );
                results.getResults().forEach(namespace -> allNamespaces.add(namespace.getId()));
                total = results.getTotal();
                currentPage++;
            } while ((long) currentPage * rSize < total);
        }

        return List.Output.builder()
            .namespaces(allNamespaces)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "A list of Kestra namespaces."
        )
        private java.util.List<String> namespaces;
    }
}
