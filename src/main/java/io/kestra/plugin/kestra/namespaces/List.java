package io.kestra.plugin.kestra.namespaces;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.PagedResultsNamespace;
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
    title = "List namespaces with paging",
    description = "Retrieves namespaces filtered by prefix with optional pagination. When page is null, iterates all pages. Defaults: size 10, existingOnly false."
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
    @Schema(title = "Namespace prefix", description = "Defaults to empty string to return all namespaces.")
    private Property<String> prefix;

    @Nullable
    @Schema(title = "Page number", description = "When null, fetches every page. Set with size to limit requests.")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(title = "Page size", description = "Defaults to 10.")
    private Property<Integer> size = Property.ofValue(10);

    @Builder.Default
    @Schema(title = "Existing namespaces only", description = "Defaults to false. When true, returns namespaces backed by stored definition, excluding transient ones.")
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
            PagedResultsNamespace results = kestraClient.namespaces()
                .searchNamespaces(
                    rPage,
                    rSize,
                    rExistingOnly,
                    tId,
                    ns,
                    null
                );
            results.getResults().forEach(namespace -> allNamespaces.add(namespace.getId()));
        } else {
            int currentPage = 1;
            long total;
            do {
                PagedResultsNamespace results = kestraClient.namespaces()
                    .searchNamespaces(
                        currentPage,
                        rSize,
                        rExistingOnly,
                        tId,
                        ns,
                        null
                    );
                results.getResults().forEach(namespace -> allNamespaces.add(namespace.getId()));
                total = results.getTotal();
            } while ((long) currentPage++ * rSize < total);
        }

        return List.Output.builder()
            .namespaces(allNamespaces)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "A list of Kestra namespaces"
        )
        private java.util.List<String> namespaces;
    }
}
