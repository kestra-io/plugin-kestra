package io.kestra.plugin.kestra.namespaces;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List distinct Kestra namespaces",
    description = "Retrieves a list of all distinct namespaces within a Kestra instance, optionally filtered by a prefix."
)
@Plugin(
    examples = {
        @Example(
            title = "List all distinct namespaces",
            full = true,
            code = """
                id: distinct_all_namespaces
                namespace: company.team

                tasks:
                  - id: list_namespaces
                    type: io.kestra.plugin.kestra.namespaces.NamespacesWithFlows
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                """
        ),
        @Example(
            title = "List distinct namespaces with a specific prefix",
            full = true,
            code = """
                id: distinct_prefixed_namespaces
                namespace: company.team

                tasks:
                  - id: list_prefixed_namespaces
                    type: io.kestra.plugin.kestra.namespaces.NamespacesWithFlows
                    kestraUrl: https://my-ee-instance.io
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    prefix: dev
                    tenantId: mytenant
                """
        )
    }
)
public class NamespacesWithFlows extends AbstractKestraTask implements RunnableTask<NamespacesWithFlows.Output> {

    @Schema(title = "The namespace prefix, if null, all namespaces will be listed.")
    @Nullable
    private Property<String> prefix;

    @Override
    public NamespacesWithFlows.Output run(RunContext runContext) throws Exception {
        String ns = runContext.render(prefix).as(String.class).orElse("");
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());

        KestraClient kestraClient = kestraClient(runContext);
        java.util.List<String> results = kestraClient.flows().listDistinctNamespaces(tId, ns);

        return NamespacesWithFlows.Output.builder()
            .namespaces(results)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "A list of distinct Kestra namespaces"
        )
        private java.util.List<String> namespaces;
    }
}

