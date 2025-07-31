package io.kestra.plugin.kestra.flows;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.Flow;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Kestra flows",
    description = "Lists all flows within a specified namespace or the current flow's namespace if none is provided."
)
@Plugin(
    examples = {
        @Example(
            title = "List flows in the current namespace",
            full = true,
            code = """
                id: list_current_namespace_flows
                namespace: company.team.myflow

                tasks:
                  - id: list_flows
                    type: io.kestra.plugin.kestra.flows.List
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                """
        ),
        @Example(
            title = "List flows in a specific namespace",
            full = true,
            code = """
                id: list_specific_namespace_flows
                namespace: company.team.admin

                tasks:
                  - id: list_dev_flows
                    type: io.kestra.plugin.kestra.flows.List
                    kestraUrl: https://my-ee-instance.io
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    namespace: dev.flows
                    tenantId: myorganization
                """
        )
    }
)
public class List extends AbstractKestraTask implements RunnableTask<List.Output> {

    @Schema(title = "The namespace to list flows on, if null, will default to the namespace of the current flow.")
    private Property<String> namespace;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        String ns = runContext.render(namespace).as(String.class).orElseGet(() -> runContext.flowInfo().namespace());
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());

        KestraClient kestraClient = kestraClient(runContext);
        java.util.List<Flow> flows = kestraClient.flows().listFlowsByNamespace(ns, tId);

        return Output.builder()
            .flows(flows)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "A list of Kestra flows found."
        )
        private java.util.List<Flow> flows;
    }
}
