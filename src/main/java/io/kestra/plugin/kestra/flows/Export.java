package io.kestra.plugin.kestra.flows;

import io.kestra.core.models.annotations.Example;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.IdWithNamespace;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Export Kestra flows",
        description = "Exports one or more Kestra flows as a ZIP archive. You can specify flows by their ID and namespace."
)
@Plugin(
        examples = {
                @Example(
                        title = "Export a single flow",
                        full = true,
                        code = """
                id: export_single_flow
                namespace: company.team

                tasks:
                  - id: export_flow
                    type: io.kestra.plugin.kestra.flows.Export
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin
                      password: password
                    idsWithNamespace:
                      - id: my_flow_id
                        namespace: my.flow.namespace
                """
                ),
                @Example(
                        title = "Export multiple flows from different namespaces",
                        full = true,
                        code = """
                id: export_multiple_flows
                namespace: company.team

                tasks:
                  - id: export_flows
                    type: io.kestra.plugin.kestra.flows.Export
                    kestraUrl: https://my-ee-instance.io
                    auth:
                      username: myuser
                      password: mypassword
                    tenantId: mytenant
                    idsWithNamespace:
                      - id: flow_one
                        namespace: prod.data
                      - id: flow_two
                        namespace: dev.analytics
                      - id: flow_three
                        namespace: common.utils
                """
                )
        }
)
public class Export extends AbstractKestraTask implements RunnableTask<Export.Output> {
    @Schema(title = "The namespace to list flows on, if null, will default to the namespace of the current flow.")
    public Property<List<IdWithNamespace>> idsWithNamespace;

    @Override
    public Export.Output run(RunContext runContext) throws Exception {
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        List<IdWithNamespace> ids = runContext.render(idsWithNamespace).asList(IdWithNamespace.class);

        KestraClient kestraClient = kestraClient(runContext);
        byte[] zipBytes = kestraClient.flows().exportFlowsByIds(tId, ids);

        InputStream inputStream = new ByteArrayInputStream(zipBytes);
        String fileName = "exported_flows.zip";
        URI storedFileUri = runContext.storage().putFile(inputStream, fileName);


        return Export.Output.builder()
            .flowsZip(storedFileUri)
            .build();
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "URI of the exported flows ZIP file"
        )
        private URI flowsZip;
    }
}
