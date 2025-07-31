package io.kestra.plugin.kestra.flows;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.IdWithNamespace;
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
                  - id: export_flow_by_id
                    type: io.kestra.plugin.kestra.flows.ExportById
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    flows:
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
                    type: io.kestra.plugin.kestra.flows.ExportById
                    kestraUrl: https://my-ee-instance.io
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    tenantId: main
                    flows:
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
public class ExportById extends AbstractKestraTask implements RunnableTask<ExportById.Output> {

    @Schema(title = "The flows to export.")
    public Property<List<IdWithNamespace>> flows;

    @Override
    public ExportById.Output run(RunContext runContext) throws Exception {
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        List<IdWithNamespace> ids = runContext.render(flows).asList(IdWithNamespace.class);

        KestraClient kestraClient = kestraClient(runContext);
        byte[] zipBytes = kestraClient.flows().exportFlowsByIds(tId, ids);


        InputStream inputStream = new ByteArrayInputStream(zipBytes);
        String fileName = "exported_flows.zip";
        URI storedFileUri = runContext.storage().putFile(inputStream, fileName);


        return ExportById.Output.builder()
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
