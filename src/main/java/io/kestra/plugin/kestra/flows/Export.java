package io.kestra.plugin.kestra.flows;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
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
    description = "Exports one or more Kestra flows as a ZIP archive. You can specify from a namespace prefix and/or labels to filter the flows to export. "
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
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    namespace: company.team
                """
        )
    }
)
public class Export extends AbstractKestraTask implements RunnableTask<Export.Output> {

    @Schema(title = "A namespace prefix filter.")
    public Property<String> namespace;

    @Schema(title = "A list of label with the format `key:value`")
    public Property<List<String>> labels;

    @Override
    public Export.Output run(RunContext runContext) throws Exception {
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        List<String> rLabels = runContext.render(labels).asList(String.class);

        KestraClient kestraClient = kestraClient(runContext);
        byte[] zipBytes = kestraClient.flows().exportFlowsByQuery(
            tId,
            null,
            null,
            null,
            rNamespace,
            rLabels
        );


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
