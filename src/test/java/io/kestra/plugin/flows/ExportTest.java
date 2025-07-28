package io.kestra.plugin.flows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.flows.Export;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
public class ExportTest extends AbstractKestraContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.flows.export";

    @Test
    public void shouldExportFlows() throws Exception {
        RunContext runContext = runContextFactory.of();

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedFlow(NAMESPACE + ".sub");

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        Export exportFlows = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .build();


        Export.Output listFlowsOutput = exportFlows.run(runContext);
        int fileCount = countFilesInZip(listFlowsOutput.getFlowsZip(), runContext);

        assertThat(listFlowsOutput.getFlowsZip(), is(notNullValue()));
        assertThat(fileCount, is(4));

        exportFlows = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE + ".sub"))
            .build();


        listFlowsOutput = exportFlows.run(runContext);

        fileCount = countFilesInZip(listFlowsOutput.getFlowsZip(), runContext);

        assertThat(listFlowsOutput.getFlowsZip(), is(notNullValue()));
        assertThat(fileCount, is(1));
    }

    public int countFilesInZip(URI zipUri, RunContext runContext) throws Exception {
        try (InputStream is = runContext.storage().getFile(zipUri);
             ZipInputStream zis = new ZipInputStream(is)) {
            int count = 0;
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    count++;
                }
            }
            return count;
        }
    }

}


