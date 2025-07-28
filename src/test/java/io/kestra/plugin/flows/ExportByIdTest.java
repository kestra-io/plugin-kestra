package io.kestra.plugin.flows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.flows.ExportById;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.IdWithNamespace;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
public class ExportByIdTest extends AbstractKestraContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.flows.byid.export";

    @Test
    public void shouldExportFlows() throws Exception {
        RunContext runContext = runContextFactory.of();

        FlowWithSource flow1 = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        FlowWithSource flow2 = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        ExportById exportFlows = ExportById.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .flows(
                Property.ofValue(
                    List.of(
                        new IdWithNamespace().id(flow1.getId()).namespace(flow1.getNamespace()),
                        new IdWithNamespace().id(flow2.getId()).namespace(flow2.getNamespace())
                    )
                )
            )
            .build();

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        ExportById.Output listFlowsOutput = exportFlows.run(runContext);

        assertThat(listFlowsOutput.getFlowsZip(), is(notNullValue()));
    }

}
