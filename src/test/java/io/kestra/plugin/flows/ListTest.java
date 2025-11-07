package io.kestra.plugin.flows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.flows.List;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.kestra.core.models.Label.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class ListTest extends AbstractKestraOssContainerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.flows.list";

    @Test
    public void shouldListFlows() throws Exception {
        RunContext runContext = runContextFactory.of();

        List listFlows = List.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .build();

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        List.Output listFlowsOutput = listFlows.run(runContext);

        assertThat(listFlowsOutput.getFlows().size(), is(2));
    }
}
