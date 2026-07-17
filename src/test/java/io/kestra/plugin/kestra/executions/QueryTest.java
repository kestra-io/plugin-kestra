package io.kestra.plugin.kestra.executions;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.FlowWithSource;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class QueryTest extends AbstractKestraOssContainerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.executions.search";

    @Test
    public void shouldSearchExecutions() throws Exception {
        RunContext runContext = runContextFactory.of();

        // Create a flow and trigger an execution to ensure there is at least one execution to find
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

        Query searchTask = Query.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .size(Property.ofValue(10))
            .fetchType(Property.ofValue(io.kestra.core.models.tasks.common.FetchType.FETCH))
            .build();

        FetchOutput output = searchTask.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getRows().size(), is(greaterThanOrEqualTo(1)));
    }

    @Test
    public void shouldSearchExecutionsByLabel() throws Exception {
        RunContext runContext = runContextFactory.of();

        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.getKestraClient().executions().createExecution(
            TENANT_ID, flow.getNamespace(), flow.getId(), List.of("key:value"), false, null, null, null, null
        );

        Query matchingLabel = Query.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .labels(Property.ofValue(Map.of("key", "value")))
            .size(Property.ofValue(10))
            .fetchType(Property.ofValue(io.kestra.core.models.tasks.common.FetchType.FETCH))
            .build();

        FetchOutput matchingOutput = matchingLabel.run(runContext);

        assertThat(matchingOutput, is(notNullValue()));
        assertThat(matchingOutput.getSize(), is(greaterThanOrEqualTo(1L)));

        Query nonMatchingLabel = matchingLabel.toBuilder()
            .labels(Property.ofValue(Map.of("key", "wrong-value")))
            .build();

        FetchOutput nonMatchingOutput = nonMatchingLabel.run(runContext);

        assertThat(nonMatchingOutput, is(notNullValue()));
        assertThat(nonMatchingOutput.getSize(), is(0L));
    }
}