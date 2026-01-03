package io.kestra.plugin.kestra.executions;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.FlowWithSource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class CountTest extends AbstractKestraOssContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.executions.count";

    @Test
    public void shouldCountExecutions() throws Exception {
        RunContext runContext = runContextFactory.of();

        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

        Count countTask = Count.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .build();

        Count.Output output = countTask.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getCount(), is(notNullValue()));
        assertThat(output.getCount(), is(greaterThanOrEqualTo(1L)));
    }
}
