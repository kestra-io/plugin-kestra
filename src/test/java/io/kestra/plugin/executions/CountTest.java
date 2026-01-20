package io.kestra.plugin.executions;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Count;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.StateType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class CountTest extends AbstractKestraOssContainerTest {

    private static final String NAMESPACE = "kestra.tests.executions.count";

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void shouldCountExecutionsWithFilters() throws Exception {
        RunContext runContext = runContextFactory.of();

        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        kestraTestDataUtils.createRandomizedExecution(flow.getId(), flow.getNamespace());

        Count task = Count.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespaces(Property.ofValue(List.of(NAMESPACE)))
            .states(Property.ofValue(List.of(StateType.SUCCESS)))
            .startDate(Property.ofExpression("{{ now() | dateAdd(-1, 'DAYS') }}"))
            .endDate(Property.ofExpression("{{ now() | dateAdd(1, 'MINUTES') }}"))
            .expression("{{ count >= 1 }}")
            .build();

        Count.Output output = Await.until(() -> {
            Count.Output o = null;
            try {
                o = task.run(runContext);
            } catch (Exception e) {
                return null;
            }
            return o.getCount() >= 1 ? o : null;
            },
            Duration.ofMillis(200),
            Duration.ofSeconds(10)
        );

        assertThat(output, is(notNullValue()));
        assertThat(output.getCount(), is(notNullValue()));
        assertThat(output.getCount(), greaterThanOrEqualTo(1L));
    }
}
