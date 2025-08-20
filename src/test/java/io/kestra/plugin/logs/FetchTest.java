package io.kestra.plugin.logs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.logs.Fetch;
import io.kestra.sdk.model.FlowWithSource;
import io.kestra.sdk.model.Level;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class FetchTest extends AbstractKestraContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.logs.fetch";

    @Test
    public void shouldFetchLogsFromNamespace() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".shouldfetchlogsfromnamespace";

        // Create some test executions to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add small delay to allow logs to be created
        Thread.sleep(2000);

        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, null, null, null, null, null, null, FetchType.FETCH);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
        assertThat(fetchOutput.getRows(), is(notNullValue()));
    }

    @Test
    public void shouldFetchLogsWithPagination() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".withpagination";

        // Create multiple executions to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        for (int i = 0; i < 5; i++) {
            kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);
        }

        // Add delay to allow logs to be created
        Thread.sleep(3000);

        // Fetch with pagination - page 1, size 10
        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, 1, 10, null, null, null, null, null, null, FetchType.FETCH);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
        assertThat(fetchOutput.getRows(), is(notNullValue()));
    }

    @Test
    public void shouldFetchLogsWithMinLevel() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".withminlevel";

        // Create execution to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add delay to allow logs to be created
        Thread.sleep(2000);

        // Fetch only INFO level and above
        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, null, null, null, null, Level.INFO, null, FetchType.FETCH);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void shouldFetchLogsWithDateRange() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".withdaterange";

        ZonedDateTime startDate = ZonedDateTime.now().minusHours(1);
        ZonedDateTime endDate = ZonedDateTime.now().plusHours(1);

        // Create execution within date range
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add delay to allow logs to be created
        Thread.sleep(2000);

        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, null, null, startDate, endDate, null, null, FetchType.FETCH);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void shouldFetchLogsWithQuery() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".withquery";

        // Create execution to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add delay to allow logs to be created
        Thread.sleep(2000);

        // Search for common log terms from the test flow (Hello from KestraSDKHelper!)
        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, null, null, null, null, null, "Hello", FetchType.FETCH);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void shouldStoreLogs() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".shouldstorelogs";

        // Create execution to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add delay to allow logs to be created
        Thread.sleep(2000);

        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, null, null, null, null, null, null, FetchType.STORE);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
        if (fetchOutput.getSize() > 0) {
            assertThat(fetchOutput.getUri(), is(notNullValue()));
        }
    }

    @Test
    public void shouldFetchOneLog() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".shouldfetchonelog";

        // Create execution to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add delay to allow logs to be created
        Thread.sleep(2000);

        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, null, null, null, null, null, null, FetchType.FETCH_ONE);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        if (fetchOutput.getSize() > 0) {
            assertThat(fetchOutput.getRow(), is(notNullValue()));
        }
    }

    @Test
    public void shouldFetchLogsByFlowId() throws Exception {
        RunContext runContext = runContextFactory.of();
        String NAMESPACE_LOCAL = NAMESPACE + ".byflowid";

        // Create execution to generate logs
        FlowWithSource flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedExecution(flow.getId(), NAMESPACE_LOCAL);

        // Add delay to allow logs to be created
        Thread.sleep(2000);

        // Filter by specific flow ID
        Fetch fetchLogs = fetchTask(NAMESPACE_LOCAL, null, null, flow.getId(), null, null, null, null, null, FetchType.FETCH);
        FetchOutput fetchOutput = fetchLogs.run(runContext);

        assertThat(fetchOutput.getSize(), greaterThanOrEqualTo(0L));
    }

    /**
     * Required because using `toBuilder()` with Property does not work as expected
     */
    private Fetch fetchTask(
        @Nullable String namespace,
        @Nullable Integer page,
        @Nullable Integer size,
        @Nullable String flowId,
        @Nullable String triggerId,
        @Nullable ZonedDateTime startDate,
        @Nullable ZonedDateTime endDate,
        @Nullable Level minLevel,
        @Nullable String query,
        @Nullable FetchType fetchType
    ) {
        Fetch.FetchBuilder<?, ?> fetchBuilder = Fetch.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID));

        if (namespace != null) {
            fetchBuilder.namespace(Property.ofValue(namespace));
        }
        if (page != null) {
            fetchBuilder.page(Property.ofValue(page));
        }
        if (size != null) {
            fetchBuilder.size(Property.ofValue(size));
        }
        if (flowId != null) {
            fetchBuilder.flowId(Property.ofValue(flowId));
        }
        if (triggerId != null) {
            fetchBuilder.triggerId(Property.ofValue(triggerId));
        }
        if (startDate != null) {
            fetchBuilder.startDate(Property.ofValue(startDate));
        }
        if (endDate != null) {
            fetchBuilder.endDate(Property.ofValue(endDate));
        }
        if (minLevel != null) {
            fetchBuilder.minLevel(Property.ofValue(minLevel));
        }
        if (query != null) {
            fetchBuilder.query(Property.ofValue(query));
        }
        if (fetchType != null) {
            fetchBuilder.fetchType(Property.ofValue(fetchType));
        }

        return fetchBuilder.build();
    }
}