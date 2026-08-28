package io.kestra.plugin.kestra.ee.locks;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.executions.Query;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.ApiLightExecution;
import io.kestra.sdk.model.AssetsControllerAssetLockRequest;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class AcquireTest extends AbstractKestraEeContainerTest {

    @Inject
    TestRunContextFactory runContextFactory;

    private Acquire.AcquireOutput acquire(String assetId) throws Exception {
        Acquire acquire = Acquire.builder()
            .id(Acquire.class.getSimpleName())
            .type(Acquire.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .assetId(Property.ofExpression("{{ inputs.assetId }}"))
            .ttl(Property.ofValue(Duration.ofMinutes(5)))
            .build();

        RunContext runContext = realRunContext(acquire, Map.of("assetId", assetId));
        return acquire.run(runContext);
    }

    // Backed by a real execution: the lockAsset endpoint validates executionId against the backend.
    private RunContext realRunContext(Task task, Map<String, Object> inputs) throws Exception {
        Flow flow = TestsUtils.mockFlow();
        String executionId = createRealExecutionId(flow.getNamespace());

        Execution execution = Execution.builder()
            .id(executionId)
            .tenantId(flow.getTenantId())
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .inputs(inputs)
            .state(new State())
            .build()
            .withState(State.Type.RUNNING);

        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);

        RunContext runContext = runContextFactory.of(flow, task, execution, taskRun);
        runContextFactory.initializer().forExecutor((DefaultRunContext) runContext);
        return runContext;
    }

    private String createRealExecutionId(String namespace) throws Exception {
        String flowId = "lock_" + IdUtils.create().replace("-", "_");
        String flow = """
            id: "%s"
            namespace: "%s"
            tasks:
              - id: noop
                type: io.kestra.plugin.core.log.Log
                message: noop
            """.formatted(flowId, namespace);

        kestraTestDataUtils.getKestraClient().flows().createFlow(TENANT_ID, flow);
        kestraTestDataUtils.createRandomizedExecution(flowId, namespace);

        return queryExecution(flowId, namespace).getId();
    }

    private ApiLightExecution queryExecution(String flowId, String namespace) throws Exception {
        RunContext runContext = runContextFactory.of();

        return Await.until(
            () ->
            {
                try {
                    Query searchTask = Query.builder()
                        .kestraUrl(Property.ofValue(KESTRA_URL))
                        .auth(
                            AbstractKestraTask.Auth.builder()
                                .username(Property.ofValue(USERNAME))
                                .password(Property.ofValue(PASSWORD))
                                .build()
                        )
                        .tenantId(Property.ofValue(TENANT_ID))
                        .namespace(Property.ofValue(namespace))
                        .flowId(Property.ofValue(flowId))
                        .size(Property.ofValue(10))
                        .fetchType(Property.ofValue(FetchType.FETCH))
                        .build();

                    var output = searchTask.run(runContext);
                    if (output.getRows() == null || output.getRows().isEmpty()) {
                        return null;
                    }

                    Object row = output.getRows().getFirst();
                    if (row instanceof java.util.ArrayList<?> arrayList && !arrayList.isEmpty()) {
                        return (ApiLightExecution) arrayList.getFirst();
                    }

                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            Duration.ofMillis(200),
            Duration.ofSeconds(5)
        );
    }

    @Test
    void shouldAcquireExecutionOwnedLock() throws Exception {
        String assetId = IdUtils.create().toLowerCase();
        kestraTestDataUtils.createAsset(assetId, "TABLE");

        Acquire.AcquireOutput output = acquire(assetId);

        assertThat(output.getOwnerType()).isEqualTo("EXECUTION");
        assertThat(output.getExecutionId()).isNotBlank();
        assertThat(output.getLockedUntil()).isAfter(Instant.now());
    }

    @Test
    void shouldRejectLockingAnAlreadyLockedAsset() throws Exception {
        String assetId = IdUtils.create().toLowerCase();
        kestraTestDataUtils.createAsset(assetId, "TABLE");
        acquire(assetId);

        assertThatThrownBy(() -> kestraTestDataUtils.getKestraClient().assets()
            .lockAsset(assetId, TENANT_ID, new AssetsControllerAssetLockRequest().ttl("PT5M")))
            .isInstanceOf(ApiException.class);
    }
}
