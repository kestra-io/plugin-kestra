package io.kestra.plugin.kestra.ee.tests;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.TestState;
import io.kestra.sdk.model.TestSuiteServiceRunByQueryRequest;
import io.kestra.sdk.model.TestSuiteServiceTestRunByQueryResult;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.plugin.kestra.ee.tests.RunTest.logTestCase;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run multiple unit tests",
    description = "Run unit tests at multiple levels: all tests for a tenant or namespace, all tests for a specific flow, or a test suite by ID."
)
@Plugin(
    examples = {
        @Example(
            title = "Run all current tenant tests",
            full = true,
            code = """
                id: run_tenant_tests
                namespace: company.team

                tasks:
                  - id: run_tenant_all_tests
                    type: io.kestra.plugin.kestra.ee.tests.RunTests
                    auth:
                      apiToken: "{{ secret('KESTRA_API_TOKEN') }}"
                """
        )
    }
)
public class RunTests extends AbstractKestraTask implements RunnableTask<RunTests.Output> {
    @Schema(title = "The namespace")
    @Nullable
    private Property<String> namespace;

    @Schema(title = "To include child namespaces or not")
    @Builder.Default
    private Property<Boolean> includeChildNamespaces = Property.ofValue(true);

    @Schema(title = "The Flow id")
    @Nullable
    private Property<String> flowId;

    @Schema(title = "Should the task be marked as FAILED when a test fails")
    @Builder.Default
    private Property<Boolean> failOnTestFailure = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var testSuitesApi = kestraClient.testSuites();

        var rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        var rIncludeChildNamespaces = runContext.render(includeChildNamespaces).as(Boolean.class).orElse(true);
        var rFlowId = runContext.render(flowId).as(String.class).orElse(null);
        var rFailOnTestFailure = runContext.render(failOnTestFailure).as(Boolean.class).orElse(false);

        var runByQueryRequest = new TestSuiteServiceRunByQueryRequest()
            .namespace(rNamespace)
            .includeChildNamespaces(rIncludeChildNamespaces)
            .flowId(rFlowId);
        runContext.logger().info("Running tests for query: namespace: '{}', includeChildNamespaces: '{}', flowId: '{}'", runByQueryRequest.getNamespace(), runByQueryRequest.getIncludeChildNamespaces(), runByQueryRequest.getFlowId());

        var result = testSuitesApi.runTestSuitesByQuery(rTenantId, runByQueryRequest);
        Objects.requireNonNull(result.getResults());
        runContext.logger().info("Requested to run {} test suites, {} test cases", result.getNumberOfTestSuitesToBeRun(), result.getNumberOfTestCasesToBeRun());

        var outputBuilder = Output.builder().result(result);
        AtomicReference<Optional<State.Type>> errorState = new AtomicReference<>(Optional.empty());
        result.getResults().forEach(testSuiteRunResult -> {
            var testSuiteFullId = testSuiteRunResult.getNamespace() + "." + testSuiteRunResult.getTestSuiteId();
            testSuiteRunResult.getResults()
                .forEach(testCaseResult -> logTestCase(runContext.logger(), testSuiteFullId, testCaseResult));

            switch (testSuiteRunResult.getState()) {
                case ERROR -> {
                    runContext.logger().error("Test '{}' ended with ERROR", testSuiteFullId);
                    errorState.set(markTaskAsError(errorState.get()));
                }
                case FAILED -> {
                    runContext.logger().warn("Test '{}' ended with {}", testSuiteFullId, testSuiteRunResult.getState());
                    if (rFailOnTestFailure) {
                        errorState.set(markTaskAsError(errorState.get()));
                    } else {
                        errorState.set(markTaskAsWarning(errorState.get()));
                    }
                }
                case SKIPPED -> {
                    runContext.logger().warn("Test '{}' SKIPPED", testSuiteFullId);
                    errorState.set(markTaskAsWarning(errorState.get()));
                }
                case SUCCESS -> {
                    runContext.logger().info("Test '{}' ended with SUCCESS", testSuiteFullId);
                }
            }
        });
        if(errorState.get().isPresent()) {
            outputBuilder.taskStateOverride(Optional.of(errorState.get().get()));
        }

        var testSuitesRunCount = result.getResults().size();
        var testSuitesRunSuccessCount = result.getResults().stream().filter(t -> TestState.SUCCESS.equals(t.getState())).count();
        outputBuilder.testSuitesRunSuccessCount(testSuitesRunSuccessCount);
        var testSuitesRunSkippedCount = result.getResults().stream().filter(t -> TestState.SKIPPED.equals(t.getState())).count();
        outputBuilder.testSuitesRunSkippedCount(testSuitesRunSkippedCount);
        var testSuitesRunFailedCount = result.getResults().stream().filter(t -> TestState.ERROR.equals(t.getState()) || TestState.FAILED.equals(t.getState())).count();
        outputBuilder.testSuitesRunFailedCount(testSuitesRunFailedCount);
        runContext.logger().info("{} Test suites finished running, {} in success, {} skipped, {} failed", testSuitesRunCount, testSuitesRunSuccessCount, testSuitesRunSkippedCount, testSuitesRunFailedCount);

        return outputBuilder.build();
    }

    private static Optional<State.Type> markTaskAsError(Optional<State.Type> errorState) {
        return Optional.of(State.Type.FAILED);
    }

    private static Optional<State.Type> markTaskAsWarning(Optional<State.Type> errorState) {
        if (errorState.isPresent() && errorState.get() == State.Type.FAILED) {
            return errorState;
        } else {
            return Optional.of(State.Type.WARNING);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Tests by query result"
        )
        private TestSuiteServiceTestRunByQueryResult result;

        @Schema(
            title = "Number of test suites run in success"
        )
        private Long testSuitesRunSuccessCount;
        @Schema(
            title = "Number of test suites skipped"
        )
        private Long testSuitesRunSkippedCount;
        @Schema(
            title = "Number of test suites failed"
        )
        private Long testSuitesRunFailedCount;

        @Builder.Default
        private Optional<State.Type> taskStateOverride = Optional.empty();

        @Override
        public Optional<State.Type> finalState() {
            return this.taskStateOverride;
        }
    }
}
