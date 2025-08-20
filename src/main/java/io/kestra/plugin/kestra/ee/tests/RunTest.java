package io.kestra.plugin.kestra.ee.tests;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a test",
    description = "This task will run a test."
)
@Plugin(
)
public class RunTest extends AbstractKestraTask implements RunnableTask<RunTest.Output> {
    @Schema(title = "The namespace")
    private Property<String> namespace;

    @Schema(title = "The test id")
    private Property<String> testId;

    @Schema(title = "Specific test cases to run")
    @Nullable
    private Property<List<String>> testCases;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var testSuitesApi = kestraClient.testSuites();

        var rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rNamespace = runContext.render(namespace).as(String.class).orElseThrow();
        var rId = runContext.render(testId).as(String.class).orElseThrow();
        var rTestCases = runContext.render(testCases).asList(String.class);

        var testFullId = rNamespace + "." + rId;
        var runRequest = new TestSuiteControllerRunRequest().testCases(rTestCases);
        runContext.logger().info("Running test '{}", testFullId);

        var result = testSuitesApi.runTestSuite(rNamespace, rId, rTenantId, runRequest);
        Objects.requireNonNull(result.getResults());

        result.getResults().forEach(testCaseResult -> {
            var executionId = testCaseResult.getExecutionId();
            var url = testCaseResult.getUrl();
            var state = testCaseResult.getState();
            if (state == TestState.ERROR) {
                runContext.logger().error("Test case '{}' ended with status: {}.\nexecutionId: {}\nexecution url: {}\nerrors: {}",
                    testCaseResult.getTestId(), state, executionId, url,
                    formatErrors(testCaseResult)
                );
            } else {
                runContext.logger().info("Test case '{}' ended with status: {}.\nexecutionId: {}\nexecution url: {}", testCaseResult.getTestId(), state, executionId, url);
            }

            if (runContext.logger().isDebugEnabled()) {
                testCaseResult.getAssertionResults().forEach(assertionResult -> {
                    runContext.logger().debug("Assertion result: {}", formatAssertionResult(assertionResult));
                });
            }
        });

        var outputBuilder = Output.builder().result(result);
        switch (result.getState()) {
            case ERROR -> {
                runContext.logger().error("Test '{}' ended with ERROR", testFullId);
                outputBuilder.taskStateOverride(Optional.of(State.Type.FAILED));
            }
            case FAILED, SKIPPED -> {
                runContext.logger().warn("Test '{}' ended with {}", testFullId, result.getState());
                outputBuilder.taskStateOverride(Optional.of(State.Type.WARNING));
            }
            case SUCCESS -> {
                runContext.logger().info("Test '{}' ended with SUCCESS", testFullId);
            }
        }
        return outputBuilder.build();
    }

    private String formatAssertionResult(AssertionResult assertionResult) {
        var status = assertionResult.getIsSuccess() ? "SUCCESS" : "FAILED";
        var str = "assertion %s: expected %s %s %s".formatted(status, assertionResult.getExpected(), assertionResult.getOperator(), assertionResult.getActual());
        if (assertionResult.getDescription() != null) {
            str += "\ndescription: %s".formatted(assertionResult.getDescription());
        }
        if (assertionResult.getErrorMessage() != null) {
            str += "\nerror message: %s".formatted(assertionResult.getErrorMessage());
        }
        return str;
    }

    private static String formatErrors(UnitTestResult testCaseResult) {
        return testCaseResult.getErrors().stream().map(err -> {
            var str = err.getMessage();
            if (err.getDetails() != null) {
                str += ", details: " + err.getDetails();
            }
            return str;
        }).collect(Collectors.joining("\n"));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Test result"
        )
        private TestSuiteRunResult result;

        @Builder.Default
        private Optional<State.Type> taskStateOverride = Optional.empty();

        @Override
        public Optional<State.Type> finalState() {
            return this.taskStateOverride;
        }
    }
}
