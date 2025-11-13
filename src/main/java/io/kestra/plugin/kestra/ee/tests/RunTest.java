package io.kestra.plugin.kestra.ee.tests;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

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
    title = "Run an unit test",
    description = "Run a single unit test. You can filter specific test cases to run, or all of them by default."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a test",
            full = true,
            code = """
               id: run_test
               namespace: company.team

               tasks:
                 - id: do_run_one_test
                   type: io.kestra.plugin.kestra.ee.tests.RunTest
                   auth:
                     apiToken: "{{ secret('KESTRA_API_TOKEN') }}"
                   namespace: company.team
                   testId: simple-testsuite
               """
        ),
        @Example(
            title = "Run a specific test testcase",
            full = true,
            code = """
                id: run_test_single_testcase
                namespace: company.team

                tasks:
                  - id: do_run_one_test
                    type: io.kestra.plugin.kestra.ee.tests.RunTest
                    auth:
                     apiToken: "{{ secret('KESTRA_API_TOKEN') }}"
                    namespace: company.team
                    testId: simple-testsuite
                    testCases:
                      - testcase_1
                """
        )
    }
)
public class RunTest extends AbstractKestraTask implements RunnableTask<RunTest.Output> {
    @Schema(title = "The namespace")
    @NotNull
    private Property<String> namespace;

    @Schema(title = "The test id")
    @NotNull
    private Property<String> testId;

    @Schema(title = "Specific test cases to run")
    @Nullable
    private Property<List<String>> testCases;

    @Schema(title= "Should the task be marked as FAILED when a test fails")
    @Builder.Default
    private Property<Boolean> failOnTestFailure = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var testSuitesApi = kestraClient.testSuites();

        var rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rNamespace = runContext.render(namespace).as(String.class).orElseThrow();
        var rId = runContext.render(testId).as(String.class).orElseThrow();
        var rTestCases = runContext.render(testCases).asList(String.class);
        var rFailOnTestFailure = runContext.render(failOnTestFailure).as(Boolean.class).orElse(false);

        var testFullId = rNamespace + "." + rId;
        var runRequest = new TestSuiteControllerRunRequest().testCases(rTestCases);
        runContext.logger().info("Running test '{}", testFullId);

        var result = testSuitesApi.runTestSuite(rNamespace, rId, rTenantId, runRequest);
        Objects.requireNonNull(result.getResults());

        result.getResults().forEach(testCaseResult -> {
            logTestCase(runContext.logger(), testFullId, testCaseResult);
        });

        var outputBuilder = Output.builder().result(result);
        switch (result.getState()) {
            case ERROR -> {
                runContext.logger().error("Test '{}' ended with ERROR", testFullId);
                outputBuilder.taskStateOverride(Optional.of(State.Type.FAILED));
            }
            case FAILED -> {
                runContext.logger().warn("Test '{}' ended with {}", testFullId, result.getState());
                if(rFailOnTestFailure){
                    outputBuilder.taskStateOverride(Optional.of(State.Type.FAILED));
                } else {
                    outputBuilder.taskStateOverride(Optional.of(State.Type.WARNING));
                }
            }
            case SKIPPED -> {
                runContext.logger().warn("Test '{}' SKIPPED", testFullId);
                outputBuilder.taskStateOverride(Optional.of(State.Type.WARNING));
            }
            case SUCCESS -> {
                runContext.logger().info("Test '{}' ended with SUCCESS", testFullId);
            }
        }
        return outputBuilder.build();
    }

    protected static void logTestCase(Logger logger, String testSuiteId, UnitTestResult testCaseResult) {
        var executionId = testCaseResult.getExecutionId();
        var url = testCaseResult.getUrl();
        var state = testCaseResult.getState();
        if (state == TestState.ERROR) {
            logger.error("{} > Test case '{}' ended with status: {}.\nexecutionId: {}\nexecution url: {}\nerrors: {}",
                testSuiteId,
                testCaseResult.getTestId(), state, executionId, url,
                formatErrors(testCaseResult)
            );
        } else {
            logger.info("{} > Test case '{}' ended with status: {}.\nexecutionId: {}\nexecution url: {}", testSuiteId, testCaseResult.getTestId(), state, executionId, url);
        }

        if (logger.isDebugEnabled()) {
            testCaseResult.getAssertionResults().forEach(assertionResult -> {
                logger.debug("{} > {} > Assertion result: {}", testSuiteId, testCaseResult.getTestId(), formatAssertionResult(assertionResult));
            });
        }
    }

    private static String formatAssertionResult(AssertionResult assertionResult) {
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
