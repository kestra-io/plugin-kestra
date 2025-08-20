
package io.kestra.plugin.tests;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.ee.tests.RunTest;
import io.kestra.sdk.model.TestState;
import io.kestra.sdk.model.TestSuiteRunResult;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
public class RunTestTest extends AbstractKestraContainerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.ee.tests.runtest";

    @Test
    public void shoudRunTest() throws Exception {
        // given
        var runContext = runContextFactory.of();
        var flow = kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        var testSuite = kestraTestDataUtils.createRandomizedTestSuite(NAMESPACE, flow.getId());

        // when
        var runTestTask = RunTest.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .testId(Property.ofValue(testSuite.getId()))
            .build();
        var runTestOutput = runTestTask.run(runContext);

        // then
        assertThat(runTestOutput).extracting(RunTest.Output::getResult).extracting(TestSuiteRunResult::getTestSuiteId).isEqualTo(testSuite.getId());
        assertThat(runTestOutput).extracting(RunTest.Output::getResult).extracting(TestSuiteRunResult::getState).isEqualTo(TestState.SUCCESS);
    }
}
