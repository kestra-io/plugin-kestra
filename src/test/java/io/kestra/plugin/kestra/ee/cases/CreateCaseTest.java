package io.kestra.plugin.kestra.ee.cases;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.CaseSeverity;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class CreateCaseTest extends AbstractKestraEeContainerTest {
    protected static final String NAMESPACE = "kestra.tests.cases.create";

    @Inject
    TestRunContextFactory runContextFactory;

    @Test
    void createsCaseAndLinksMatchingExecutionOnSecondRun() throws Exception {
        String taskId = "open_incident_" + IdUtils.create();
        String title = "Health check failed for " + IdUtils.create();

        CreateCase createCase = CreateCase.builder()
            .id(taskId)
            .type(CreateCase.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE))
            .title(Property.ofExpression("{{ inputs.title }}"))
            .severity(Property.ofValue(CaseSeverity.CRITICAL))
            .linkMatchingExecutions(Property.ofValue(true))
            .build();

        RunContext firstRunContext = TestsUtils.mockRunContext(
            this.runContextFactory, createCase, java.util.Map.of("title", title)
        );
        CreateCase.Output firstOutput = createCase.run(firstRunContext);

        assertThat(firstOutput.getCaseId()).isNotBlank();
        assertThat(firstOutput.getCreated()).isTrue();

        RunContext secondRunContext = TestsUtils.mockRunContext(
            this.runContextFactory, createCase, java.util.Map.of("title", title)
        );
        CreateCase.Output secondOutput = createCase.run(secondRunContext);

        assertThat(secondOutput.getCaseId()).isEqualTo(firstOutput.getCaseId());
        assertThat(secondOutput.getCreated()).isFalse();
    }
}
