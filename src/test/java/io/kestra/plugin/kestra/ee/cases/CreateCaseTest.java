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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void attachesToExplicitCaseIdWithOverriddenExecutionId() throws Exception {
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
            .build();

        RunContext firstRunContext = TestsUtils.mockRunContext(
            this.runContextFactory, createCase, java.util.Map.of("title", title)
        );
        CreateCase.Output firstOutput = createCase.run(firstRunContext);

        assertThat(firstOutput.getCaseId()).isNotBlank();

        String attachTaskId = "attach_" + IdUtils.create();
        CreateCase attachToCase = CreateCase.builder()
            .id(attachTaskId)
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
            .caseId(Property.ofValue(firstOutput.getCaseId()))
            .executionId(Property.ofExpression("{{ inputs.execId }}"))
            .build();

        RunContext secondRunContext = TestsUtils.mockRunContext(
            this.runContextFactory, attachToCase, java.util.Map.of("title", title, "execId", IdUtils.create())
        );
        CreateCase.Output secondOutput = attachToCase.run(secondRunContext);

        assertThat(secondOutput.getCaseId()).isEqualTo(firstOutput.getCaseId());
        assertThat(secondOutput.getCreated()).isFalse();
    }

    @Test
    void attachesToExplicitCaseIdWithoutTitle() throws Exception {
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
            .build();

        RunContext firstRunContext = TestsUtils.mockRunContext(
            this.runContextFactory, createCase, java.util.Map.of("title", title)
        );
        CreateCase.Output firstOutput = createCase.run(firstRunContext);

        String attachTaskId = "attach_" + IdUtils.create();
        CreateCase attachToCase = CreateCase.builder()
            .id(attachTaskId)
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
            .caseId(Property.ofValue(firstOutput.getCaseId()))
            .executionId(Property.ofExpression("{{ inputs.execId }}"))
            .build();

        RunContext secondRunContext = TestsUtils.mockRunContext(
            this.runContextFactory, attachToCase, java.util.Map.of("execId", IdUtils.create())
        );
        CreateCase.Output secondOutput = attachToCase.run(secondRunContext);

        assertThat(secondOutput.getCaseId()).isEqualTo(firstOutput.getCaseId());
        assertThat(secondOutput.getCreated()).isFalse();
    }

    @Test
    void throwsWhenTitleMissingAndNotAttaching() {
        String taskId = "open_incident_" + IdUtils.create();

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
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, createCase, java.util.Map.of());

        assertThatThrownBy(() -> createCase.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("title is required");
    }

    @Test
    void throwsWhenExecutionIdOverrideResolvesBlank() throws Exception {
        String taskId = "attach_" + IdUtils.create();

        CreateCase attachToCase = CreateCase.builder()
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
            .title(Property.ofValue("irrelevant"))
            .caseId(Property.ofValue(IdUtils.create()))
            .executionId(Property.ofValue(""))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, attachToCase, java.util.Map.of());

        assertThatThrownBy(() -> attachToCase.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("executionId resolved to blank");
    }
}
