package io.kestra.plugin.kestra.ee.cases;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.Label;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class SetTicketTest extends AbstractKestraEeContainerTest {
    protected static final String NAMESPACE = "kestra.tests.cases.setticket";

    @Inject
    TestRunContextFactory runContextFactory;

    private String createCase() throws Exception {
        CreateCase createCase = CreateCase.builder()
            .id("open_case_" + IdUtils.create())
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
            .title(Property.ofValue("Case for SetTicket test " + IdUtils.create()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, createCase, Map.of());
        return createCase.run(runContext).getCaseId();
    }

    private SetTicket setTicketTask(String caseId, String system, String key, String url) {
        var builder = SetTicket.builder()
            .id("set_ticket_" + IdUtils.create())
            .type(SetTicket.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .system(Property.ofValue(system))
            .key(Property.ofValue(key))
            .url(Property.ofValue(url));
        if (caseId != null) {
            builder.caseId(Property.ofValue(caseId));
        }
        return builder.build();
    }

    private RunContext runContext() {
        SetTicket anchor = SetTicket.builder().id("anchor_" + IdUtils.create()).type(SetTicket.class.getName()).build();
        return TestsUtils.mockRunContext(this.runContextFactory, anchor, Map.of());
    }

    @Test
    void replacesAnExistingTicketInsteadOfFailing() throws Exception {
        String caseId = createCase();

        setTicketTask(caseId, "GitHub", "acme/ops#412", "https://github.com/acme/ops/issues/412").run(runContext());
        SetTicket.Output output = setTicketTask(caseId, "Atlassian Jira", "OPS-7", "https://acme.atlassian.net/browse/OPS-7").run(runContext());

        assertThat(output.getCaseId()).isEqualTo(caseId);
    }

    @Test
    void failsWithTheCaseIdNamedWhenTheCaseDoesNotExist() {
        String missing = IdUtils.create();

        assertThatThrownBy(() -> setTicketTask(missing, "GitHub", "acme/ops#412", "https://github.com/acme/ops/issues/412").run(runContext()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(missing);
    }

    @Test
    void failsWhenNoCaseIdIsSetAndNoLabelCarriesOne() {
        assertThatThrownBy(() -> setTicketTask(null, "GitHub", "acme/ops#412", "https://github.com/acme/ops/issues/412").run(runContext()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("caseId");
    }

    @Test
    void resolvesTheCaseIdFromTheSystemCaseIdLabelWhenTheCaseIdPropertyIsUnset() throws Exception {
        String caseId = createCase();
        SetTicket task = setTicketTask(null, "GitHub", "acme/ops#412", "https://github.com/acme/ops/issues/412");

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of())
            .withLabels(List.of(new Label("system.caseId", caseId)));
        RunContext runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task));

        assertThat(task.run(runContext).getCaseId()).isEqualTo(caseId);
    }
}
