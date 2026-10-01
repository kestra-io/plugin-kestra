package io.kestra.plugin.kestra.ee.cases;

import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.CasesControllerCaseTicketRequest;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Record an external ticket on a case",
    description = """
        Attaches a ticket from an external ticketing system (GitHub, Jira, ServiceNow, ...) to a case, so it shows up \
        on the case detail page. Calling this task again on the same case replaces the previously recorded ticket.

        Jira is not yet usable end-to-end: `io.kestra.plugin.jira.issues.Create` returns no outputs, so the issue key \
        and URL cannot be read back. See https://github.com/kestra-io/plugin-jira/issues/101."""
)
@Plugin(
    examples = {
        @Example(
            title = "Attach an existing ticket from a flow run as a case action. `caseId` is omitted: it comes from the `system.caseId` label Kestra sets on any execution started from a case.",
            full = true,
            code = """
                id: link_existing_ticket
                namespace: system

                inputs:
                  - id: ticket_key
                    type: STRING
                  - id: ticket_url
                    type: URI

                tasks:
                  - id: link
                    type: io.kestra.plugin.kestra.ee.cases.SetTicket
                    system: GitHub
                    key: "{{ inputs.ticket_key }}"
                    url: "{{ inputs.ticket_url }}"
                """
        ),
        @Example(
            title = "Open a case, open a GitHub issue, and record the issue on the case.",
            full = true,
            code = """
                id: payments_api_health
                namespace: company.team

                tasks:
                  - id: probe
                    type: io.kestra.plugin.core.http.Request
                    uri: https://api.example.com/payments/health

                errors:
                  - id: open_case
                    type: io.kestra.plugin.kestra.ee.cases.CreateCase
                    title: "Payments API health check failed"
                    severity: CRITICAL
                    linkMatchingExecutions: true

                  - id: open_issue
                    type: io.kestra.plugin.github.issues.Create
                    runIf: "{{ outputs.open_case.created }}"
                    oauthToken: "{{ secret('GITHUB_TOKEN') }}"
                    repository: acme/ops
                    title: "Payments API health check failed"
                    body: "Kestra case: {{ outputs.open_case.url }}"

                  - id: link_ticket
                    type: io.kestra.plugin.kestra.ee.cases.SetTicket
                    runIf: "{{ outputs.open_case.created }}"
                    caseId: "{{ outputs.open_case.caseId }}"
                    system: GitHub
                    key: "acme/ops#{{ outputs.open_issue.issueNumber }}"
                    url: "{{ outputs.open_issue.issueUrl }}"
                """
        ),
        @Example(
            title = "Open a ServiceNow incident and record it on the case. `servicenow.Post` returns only the raw record, so the key comes from `result.number` and the URL is assembled from the same domain the task was given.",
            full = true,
            code = """
                id: open_incident_ticket
                namespace: system

                variables:
                  servicenow_domain: acme

                tasks:
                  - id: open_incident
                    type: io.kestra.plugin.servicenow.Post
                    domain: "{{ vars.servicenow_domain }}"
                    username: "{{ secret('SNOW_USERNAME') }}"
                    password: "{{ secret('SNOW_PASSWORD') }}"
                    table: incident
                    data:
                      short_description: "Payments API health check failed"
                      description: "Tracked in Kestra: {{ labels.system.caseURL }}"

                  - id: link_ticket
                    type: io.kestra.plugin.kestra.ee.cases.SetTicket
                    system: ServiceNow
                    key: "{{ outputs.open_incident.result.number }}"
                    url: "https://{{ vars.servicenow_domain }}.service-now.com/nav_to.do?uri=incident.do?sys_id={{ outputs.open_incident.result.sys_id }}"
                """
        )
    }
)
public class SetTicket extends AbstractKestraTask implements RunnableTask<SetTicket.Output> {

    @Schema(
        title = "Id of the case to set the ticket on",
        description = "Defaults to the `system.caseId` label, which Kestra sets on any execution started from a case, so this can be omitted in a flow attached to a case as an action."
    )
    @PluginProperty(group = "source")
    private Property<String> caseId;

    @NotNull
    @Schema(title = "Name of the ticketing system", description = "A free-text display name, e.g. `GitHub`, `Atlassian Jira` or `ServiceNow`. Stored verbatim.")
    @PluginProperty(group = "main")
    private Property<String> system;

    @NotNull
    @Schema(title = "Ticket key", description = "What is displayed on the case and matched by the Cases search box, e.g. `acme/ops#412`.")
    @PluginProperty(group = "main")
    private Property<String> key;

    @NotNull
    @Schema(title = "Ticket URL", description = "Where a click on the ticket goes.")
    @PluginProperty(group = "main")
    private Property<String> url;

    @Override
    public Output run(RunContext runContext) throws Exception {
        RunContext.FlowInfo flowInfo = runContext.flowInfo();

        String rTenantId = runContext.render(tenantId).as(String.class).orElse(flowInfo.tenantId());
        String rCaseId = runContext.render(caseId).as(String.class).orElseGet(() -> caseIdFromLabel(runContext));
        if (rCaseId == null || rCaseId.isBlank()) {
            throw new IllegalArgumentException(
                "No caseId was set and the execution carries no 'system.caseId' label. Set the caseId property, or run this task from a flow started from a case."
            );
        }

        CasesControllerCaseTicketRequest request = new CasesControllerCaseTicketRequest()
            .system(required(runContext, system, "system"))
            .key(required(runContext, key, "key"))
            .url(required(runContext, url, "url"));

        try {
            kestraClient(runContext).cases().setTicket(rCaseId, rTenantId, request);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                throw new IllegalArgumentException(caseNotFoundOrEndpointMissingMessage(rTenantId, rCaseId), e);
            }
            throw e;
        }

        return Output.builder().caseId(rCaseId).build();
    }

    private static String required(RunContext runContext, Property<String> property, String name) throws IllegalVariableEvaluationException {
        return runContext.render(property).as(String.class)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("Property '%s' is required and rendered to an empty value.".formatted(name)));
    }

    String caseIdFromLabel(RunContext runContext) {
        Object labels = runContext.getVariables().get("labels");
        if (labels instanceof Map<?, ?> labelsMap && labelsMap.get("system") instanceof Map<?, ?> systemMap) {
            Object value = systemMap.get("caseId");
            return value != null ? value.toString() : null;
        }
        return null;
    }

    static String caseNotFoundOrEndpointMissingMessage(String tenantId, String caseId) {
        return "PUT /api/v1/%s/cases/%s/ticket returned 404: either case '%s' does not exist in tenant '%s', or this instance does not expose the case-ticket endpoint (Kestra EE only)."
            .formatted(tenantId, caseId, caseId, tenantId);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The id of the case the ticket was set on", description = "Echoes the resolved case id, including when it came from the `system.caseId` label.")
        private String caseId;
    }
}
