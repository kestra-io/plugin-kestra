package io.kestra.plugin.kestra.ee.cases;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.kestra.core.models.Label;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.ListOrMapOfLabelDeserializer;
import io.kestra.core.serializers.ListOrMapOfLabelSerializer;
import io.kestra.core.validations.NoSystemLabelValidation;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.CaseAction;
import io.kestra.sdk.model.CaseSeverity;
import io.kestra.sdk.model.CaseStatus;
import io.kestra.sdk.model.CasesControllerCaseFromTaskRequest;
import io.kestra.sdk.model.SlaConfig;
import io.kestra.sdk.model.Subjects;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
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
    title = "Open an incident-management Case, or attach the current execution to an already-open matching one",
    description = """
        Creates a new Case in Kestra's incident-management feature, typically from an `errors:` task so an incident \
        is opened whenever a flow fails. The execution that triggered the task is automatically linked to the case.

        When `linkMatchingExecutions` is `true`, the task first looks for an active (non-resolved, non-cancelled) \
        case previously created by this same task (same flow + task id); if one is found, the triggering execution \
        is attached to it instead of creating a new case."""
)
@Plugin(
    examples = {
        @Example(
            title = "Open a case when a flow fails.",
            full = true,
            code = """
                id: health_check
                namespace: company.team

                tasks:
                  - id: check
                    type: io.kestra.plugin.core.http.Request
                    uri: https://example.com/health

                errors:
                  - id: open_incident
                    type: io.kestra.plugin.kestra.ee.cases.CreateCase
                    title: "{{ execution.id }} failed for {{ flow.id }}"
                    caseDescription: "Health check failed."
                    severity: CRITICAL
                    sla:
                      acknowledgement: PT1H
                      resolution: PT8H
                    linkMatchingExecutions: true
                    assignees:
                      users:
                        - a@b.c
                      groups:
                        - Admins
                """
        )
    }
)
public class CreateCase extends AbstractKestraTask implements RunnableTask<CreateCase.Output> {

    @Schema(
        title = "Namespace the case belongs to",
        description = "Defaults to the flow's namespace."
    )
    @PluginProperty(group = "source")
    private Property<String> namespace;

    @Schema(title = "Case title", description = "Required unless `caseId` is set (attaching to an existing case doesn't need a new title).")
    @PluginProperty(group = "main")
    private Property<String> title;

    @Schema(title = "Case description", description = "Supports Markdown.")
    @PluginProperty(group = "main")
    private Property<String> caseDescription;

    @Schema(title = "Case severity", description = "Defaults to `MEDIUM` if not set.")
    @PluginProperty(group = "main")
    private Property<CaseSeverity> severity;

    @Schema(title = "Initial case status", description = "Defaults to `OPEN`.")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<CaseStatus> status = Property.ofValue(CaseStatus.OPEN);

    @Schema(title = "SLA targets for the case")
    @PluginProperty(group = "reliability")
    private SlaProperty sla;

    @Schema(
        title = "Attach to a matching open case instead of creating a new one",
        description = "When `true`, looks for an active (non-resolved, non-cancelled) case previously created by this same task and attaches the triggering execution to it instead of creating a new case."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> linkMatchingExecutions = Property.ofValue(false);

    @Schema(
        title = "Attach to an existing case instead of creating one",
        description = "When set, the triggering execution is attached to this case id instead of creating a new case. Used by the auto-generated \"keep attaching matching executions\" system flow."
    )
    @PluginProperty(group = "source")
    private Property<String> caseId;

    @Schema(
        title = "The execution id to link",
        description = "Defaults to the current task run's own triggering execution. Set this to `{{ trigger.executionId }}` when this task runs inside a Flow-trigger-driven system flow (e.g. the auto-generated \"keep attaching matching executions\" flow), so the upstream triggering execution is linked instead of the system flow's own execution."
    )
    @PluginProperty(group = "source")
    private Property<String> executionId;

    @Schema(title = "Assignees")
    @PluginProperty(group = "advanced")
    private SubjectsProperty assignees;

    @Schema(title = "Watchers")
    @PluginProperty(group = "advanced")
    private SubjectsProperty watchers;

    @Schema(title = "Asset IDs to link to the case")
    @PluginProperty(group = "advanced")
    private Property<List<String>> assetIds;

    @Schema(
        title = "Labels to attach to the case",
        description = "Labels as a list of Label (key/value pairs) or as a map of string to string.",
        implementation = Object.class,
        oneOf = {
            Label[].class,
            Map.class
        }
    )
    @PluginProperty(group = "advanced")
    @JsonSerialize(using = ListOrMapOfLabelSerializer.class)
    @JsonDeserialize(using = ListOrMapOfLabelDeserializer.class)
    private List<@NoSystemLabelValidation Label> labels;

    @Schema(
        title = "Actions attached to the case",
        description = "System flows runnable directly from the case's detail page."
    )
    @PluginProperty(group = "advanced")
    private List<CaseActionProperty> actions;

    @Override
    public Output run(RunContext runContext) throws Exception {
        RunContext.FlowInfo flowInfo = runContext.flowInfo();
        RunContext.TaskRunInfo taskRunInfo = runContext.taskRunInfo();

        String rTenantId = runContext.render(tenantId).as(String.class).orElse(flowInfo.tenantId());
        String rNamespace = runContext.render(namespace).as(String.class).orElse(flowInfo.namespace());
        String rTitle = runContext.render(title).as(String.class).orElse(null);
        String rDescription = runContext.render(caseDescription).as(String.class).orElse(null);
        String rCaseId = runContext.render(caseId).as(String.class).orElse(null);
        if (rCaseId == null && (rTitle == null || rTitle.isBlank())) {
            throw new IllegalArgumentException("title is required when creating a new case (omit it only when caseId is set to attach to an existing case)");
        }
        CaseSeverity rSeverity = runContext.render(severity).as(CaseSeverity.class).orElse(null);
        CaseStatus rStatus = runContext.render(status).as(CaseStatus.class).orElse(CaseStatus.OPEN);
        boolean rLinkMatchingExecutions = runContext.render(linkMatchingExecutions).as(Boolean.class).orElse(false);
        SlaConfig rSla = renderSla(runContext);
        Subjects rAssignees = renderSubjects(runContext, assignees);
        Subjects rWatchers = renderSubjects(runContext, watchers);
        List<String> rAssetIds = assetIds != null ? runContext.render(assetIds).asList(String.class) : List.of();
        List<io.kestra.sdk.model.Label> rLabels = renderLabels(runContext);
        List<CaseAction> rActions = renderActions(runContext);

        String rExecutionId;
        if (this.executionId != null) {
            rExecutionId = runContext.render(this.executionId).as(String.class).orElse(null);
            if (rExecutionId == null || rExecutionId.isBlank()) {
                throw new IllegalArgumentException("executionId resolved to blank — this task is likely running as a manually-triggered system flow with no upstream trigger execution");
            }
        } else {
            rExecutionId = taskRunInfo.executionId();
        }
        String executionState = currentExecutionState(runContext);

        CasesControllerCaseFromTaskRequest request = new CasesControllerCaseFromTaskRequest()
            .namespace(rNamespace)
            .title(rTitle)
            .description(rDescription)
            .severity(rSeverity)
            .status(rStatus)
            .sla(rSla)
            .assignees(rAssignees)
            .watchers(rWatchers)
            .labels(rLabels.isEmpty() ? null : rLabels)
            .actions(rActions.isEmpty() ? null : rActions)
            .assetIds(rAssetIds.isEmpty() ? null : rAssetIds)
            .linkMatchingExecutions(rLinkMatchingExecutions)
            .flowNamespace(flowInfo.namespace())
            .flowId(flowInfo.id())
            .taskId(this.id)
            .executionId(rExecutionId)
            .executionState(executionState)
            .caseId(rCaseId);

        Map<String, Object> result = kestraClient(runContext).cases().createFromTask(rTenantId, request);
        if (result == null) {
            throw new IllegalStateException("createFromTask returned no result");
        }

        Object caseIdValue = result.get("caseId");
        if (!(caseIdValue instanceof String resultCaseId)) {
            throw new IllegalStateException("createFromTask response is missing a string 'caseId' field, got: " + caseIdValue);
        }
        Object createdValue = result.get("created");
        if (!(createdValue instanceof Boolean resultCreated)) {
            throw new IllegalStateException("createFromTask response is missing a boolean 'created' field, got: " + createdValue);
        }

        return Output.builder()
            .caseId(resultCaseId)
            .created(resultCreated)
            .build();
    }

    private SlaConfig renderSla(RunContext runContext) throws Exception {
        if (sla == null) {
            return null;
        }

        Duration acknowledgement = sla.getAcknowledgement() != null
            ? runContext.render(sla.getAcknowledgement()).as(Duration.class).orElse(null)
            : null;
        Duration resolution = sla.getResolution() != null
            ? runContext.render(sla.getResolution()).as(Duration.class).orElse(null)
            : null;

        if (acknowledgement == null && resolution == null) {
            return null;
        }
        return new SlaConfig()
            .acknowledgement(acknowledgement != null ? acknowledgement.toString() : null)
            .resolution(resolution != null ? resolution.toString() : null);
    }

    private Subjects renderSubjects(RunContext runContext, SubjectsProperty subjects) throws Exception {
        if (subjects == null) {
            return null;
        }

        List<String> users = subjects.getUsers() != null ? runContext.render(subjects.getUsers()).asList(String.class) : List.of();
        List<String> groups = subjects.getGroups() != null ? runContext.render(subjects.getGroups()).asList(String.class) : List.of();
        return new Subjects().users(users).groups(groups);
    }

    private List<io.kestra.sdk.model.Label> renderLabels(RunContext runContext) throws Exception {
        if (labels == null) {
            return List.of();
        }

        List<io.kestra.sdk.model.Label> rendered = new ArrayList<>();
        for (Label label : labels) {
            rendered.add(new io.kestra.sdk.model.Label()
                .key(runContext.render(label.key()))
                .value(runContext.render(label.value())));
        }
        return rendered;
    }

    private List<CaseAction> renderActions(RunContext runContext) throws Exception {
        if (actions == null) {
            return List.of();
        }

        List<CaseAction> rendered = new ArrayList<>();
        for (CaseActionProperty action : actions) {
            rendered.add(new CaseAction()
                .label(runContext.render(action.getLabel()).as(String.class).orElse(null))
                .namespace(runContext.render(action.getNamespace()).as(String.class).orElse(null))
                .flowId(runContext.render(action.getFlowId()).as(String.class).orElse(null)));
        }
        return rendered;
    }

    @SuppressWarnings("unchecked")
    private String currentExecutionState(RunContext runContext) {
        Object executionVar = runContext.getVariables().get("execution");
        if (executionVar instanceof Map<?, ?> executionMap) {
            Object state = executionMap.get("state");
            return state != null ? state.toString() : null;
        }
        return null;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @ToString
    public static class SubjectsProperty {
        @Schema(title = "User emails")
        @PluginProperty(group = "advanced")
        private Property<List<String>> users;

        @Schema(title = "Group names")
        @PluginProperty(group = "advanced")
        private Property<List<String>> groups;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @ToString
    public static class SlaProperty {
        @Schema(title = "Time to acknowledge, counted from case creation")
        @PluginProperty(group = "reliability")
        private Property<Duration> acknowledgement;

        @Schema(title = "Time to resolve, counted from case creation")
        @PluginProperty(group = "reliability")
        private Property<Duration> resolution;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @ToString
    public static class CaseActionProperty {
        @Schema(title = "Display label for the action")
        @PluginProperty(group = "advanced")
        private Property<String> label;

        @Schema(title = "Namespace of the flow to run")
        @PluginProperty(group = "advanced")
        private Property<String> namespace;

        @Schema(title = "Id of the flow to run")
        @PluginProperty(group = "advanced")
        private Property<String> flowId;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The id of the case that was created or attached to")
        private String caseId;

        @Schema(title = "Whether a new case was created", description = "`false` if the triggering execution was attached to an existing matching case instead.")
        private Boolean created;
    }
}
