package io.kestra.plugin.kestra.ee.reusableinputs;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.api.ReusableInputsApi;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.ReusableInputsWithSource;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
@Schema(
    title = "Create or update reusable inputs",
    description = """
        Creates a set of reusable inputs, or saves a new revision of an existing one. With `inputs`, every input of the set is replaced. \
        With `defaults`, only the defaults of existing inputs are changed, keyed by input id (`form.child` for an input of a `FORM`), \
        and everything else in the set is kept. The default of a `TABLE` input can be its rows, or the URI of a CSV or ION file holding them, \
        e.g. a `FILE` input: a CSV is read by its header, with one cell per header on every row, separated by commas, semicolons or tabs. \
        Each text cell is converted to the type of its column, and an empty cell is left empty. The task fails on a header or key that isn't a column.

        The task edits the revision executions use. With `draft`, it saves a draft instead, which executions skip and which may be invalid, \
        and edits the latest revision, drafts included. The source is rewritten from its content, so comments in it are not kept.

        The set is validated the same way as a save from the editor, and the task fails on an invalid set with the same message. \
        The new revision appears in Revisions and in audit logs under the user or service account the task is authenticated with.
        Enterprise Edition only."""
)
@Plugin(
    examples = {
        @Example(
            title = "Create or replace the inputs of a set.",
            full = true,
            code = """
                id: reusable_inputs_set
                namespace: company.team

                tasks:
                  - id: set_bom
                    type: io.kestra.plugin.kestra.ee.reusableinputs.Set
                    reusableInputsId: bom
                    reusableInputsDescription: Bill of materials of the application
                    inputs:
                      - id: environment
                        type: SELECT
                        values: [dev, prod]
                        defaults: dev
                      - id: cpus
                        type: INT
                        min: 1
                        defaults: 2
                """
        ),
        @Example(
            title = "Change only the defaults of an existing set.",
            full = true,
            code = """
                id: reusable_inputs_defaults
                namespace: company.team

                tasks:
                  - id: update_defaults
                    type: io.kestra.plugin.kestra.ee.reusableinputs.Set
                    reusableInputsId: bom
                    defaults:
                      environment: prod
                      cpus: 4
                """
        ),
        @Example(
            title = "Replace the rows of a TABLE input with the rows of a CSV file.",
            full = true,
            code = """
                id: reusable_inputs_import_csv
                namespace: company.team

                inputs:
                  - id: servers
                    type: FILE

                tasks:
                  - id: set_servers
                    type: io.kestra.plugin.kestra.ee.reusableinputs.Set
                    reusableInputsId: bom
                    defaults:
                      servers: "{{ inputs.servers }}"
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<Set.Output> {
    @Schema(
        title = "Namespace of the reusable inputs",
        description = "Defaults to the namespace of the flow."
    )
    @PluginProperty(group = "main")
    private Property<String> namespace;

    @NotNull
    @Schema(
        title = "ID of the reusable inputs"
    )
    @PluginProperty(group = "main")
    private Property<String> reusableInputsId;

    @Schema(
        title = "Description of the reusable inputs"
    )
    @PluginProperty(group = "main")
    private Property<String> reusableInputsDescription;

    @Schema(
        title = "Inputs of the set",
        description = """
            Each entry is an input declaration, as in a flow. When set, every input of the set is replaced. Required to create a set.
            The declarations are saved as written: an expression in them, such as a `SELECT` `expression` or a `defaults` with `{{ now() }}`, \
            is evaluated by the executions using the set, not by this task. A single expression returning the whole list is evaluated."""
    )
    @PluginProperty(group = "main")
    private Property<List<Map<String, Object>>> inputs;

    @Schema(
        title = "Defaults to change, keyed by input id",
        description = """
            Only the defaults of existing inputs are changed; use `form.child` for an input of a `FORM`. The task fails on an id the set doesn't have.
            Values are evaluated by this task, so that they can come from outputs; to save an expression as the default, wrap it in `{% raw %}`."""
    )
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> defaults;

    @Schema(
        title = "Save the revision as a draft",
        description = "A draft is skipped by executions and may be invalid."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> draft = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rNamespace = runContext.render(namespace).as(String.class).orElseGet(() -> runContext.flowInfo().namespace());
        String rId = runContext.render(reusableInputsId).as(String.class).orElseThrow();
        List<Map<String, Object>> rInputs = inputs == null ? null : declarations(runContext);
        Map<String, Object> rDefaults = defaults == null ? null : runContext.render(defaults).asMap(String.class, Object.class);
        boolean rDraft = runContext.render(draft).as(Boolean.class).orElse(false);

        ReusableInputsApi api = kestraClient(runContext).reusableInputs();
        String source = ReusableInputsSources.apply(
            currentSource(api, rTenant, rNamespace, rId, rDraft),
            rNamespace,
            rId,
            runContext.render(reusableInputsDescription).as(String.class).orElse(null),
            rInputs,
            rDefaults,
            runContext.storage()::getFile
        );

        ReusableInputsWithSource saved;
        try {
            saved = api.createOrUpdateReusableInputs(rNamespace, rId, rTenant, source, false, rDraft);
        } catch (ApiException e) {
            throw new IllegalArgumentException(
                "Reusable inputs '%s' could not be saved in namespace '%s': %s".formatted(rId, rNamespace, ReusableInputsSources.errorMessage(e)),
                e
            );
        }

        return Output.builder()
            .namespace(saved.getNamespace())
            .id(saved.getId())
            .revision(saved.getRevision())
            .draft(Boolean.TRUE.equals(saved.getDraft()))
            .build();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<Map<String, Object>> declarations(RunContext runContext) throws IllegalVariableEvaluationException {
        Optional<List<Map>> asWritten = Property.asStaticList((Property) inputs, Map.class);
        return asWritten.isPresent() ? (List) asWritten.get() : (List) runContext.render(inputs).asList(Map.class);
    }

    /**
     * A set inherited from a parent namespace is not edited from here: the task works on the exact namespace.
     * A set that only has drafts has no revision executions use, so its latest draft is edited.
     */
    @Nullable
    private static String currentSource(ReusableInputsApi api, String tenant, String namespace, String id, boolean draft) {
        try {
            ReusableInputsWithSource latest = api.reusableInputs(namespace, id, tenant, null, draft);
            return namespace.equals(latest.getNamespace()) ? latest.getSource() : null;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return null;
            }
            if (e.getCode() == 422 && !draft) {
                return currentSource(api, tenant, namespace, id, true);
            }
            throw new IllegalArgumentException(
                "Reusable inputs '%s' could not be read in namespace '%s': %s".formatted(id, namespace, ReusableInputsSources.errorMessage(e)),
                e
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Namespace of the reusable inputs")
        private final String namespace;

        @Schema(title = "ID of the reusable inputs")
        private final String id;

        @Schema(title = "Revision saved by the task")
        private final Integer revision;

        @Schema(title = "Whether the revision was saved as a draft")
        private final Boolean draft;
    }
}
