package io.kestra.plugin.kestra.ee.reusableinputs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete reusable inputs",
    description = """
        Deletes a set of reusable inputs from a namespace. A set inherited from a parent namespace can only be deleted in its own namespace.
        Enterprise Edition only."""
)
@Plugin(
    examples = {
        @Example(
            title = "Delete the reusable inputs of a namespace.",
            full = true,
            code = """
                id: reusable_inputs_delete
                namespace: company.team

                tasks:
                  - id: delete_bom
                    type: io.kestra.plugin.kestra.ee.reusableinputs.Delete
                    reusableInputsId: bom
                """
        )
    }
)
public class Delete extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Namespace of the reusable inputs",
        description = "Defaults to the namespace of the flow."
    )
    @PluginProperty(group = "main")
    private Property<String> namespace;

    @NotNull
    @Schema(
        title = "ID of the reusable inputs to delete"
    )
    @PluginProperty(group = "main")
    private Property<String> reusableInputsId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rNamespace = runContext.render(namespace).as(String.class).orElseGet(() -> runContext.flowInfo().namespace());

        kestraClient(runContext).reusableInputs().deleteReusableInputs(
            rNamespace,
            runContext.render(reusableInputsId).as(String.class).orElseThrow(),
            rTenant
        );

        return null;
    }
}
