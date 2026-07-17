package io.kestra.plugin.kestra.ee.iam.groups;

import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.IAMGroupControllerApiCreateGroupRequest;
import io.kestra.sdk.model.IAMGroupControllerApiUpdateGroupRequest;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;

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
    title = "Create or update a group",
    description = """
        Upserts a group by name: searches for an existing group with the given name and updates it if found, or creates a new one otherwise.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Upsert a group.",
            full = true,
            code = """
                id: iam_group_set
                namespace: company.team

                tasks:
                  - id: upsert_group
                    type: io.kestra.plugin.kestra.ee.iam.groups.Set
                    name: "data-engineers"
                    groupDescription: "Group for the data engineering team"
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<Set.Output> {

    @NotNull
    @Schema(title = "Group name")
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(title = "Group description")
    @PluginProperty(group = "main")
    private Property<String> groupDescription;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var rDescription = runContext.render(groupDescription).as(String.class).orElse(null);

        var existing = kestraClient.groups().searchGroups(
            rTenant, 1, 100, null, null,
            List.of(new QueryFilter().field(QueryFilterField.NAME).operation(QueryFilterOp.EQUALS).value(rName))
        ).getResults();

        String groupId;
        if (!existing.isEmpty()) {
            var existingGroup = existing.getFirst();
            groupId = existingGroup.getId();
            kestraClient.groups().updateGroup(
                groupId, rTenant,
                new IAMGroupControllerApiUpdateGroupRequest().name(rName).description(rDescription)
            );
        } else {
            var created = kestraClient.groups().createGroup(
                rTenant,
                new IAMGroupControllerApiCreateGroupRequest().name(rName).description(rDescription)
            );
            groupId = created.getId();
        }

        return Output.builder().id(groupId).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "ID of the created or updated group")
        private String id;
    }
}
