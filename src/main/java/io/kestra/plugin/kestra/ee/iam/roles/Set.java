package io.kestra.plugin.kestra.ee.iam.roles;

import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.IAMRoleControllerApiRoleCreateOrUpdateRequest;
import io.kestra.sdk.model.IAMRoleControllerApiRoleCreateOrUpdateRequestPermissions;
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
    title = "Create or update a role",
    description = """
        Upserts a role by name: searches for an existing role with the given name and updates it if found, or creates a new one otherwise.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Upsert a role.",
            full = true,
            code = """
                id: iam_role_set
                namespace: company.team

                tasks:
                  - id: upsert_role
                    type: io.kestra.plugin.kestra.ee.iam.roles.Set
                    name: "data-engineer-role"
                    roleDescription: "Role for data engineers"
                    permissions:
                      FLOW:
                        - READ
                        - CREATE
                      EXECUTION:
                        - READ
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<Set.Output> {

    @NotNull
    @Schema(title = "Role name")
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(title = "Role description")
    @PluginProperty(group = "main")
    private Property<String> roleDescription;

    @Schema(title = "Whether this role is the default role for new users")
    @PluginProperty(group = "main")
    private Property<Boolean> isDefault;

    @NotNull
    @Schema(
        title = "Role permissions",
        description = """
            Map of resource type to list of allowed actions, e.g. `{"FLOW": ["READ", "CREATE"], "EXECUTION": ["READ"]}`.
            Valid resource types include: FLOW, BLUEPRINT, NAMESPACE, EXECUTION, USER, GROUP, ROLE, BINDING, AUDITLOG, SECRET, KVSTORE, SETTING, APP, ASSET, TEST, DASHBOARD, SERVICEACCOUNT, and others.
            """
    )
    @PluginProperty(group = "main")
    private Property<Map<String, List<String>>> permissions;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var rDescription = runContext.render(roleDescription).as(String.class).orElse(null);
        var rIsDefault = runContext.render(isDefault).as(Boolean.class).orElse(null);
        var rPermissionsMap = runContext.render(permissions).asMap(String.class, Object.class);
        var rPermissions = JacksonMapper.ofJson().convertValue(rPermissionsMap, IAMRoleControllerApiRoleCreateOrUpdateRequestPermissions.class);

        var existing = kestraClient.roles().searchRoles(
            rTenant, 1, 100, null, null,
            List.of(new QueryFilter().field(QueryFilterField.NAME).operation(QueryFilterOp.EQUALS).value(rName))
        ).getResults();

        var request = new IAMRoleControllerApiRoleCreateOrUpdateRequest()
            .name(rName)
            .description(rDescription)
            .isDefault(rIsDefault)
            .permissions(rPermissions);

        String roleId;
        if (!existing.isEmpty()) {
            var existingRole = existing.getFirst();
            roleId = existingRole.getId();
            kestraClient.roles().updateRole(roleId, rTenant, request);
        } else {
            var created = kestraClient.roles().createRole(rTenant, request);
            roleId = created.getId();
        }

        return Output.builder().id(roleId).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "ID of the created or updated role")
        private String id;
    }
}
