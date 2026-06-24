package io.kestra.plugin.kestra.ee.iam.bindings;

import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.BindingType;
import io.kestra.sdk.model.IAMBindingControllerApiBindingDetail;
import io.kestra.sdk.model.IAMBindingControllerApiCreateBindingRequest;
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
    title = "Create a role binding",
    description = """
        Creates a role binding for a user or group. If a binding already exists (HTTP 409), the task returns the existing binding ID unless `failIfExists` is set to `true`.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Bind a user to a role.",
            full = true,
            code = """
                id: iam_binding_set
                namespace: company.team

                tasks:
                  - id: create_binding
                    type: io.kestra.plugin.kestra.ee.iam.bindings.Set
                    subjectType: USER
                    externalId: "user-external-id"
                    roleId: "role-id"
                    namespace: "company.team"
                """
        )
    }
)
public class Set extends AbstractKestraTask implements RunnableTask<Set.Output> {

    @NotNull
    @Schema(
        title = "Binding subject type",
        description = "Whether the binding applies to a `USER` or a `GROUP`."
    )
    @PluginProperty(group = "main")
    private Property<BindingType> subjectType;

    @NotNull
    @Schema(title = "External ID of the user or group to bind")
    @PluginProperty(group = "main")
    private Property<String> externalId;

    @NotNull
    @Schema(title = "Role ID to assign")
    @PluginProperty(group = "main")
    private Property<String> roleId;

    @Schema(
        title = "Namespace scope for the binding",
        description = "If omitted, the binding applies to the whole tenant."
    )
    @PluginProperty(group = "main")
    private Property<String> namespace;

    @Builder.Default
    @Schema(
        title = "Fail if a binding already exists",
        description = "When `false` (default), the task returns the existing binding ID on conflict. When `true`, a 409 conflict throws an exception."
    )
    @PluginProperty(group = "reliability")
    private Property<Boolean> failIfExists = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rType = runContext.render(subjectType).as(BindingType.class).orElseThrow();
        var rExternalId = runContext.render(externalId).as(String.class).orElseThrow();
        var rRoleId = runContext.render(roleId).as(String.class).orElseThrow();
        var rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        var rFailIfExists = runContext.render(failIfExists).as(Boolean.class).orElse(false);

        var request = new IAMBindingControllerApiCreateBindingRequest()
            .type(rType)
            .externalId(rExternalId)
            .roleId(rRoleId);
        if (rNamespace != null) {
            request.namespaceId(rNamespace);
        }

        try {
            var created = kestraClient.bindings().createBinding(rTenant, request);
            return Output.builder().id(created.getId()).build();
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                if (rFailIfExists) {
                    throw new IllegalStateException(
                        "Binding already exists for type=" + rType + " externalId=" + rExternalId + " roleId=" + rRoleId
                            + ". Set failIfExists=false to return the existing binding ID instead.",
                        e
                    );
                }
                // Filter server-side by type to reduce candidates, then match on roleId and externalId in memory
                var typeFilter = new QueryFilter()
                    .field(QueryFilterField.TYPE)
                    .operation(QueryFilterOp.EQUALS)
                    .value(rType.getValue());
                var existing = kestraClient.bindings()
                    .searchBindings(rTenant, 1, 100, null, List.of(typeFilter))
                    .getResults().stream()
                    .filter(b -> matchesBinding(b, rType, rExternalId, rRoleId))
                    .findFirst();
                return Output.builder().id(existing.map(IAMBindingControllerApiBindingDetail::getId).orElse(null)).build();
            }
            throw e;
        }
    }

    /**
     * Returns true when the binding detail matches the specific subject (type + externalId) and role being created.
     * The externalId lives in user.id for USER bindings and in group.id for GROUP bindings.
     */
    private static boolean matchesBinding(IAMBindingControllerApiBindingDetail b, BindingType type, String externalId, String roleId) {
        if (b.getRole() == null || !roleId.equals(b.getRole().getId())) {
            return false;
        }
        if (type == BindingType.USER) {
            return b.getUser() != null && externalId.equals(b.getUser().getId());
        }
        return b.getGroup() != null && externalId.equals(b.getGroup().getId());
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "ID of the created or existing binding")
        private String id;
    }
}
