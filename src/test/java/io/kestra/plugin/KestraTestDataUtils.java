package io.kestra.plugin;

import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.*;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j; // Added for logging

import java.util.Random;
import java.util.UUID;

/**
 * A helper class for Kestra SDK operations for test setup and data generation.
 * This class manages its own KestraClient instance based on provided connection details.
 */
@Slf4j
public class KestraTestDataUtils {
    private final String NAMESPACE = "default";
    private final String tenantId;
    @Getter
    private final KestraClient kestraClient;
    private static final Random random = new Random();

    public KestraTestDataUtils(String kestraUrl, String username, String password, String tenantId) {
        this.tenantId = tenantId;

        var builder = KestraClient.builder();
        builder.url(kestraUrl);
        builder.basicAuth(username, password);
        this.kestraClient = builder.build();
        log.debug("KestraSDKHelper initialized with URL: {} and tenant ID: {}", kestraUrl, tenantId);
    }

    private Role createRandomizedRole() throws ApiException {
        RolePermissions permissions = new RolePermissions()
            .addFLOWItem(String.valueOf(Action.CREATE));

        String name = "Role-" + random.nextInt(10000);
        Role role = new Role()
            .name(name)
            .permissions(permissions);

        return kestraClient.roles().createRole(tenantId, role);
    }

    public Group createRandomizedGroup() throws ApiException {
        String name = "Group-" + random.nextInt(10000);
        AbstractGroupControllerGroupWithMembers group = new AbstractGroupControllerGroupWithMembers()
            .name(name);

        return kestraClient.groups().createGroup(tenantId, group);
    }

    public AbstractBindingControllerBindingDetail createGroupBinding(String groupId, String roleId) throws ApiException {
        Binding binding = new Binding()
            .type(BindingType.GROUP)
            .externalId(groupId)
            .roleId(roleId);

        return kestraClient.bindings().createBinding(tenantId, binding);
    }

    public FlowWithSource createRandomizedFlow(@Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String flow =
            """
                id: random_flow_%s
                namespace: %s

                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: Hello from KestraSDKHelper! 🚀
                """.formatted(UUID.randomUUID().toString().substring(0, 8).replace("-", "_"), np);

        return kestraClient.flows().createFlow(tenantId, flow);
    }

    public Invitation createRandomizedInvitation(@Nullable String email) throws ApiException {

        return kestraClient.invitations().createInvitation(
            tenantId,
            new Invitation().email(email != null ? email : "random" + random.nextInt(1000000) + "@random.com") // Increased range for email
        );
    }

    public Namespace createRandomizedNamespace(@Nullable String namespaceId) throws ApiException {
        String nId = "namespace-" + UUID.randomUUID().toString().substring(0, 8).replace("-", "_");
        Namespace namespace = new Namespace().id(namespaceId != null ? namespaceId : nId);

        return kestraClient.namespaces().createNamespace(tenantId, namespace);
    }

    public void createRandomizedKVEntry(@Nullable String key, @Nullable String value, @Nullable String namespace) throws ApiException {
        kestraClient.kv().setKeyValue(
            namespace != null ? namespace : NAMESPACE,
            key != null ? key : "key-" + UUID.randomUUID().toString().substring(0, 8).replace("-", "_"),
            tenantId,
            value != null ? value : "value-" + random.nextInt(10000)
        );
    }

    public ExecutionControllerExecutionResponse createRandomizedExecution(String flowId, @Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : NAMESPACE;

        return kestraClient.executions().createExecution(
            np,
            flowId,
            false,
            tenantId,
            null,
            null,
            null
        ).getFirst();
    }

}
