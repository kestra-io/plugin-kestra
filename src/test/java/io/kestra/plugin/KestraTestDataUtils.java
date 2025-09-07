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

    public FlowWithSource createRandomizedPauseFlow(@Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : "default";
    String flow =
        """
                id: random_flow_%s
                namespace: %s

                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: Hello from KestraSDKHelper! 🚀
                  - id: pause
                    type: io.kestra.plugin.core.flow.Pause
                """
            .formatted(UUID.randomUUID().toString().substring(0, 8).replace("-", "_"), np);

        return kestraClient.flows().createFlow(tenantId, flow);
    }

    public FlowWithSource createRandomizedFlowWithLabel(@Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String flow =
            """
                id: random_flow_%s
                namespace: %s
                labels:
                  - key: value
                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: Hello from KestraSDKHelper! 🚀
                """.formatted(UUID.randomUUID().toString().substring(0, 8).replace("-", "_"), np);

        return kestraClient.flows().createFlow(tenantId, flow);
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

    /**
     * Creates a randomized execution for a given flow ID and namespace.
     * If the namespace is null, it defaults to "default".
     * This method always throws an ApiException because the result can not be parsed, so its only used
     * for testing the Query executions task.
     *
     * @param flowId    The ID of the flow to trigger.
     * @param namespace The namespace in which to trigger the execution, or null for default.
     * @throws ApiException If there is an error triggering the execution.
     */
    public void createRandomizedExecution(String flowId, @Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : NAMESPACE;
        try {
            kestraClient.executions().triggerExecution(
                np,
                flowId,
                false,
                tenantId,
                null,
                null
            );
        } catch (ApiException e) {
            log.error("ApiException thrown, probably false positive as we are in `createRandomizedExecution` :" + e.getMessage());
        }
    }

    public void killExecution(String executionId, boolean isOnKillCascade) throws ApiException {
        try {
            kestraClient.executions().killExecution(
                executionId,
                isOnKillCascade,
                tenantId
            );
        } catch (ApiException e) {
            log.error("ApiException thrown, probably false positive as we are in `killExecution` :" + e.getMessage());
        }
    }

    public TestSuite createRandomizedTestSuite(String namespace, String flowId) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String testSuite =
            """
                id: random_testsuite_%s
                namespace: %s
                flowId: %s
                testCases:
                  - id: test_case_1
                    type: io.kestra.core.tests.flow.UnitTest
                    assertions:
                      - value: 200
                        equalTo: 200
                  - id: test_case_2
                    type: io.kestra.core.tests.flow.UnitTest
                    assertions:
                      - value: 300
                        equalTo: 300
                """.formatted(
                UUID.randomUUID().toString().substring(0, 8).replace("-", "_"),
                np,
                flowId
            );
        return kestraClient.testSuites().createTestSuite(tenantId, testSuite);
    }
    public TestSuite createRandomizedCrashingTestSuite(String namespace, String flowId) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String testSuite =
            """
                id: random_crashing_testsuite_%s
                namespace: %s
                flowId: %s
                testCases:
                  - id: test_case_1
                    type: io.kestra.core.tests.flow.UnitTest
                    assertions:
                      - value: 200
                        equalTo: 200
                  - id: test_case_2_crashing
                    type: io.kestra.core.tests.flow.UnitTest
                    assertions:
                      - value: "{{ unknown_pebble_expression_causing_error }}"
                        equalTo: 111
                """.formatted(
                UUID.randomUUID().toString().substring(0, 8).replace("-", "_"),
                np,
                flowId
            );
        return kestraClient.testSuites().createTestSuite(tenantId, testSuite);
    }
    public TestSuite createRandomizedFailingTestSuite(String namespace, String flowId) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String testSuite =
            """
                id: random_failing_testsuite_%s
                namespace: %s
                flowId: %s
                testCases:
                  - id: test_case_1
                    type: io.kestra.core.tests.flow.UnitTest
                    assertions:
                      - value: 200
                        equalTo: 200
                  - id: test_case_2_failing
                    type: io.kestra.core.tests.flow.UnitTest
                    assertions:
                      - value: 333
                        equalTo: 111
                """.formatted(
                UUID.randomUUID().toString().substring(0, 8).replace("-", "_"),
                np,
                flowId
            );
        return kestraClient.testSuites().createTestSuite(tenantId, testSuite);
    }
}
