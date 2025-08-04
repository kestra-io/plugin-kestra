package io.kestra.plugin;

import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.Tenant;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

@Testcontainers
@Slf4j
public class AbstractKestraContainerTest {
    protected static final String USERNAME = "admin@admin.com";
    protected static final String PASSWORD = "Root!1234";
    protected static final String TENANT_ID = "main";
    protected static String KESTRA_URL;

    protected static KestraTestDataUtils kestraTestDataUtils;

    @Container
    protected static GenericContainer<?> kestraContainer =
        new GenericContainer<>(
            DockerImageName.parse("ghcr.io/kestra-io/kestra-ee:develop"))
            .withExposedPorts(8080)
            .withEnv("KESTRA_SECURITY_SUPER_ADMIN_USERNAME", USERNAME)
            .withEnv("KESTRA_SECURITY_SUPER_ADMIN_PASSWORD", PASSWORD)
            .withEnv("KESTRA_EE_LICENSE_PUBLICKEY", System.getenv("KESTRA_EE_LICENSE_PUBLICKEY"))
            .withEnv("KESTRA_EE_LICENSE_KEY", System.getenv("KESTRA_EE_LICENSE_KEY"))
            .withEnv("KESTRA_EE_LICENSE_ID", System.getenv("KESTRA_EE_LICENSE_ID"))
            .withEnv("KESTRA_CONFIGURATION",
                """
                kestra:
                  encryption:
                    secret-key: I6EGNzRESu3X3pKZidrqCGOHQFUFC0yK
                  secret:
                    type: jdbc
                    jdbc:
                      secret: I6EGNzRESu3X3pKZidrqCGOHQFUFC0yK
                """)
            .withCommand("server local")
            .waitingFor(
                Wait.forHttp("/ui/login")
                    .forStatusCode(200)
            )
            .withLogConsumer(new Slf4jLogConsumer(log))
            .withStartupTimeout(Duration.ofMinutes(2));

    @BeforeAll
    static void setupKestra() throws ApiException {
        KESTRA_URL = "http://" + kestraContainer.getHost() + ":" + kestraContainer.getMappedPort(8080);

        log.info("Kestra started at URL: {}", KESTRA_URL);

        kestraTestDataUtils = new KestraTestDataUtils(
            KESTRA_URL,
            USERNAME,
            PASSWORD,
            TENANT_ID
        );

        generateDatas();
    }

    static void generateDatas() throws ApiException {
        KestraClient kc = kestraTestDataUtils.getKestraClient();

        Tenant tenant = new Tenant().id(TENANT_ID).name(TENANT_ID);
        kc.tenants().create(tenant);
    }
}
