package io.kestra.plugin;

import io.kestra.sdk.internal.ApiException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public abstract class AbstractKestraContainerTest {
    protected final String USERNAME = "admin@admin.com";
    protected final String PASSWORD = "Root!1234";
    protected final String TENANT_ID = "main";

    protected String KESTRA_URL;
    protected KestraTestDataUtils kestraTestDataUtils;

    protected abstract GenericContainer<?> getContainer();

    @BeforeAll
    void setupKestra() throws ApiException {
        GenericContainer<?> kestraContainer = getContainer();
        kestraContainer.start();

        KESTRA_URL = "http://" + kestraContainer.getHost() + ":" + kestraContainer.getMappedPort(8080);
        log.info("Kestra started at URL: {}", KESTRA_URL);

        kestraTestDataUtils = new KestraTestDataUtils(KESTRA_URL, USERNAME, PASSWORD, TENANT_ID);
    }

    protected GenericContainer<?> buildContainer(String image, boolean ee) {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(image))
            .withExposedPorts(8080)
            .withEnv("KESTRA_SECURITY_SUPER_ADMIN_USERNAME", USERNAME)
            .withEnv("KESTRA_SECURITY_SUPER_ADMIN_PASSWORD", PASSWORD)
            .withEnv("KESTRA_CONFIGURATION", """
                kestra:
                  server:
                    basic-auth:
                      username: admin@admin.com
                      password: Root!1234
                  ee:
                    tenants:
                      enabled: false
                      defaultTenant: true
                  encryption:
                    secret-key: I6EGNzRESu3X3pKZidrqCGOHQFUFC0yK
                  secret:
                    type: jdbc
                    jdbc:
                      secret: I6EGNzRESu3X3pKZidrqCGOHQFUFC0yK
                  security:
                    super-admin:
                      username: admin@admin.com
                      password: Root!1234
                      tenantAdminAccess: main
                """)
            .withCommand("server local")
            .waitingFor(Wait.forHttp("/ui/login").forStatusCode(200))
            .withLogConsumer(new Slf4jLogConsumer(log))
            .withStartupTimeout(Duration.ofMinutes(2));

        if (ee) {
            container
                .withEnv("KESTRA_EE_LICENSE_PUBLICKEY", System.getenv("KESTRA_EE_LICENSE_PUBLICKEY"))
                .withEnv("KESTRA_EE_LICENSE_KEY", System.getenv("KESTRA_EE_LICENSE_KEY"))
                .withEnv("KESTRA_EE_LICENSE_ID", System.getenv("KESTRA_EE_LICENSE_ID"));
        }

        return container;
    }
}
