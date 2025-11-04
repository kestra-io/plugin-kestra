package io.kestra.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.BasicAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.Tenant;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

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

        generateData();
    }

    @SneakyThrows
    static void generateData() throws ApiException {
        Tenant tenant = new Tenant().id(TENANT_ID).name(TENANT_ID);

        var auth  = BasicAuthConfiguration.builder().username(Property.ofValue(USERNAME)).password(Property.ofValue(PASSWORD)).build();
        var httpClient = HttpClient.builder().configuration(HttpConfiguration.builder().auth(auth).build()).build();

        var res = httpClient.request(HttpRequest.builder()
                .method("POST")
                .body(HttpRequest.RequestBody.from(new StringEntity(new ObjectMapper().writeValueAsString(tenant))))
                .uri(URI.create(KESTRA_URL + "/api/v1/tenants"))
            .build());
        assertThat(res.getStatus().getCode()).isEqualTo(201);
    }
}
