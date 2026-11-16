package io.kestra.plugin.kestra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.ee.tests.RunTest;
import io.kestra.plugin.kestra.ee.tests.RunTests;
import io.kestra.plugin.kestra.namespaces.List;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AbstractKestraTaskReadTimeoutTest {
    private static final long SERVER_DELAY_MS = 2000;

    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange ->
        {
            try {
                Thread.sleep(SERVER_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            var body = "{\"results\":[],\"total\":0}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            try {
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (IOException ignored) {
                // client already gave up after its read timeout
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private List.ListBuilder<?, ?> task() {
        return List.builder()
            .kestraUrl(Property.ofValue("http://127.0.0.1:" + server.getAddress().getPort()))
            .auth(AbstractKestraTask.Auth.builder().auto(Property.ofValue(false)).build());
    }

    @Test
    void shouldFailWhenReadTimeoutIsShorterThanServerDelay() throws Exception {
        var runContext = runContextFactory.of();
        var task = task().readTimeout(Property.ofValue(Duration.ofSeconds(1))).build();

        var error = assertThrows(Exception.class, () -> task.run(runContext));

        assertThat(String.valueOf(error.getMessage()).toLowerCase() + error.getCause(), containsString("time"));
    }

    @Test
    void shouldSucceedWhenReadTimeoutIsLongerThanServerDelay() throws Exception {
        var runContext = runContextFactory.of();
        var task = task().readTimeout(Property.ofValue(Duration.ofSeconds(10))).build();

        var output = task.run(runContext);

        assertThat(output.getNamespaces().isEmpty(), is(true));
    }

    @Test
    void shouldFallBackToTaskTimeoutWhenReadTimeoutIsUnset() throws Exception {
        var runContext = runContextFactory.of();
        var task = task().timeout(Property.ofValue(Duration.ofSeconds(1))).build();

        assertThat(task.resolveReadTimeout(runContext), is(Duration.ofSeconds(1)));
        assertThrows(Exception.class, () -> task.run(runContext));
    }

    @Test
    void shouldResolveReadTimeoutInPriorityOrder() throws Exception {
        var runContext = runContextFactory.of();

        var both = task()
            .readTimeout(Property.ofValue(Duration.ofSeconds(5)))
            .timeout(Property.ofValue(Duration.ofSeconds(1)))
            .build();
        assertThat(both.resolveReadTimeout(runContext), is(Duration.ofSeconds(5)));

        assertThat(task().build().resolveReadTimeout(runContext), nullValue());

        var runTests = RunTests.builder().build();
        assertThat(runTests.resolveReadTimeout(runContext), is(Duration.ofMillis(Integer.MAX_VALUE)));
        var runTest = RunTest.builder().build();
        assertThat(runTest.resolveReadTimeout(runContext), is(Duration.ofMillis(Integer.MAX_VALUE)));

        var overridden = RunTests.builder().readTimeout(Property.ofValue(Duration.ofMinutes(1))).build();
        assertThat(overridden.resolveReadTimeout(runContext), is(Duration.ofMinutes(1)));
    }

    @Test
    void shouldRejectNonPositiveTimeouts() throws Exception {
        var runContext = runContextFactory.of();

        var zeroRead = task().readTimeout(Property.ofValue(Duration.ZERO)).build();
        var readError = assertThrows(IllegalArgumentException.class, () -> zeroRead.run(runContext));
        assertThat(readError.getMessage(), is("readTimeout must be a positive duration"));

        var negativeConnect = task().connectTimeout(Property.ofValue(Duration.ofSeconds(-1))).build();
        var connectError = assertThrows(IllegalArgumentException.class, () -> negativeConnect.run(runContext));
        assertThat(connectError.getMessage(), is("connectTimeout must be a positive duration"));
    }
}
