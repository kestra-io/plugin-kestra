package io.kestra.plugin.kestra.dashboards;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kestra.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.DashboardControllerDashboardResponse;

import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
@Slf4j
@Execution(ExecutionMode.SAME_THREAD)
public class ExportTest extends AbstractKestraContainerTest {

    /*
     * TODO: "kestra:local-dashboards-export" is a placeholder tag for a locally-built OSS image that
     * exposes the dashboard chart export route (POST /dashboards/{id}/charts/{chartId}/export). It does
     * not exist yet: this route ships with Kestra 2.0. Build and tag such an image locally before running
     * this test; until then this test class cannot execute.
     */
    @Container
    protected static final GenericContainer<?> KESTRA_LOCAL_CONTAINER = new ExportTest() {
    }.buildContainer("kestra:local-dashboards-export", false);

    @Override
    protected GenericContainer<?> getContainer() {
        return KESTRA_LOCAL_CONTAINER;
    }

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.dashboards.export";

    @Test
    public void shouldExportSingleChartAsCsv() throws Exception {
        RunContext runContext = runContextFactory.of();

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        String dashboardId = randomId("dashboard");
        String chartId = "flows_table";
        createDashboard(dashboardId, false, chartId);

        Export exportChart = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .dashboardId(Property.ofValue(dashboardId))
            .chartId(Property.ofValue(chartId))
            .format(Property.ofValue(Export.Format.CSV))
            .build();

        Export.Output output = exportChart.run(runContext);

        assertThat(output.getFiles(), aMapWithSize(1));
        assertThat(output.getFiles(), hasKey(chartId));

        String csv = readFileAsString(output.getFiles().get(chartId), runContext);
        String[] lines = csv.strip().split("\n");

        assertThat(csv, is(not("")));
        assertThat(lines.length, is(greaterThanOrEqualTo(2))); // header row + at least one data row
    }

    @Test
    public void shouldExportEveryNonMarkdownChartWhenChartIdOmitted() throws Exception {
        RunContext runContext = runContextFactory.of();

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        String dashboardId = randomId("dashboard");
        String chartId1 = "flows_table_1";
        String chartId2 = "flows_table_2";
        createDashboard(dashboardId, true, chartId1, chartId2);

        Export exportChart = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .dashboardId(Property.ofValue(dashboardId))
            .build();

        Export.Output output = exportChart.run(runContext);

        assertThat(output.getFiles(), aMapWithSize(2));
        assertThat(output.getFiles(), hasKey(chartId1));
        assertThat(output.getFiles(), hasKey(chartId2));
    }

    /*
     * Relies on every fresh Kestra instance shipping a built-in dashboard resolved through the "_default"
     * sentinel id (per the task's OSS contract); we cannot provision it ourselves through the SDK.
     */
    @Test
    public void shouldFallBackToDefaultDashboardWhenDashboardIdOmitted() throws Exception {
        RunContext runContext = runContextFactory.of();

        Export exportChart = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .build();

        Export.Output output = exportChart.run(runContext);

        assertThat(output.getFiles(), is(notNullValue()));
        assertThat(output.getFiles().isEmpty(), is(false));
    }

    @Test
    public void shouldExportChartAsIon() throws Exception {
        RunContext runContext = runContextFactory.of();

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);

        String dashboardId = randomId("dashboard");
        String chartId = "flows_table";
        createDashboard(dashboardId, false, chartId);

        Export exportCsv = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .dashboardId(Property.ofValue(dashboardId))
            .chartId(Property.ofValue(chartId))
            .format(Property.ofValue(Export.Format.CSV))
            .build();

        Export exportIon = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .dashboardId(Property.ofValue(dashboardId))
            .chartId(Property.ofValue(chartId))
            .format(Property.ofValue(Export.Format.ION))
            .build();

        byte[] csvBytes = readFileAsBytes(exportCsv.run(runContext).getFiles().get(chartId), runContext);
        byte[] ionBytes = readFileAsBytes(exportIon.run(runContext).getFiles().get(chartId), runContext);

        assertThat(csvBytes.length, is(not(0)));
        assertThat(ionBytes.length, is(not(0)));
        assertThat(ionBytes, is(not(csvBytes)));
    }

    @Test
    public void shouldFailWhenChartIdIsAMarkdownChart() throws Exception {
        RunContext runContext = runContextFactory.of();

        String dashboardId = randomId("dashboard");
        createDashboard(dashboardId, true);

        Export exportChart = Export.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .dashboardId(Property.ofValue(dashboardId))
            .chartId(Property.ofValue("markdown_note"))
            .format(Property.ofValue(Export.Format.CSV))
            .build();

        assertThrows(Exception.class, () -> exportChart.run(runContext));
    }

    private void createDashboard(String dashboardId, boolean includeMarkdown, String... tableChartIds) throws Exception {
        StringBuilder charts = new StringBuilder();
        for (String chartId : tableChartIds) {
            charts.append("""
                  - id: %s
                    type: io.kestra.plugin.core.dashboard.chart.Table
                    chartOptions:
                      displayName: Flows table %s
                    data:
                      type: io.kestra.plugin.core.dashboard.data.Flows
                      columns:
                        namespace:
                          field: NAMESPACE
                        id:
                          field: ID
                """.formatted(chartId, chartId));
        }
        if (includeMarkdown) {
            charts.append("""
                  - id: markdown_note
                    type: io.kestra.plugin.core.dashboard.chart.Markdown
                    chartOptions:
                      displayName: Notes
                    source:
                      type: Text
                      content: |
                        Some notes here.
                """);
        }

        String dashboardYaml = """
            id: %s
            title: Export test dashboard
            description: Dashboard used by ExportTest
            timeWindow:
              default: P30D
              max: P365D

            charts:
            %s""".formatted(dashboardId, charts);

        DashboardControllerDashboardResponse dashboard = kestraTestDataUtils.getKestraClient()
            .dashboards()
            .createDashboard(TENANT_ID, dashboardYaml);

        assertThat(dashboard.getId(), is(dashboardId));
    }

    private String randomId(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "_");
    }

    private String readFileAsString(URI uri, RunContext runContext) throws Exception {
        return new String(readFileAsBytes(uri, runContext), StandardCharsets.UTF_8);
    }

    private byte[] readFileAsBytes(URI uri, RunContext runContext) throws Exception {
        try (InputStream is = runContext.storage().getFile(uri)) {
            return is.readAllBytes();
        }
    }

}
