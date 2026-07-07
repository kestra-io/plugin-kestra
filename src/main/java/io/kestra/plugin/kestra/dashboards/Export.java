package io.kestra.plugin.kestra.dashboards;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.dashboards.charts.DataChart;
import io.kestra.core.models.dashboards.charts.DataChartKPI;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.ChartChartOption;
import io.kestra.sdk.model.ChartFiltersOverrides;
import io.kestra.sdk.model.DashboardControllerDashboardResponse;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Export a dashboard's chart data to file(s)",
    description = "Exports one or every non-Markdown chart of a Kestra dashboard to internal storage, ready to be attached to a report, e.g. by email. Defaults to the current tenant's default dashboard and CSV format."
)
@Plugin(
    examples = {
        @Example(
            title = "Export a dashboard chart to CSV and email it as a weekly report.",
            full = true,
            code = """
                id: weekly_compliance_report
                namespace: company.team

                tasks:
                  - id: export_chart
                    type: io.kestra.plugin.kestra.dashboards.Export
                    kestraUrl: http://localhost:8080
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: "{{ secret('KESTRA_PASSWORD') }}" # pass your Kestra password as secret or KV pair
                    dashboardId: compliance_overview
                    chartId: executions_by_namespace
                    format: CSV

                  - id: send_email
                    type: io.kestra.plugin.email.MailSend
                    from: hello@kestra.io
                    to: compliance@kestra.io
                    username: "{{ secret('EMAIL_USERNAME') }}"
                    password: "{{ secret('EMAIL_PASSWORD') }}"
                    host: mail.privateemail.com
                    port: 465 # or 587
                    subject: "Weekly Compliance Report"
                    htmlTextContent: "Please find attached the weekly compliance report."
                    attachments:
                      - name: executions_by_namespace.csv
                        uri: "{{ outputs.export_chart.files['executions_by_namespace'] }}"
                        contentType: text/csv

                triggers:
                  - id: schedule
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: 0 8 * * 1
                """
        )
    }
)
public class Export extends AbstractKestraTask implements RunnableTask<Export.Output> {
    private static final String DEFAULT_DASHBOARD_ID = "_default";

    @Schema(title = "Dashboard id", description = "Identifier of the dashboard to export from. Defaults to the current tenant's default dashboard.")
    @PluginProperty(group = "source")
    private Property<String> dashboardId;

    @Schema(title = "Chart id", description = "Identifier of the chart to export. When omitted, every non-Markdown chart of the dashboard is exported.")
    @PluginProperty(group = "source")
    private Property<String> chartId;

    @Builder.Default
    @Schema(title = "Export format", description = "Supported values: `CSV`, `ION`. Defaults to `CSV`.")
    @PluginProperty(group = "processing")
    private Property<Format> format = Property.ofValue(Format.CSV);

    @Schema(title = "Start date", description = "Overrides the dashboard's default time window start.")
    @PluginProperty(group = "advanced")
    private Property<OffsetDateTime> startDate;

    @Schema(title = "End date", description = "Overrides the dashboard's default time window end.")
    @PluginProperty(group = "advanced")
    private Property<OffsetDateTime> endDate;

    @Override
    public Export.Output run(RunContext runContext) throws Exception {
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rDashboardId = runContext.render(dashboardId).as(String.class).orElse(DEFAULT_DASHBOARD_ID);
        Format rFormat = runContext.render(format).as(Format.class).orElse(Format.CSV);
        String rChartId = runContext.render(chartId).as(String.class).orElse(null);
        OffsetDateTime rStartDate = runContext.render(startDate).as(OffsetDateTime.class).orElse(null);
        OffsetDateTime rEndDate = runContext.render(endDate).as(OffsetDateTime.class).orElse(null);

        KestraClient kestraClient = kestraClient(runContext);

        List<String> chartIds = rChartId != null
            ? List.of(rChartId)
            : resolveExportableChartIds(kestraClient, rDashboardId, tId);

        ChartFiltersOverrides filters = new ChartFiltersOverrides();
        if (rStartDate != null) {
            filters.startDate(rStartDate);
        }
        if (rEndDate != null) {
            filters.endDate(rEndDate);
        }

        String extension = rFormat.name().toLowerCase();
        Map<String, URI> files = new LinkedHashMap<>();
        for (String cId : chartIds) {
            byte[] chartBytes = kestraClient.dashboards().exportDashboardChart(rDashboardId, cId, tId, filters, rFormat.name());
            String fileName = rDashboardId + "_" + cId + "." + extension;
            files.put(cId, runContext.storage().putFile(new ByteArrayInputStream(chartBytes), fileName));
        }

        return Export.Output.builder()
            .files(files)
            .build();
    }

    private List<String> resolveExportableChartIds(KestraClient kestraClient, String dashId, String tId) throws Exception {
        DashboardControllerDashboardResponse dashboard = kestraClient.dashboards().dashboard(dashId, tId);
        if (dashboard.getCharts() == null) {
            return List.of();
        }

        return dashboard.getCharts().stream()
            .filter(Export::isExportableChart)
            .map(ChartChartOption::getId)
            .toList();
    }

    private static boolean isExportableChart(ChartChartOption chart) {
        try {
            Class<?> chartClass = Class.forName(chart.getType());
            return DataChart.class.isAssignableFrom(chartClass) || DataChartKPI.class.isAssignableFrom(chartClass);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public enum Format {
        CSV,
        ION
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Exported chart files",
            description = "Map of chart id to the URI of its exported file in internal storage."
        )
        private Map<String, URI> files;
    }
}
