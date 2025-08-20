package io.kestra.plugin.kestra.logs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Fetch Kestra Execution Logs"
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch logs for a specific trigger",
            full = true,
            code = """
                id: fetch_trigger_logs
                namespace: company.team

                tasks:
                  - id: get_logs
                    type: io.kestra.plugin.kestra.logs.Fetch
                    kestraUrl: http://localhost:8080
                    triggerId: "my-trigger-id"
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    fetchType: STORE # Store the results in a file
                """
        ),
        @Example(
            title = "Fetch ERROR level logs from last hour",
            full = true,
            code = """
                id: fetch_error_logs
                namespace: company.team

                tasks:
                  - id: fetch_errors
                    type: io.kestra.plugin.kestra.logs.Fetch
                    kestraUrl: http://localhost:8080
                    namespace: company.team
                    minLevel: ERROR
                    startDate: "{{ now() | dateAdd(-1, 'HOURS') }}"
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    fetchType: FETCH # Fetch the results directly in the task output
                """
        ),
        @Example(
            title = "Fetch logs with query filters",
            full = true,
            code = """
                id: fetch_filtered_logs
                namespace: company.team

                tasks:
                  - id: get_filtered_logs
                    type: io.kestra.plugin.kestra.logs.Fetch
                    kestraUrl: http://localhost:8080
                    namespace: company.team
                    query: "error OR exception"
                    minLevel: WARN
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                """
        )
    }
)
public class Fetch extends AbstractKestraTask implements RunnableTask<FetchOutput> {
    @Nullable
    @Schema(title = "If not provided, all pages are fetched",
        description = "To efficiently fetch only the first 10 API results, you can use `page: 1` along with `size: 10`.")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(title = "The number of results to return per page.")
    private Property<Integer> size = Property.ofValue(100);

    @Nullable
    @Builder.Default
    @Schema(title = "The way the fetched data will be stored.")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Nullable
    @Schema(title = "To fetch logs from a specific trigger ID.")
    private Property<String> triggerId;

    @Nullable
    @Schema(title = "To fetch logs from a specific namespace.")
    private Property<String> namespace;

    @Nullable
    @Schema(title = "To fetch logs from a specific flow.")
    private Property<String> flowId;

    @Nullable
    @Schema(title = "To fetch logs created after a given start date.")
    private Property<ZonedDateTime> startDate;

    @Nullable
    @Schema(title = "To fetch logs created before a given end date.")
    private Property<ZonedDateTime> endDate;

    @Nullable
    @Schema(title = "Minimum log level to fetch (TRACE, DEBUG, INFO, WARN, ERROR).")
    private Property<Level> minLevel;

    @Nullable
    @Schema(title = "Search query string to filter logs.")
    private Property<String> query;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);
        FetchOutput.FetchOutputBuilder output = FetchOutput.builder();

        Integer rPage = runContext.render(this.page).as(Integer.class).orElse(null);
        Integer rSize = runContext.render(this.size).as(Integer.class).orElse(100);

        List<LogEntry> logs = new java.util.ArrayList<>();
        long total;

        if (rPage != null) {
            PagedResultsLogEntry results = executeSearch(runContext, kestraClient, rPage, rSize);
            logs.addAll(results.getResults());
            total = results.getTotal();
        } else {
            int currentPage = 1;
            do {
                PagedResultsLogEntry results = executeSearch(runContext, kestraClient, currentPage, rSize);
                logs.addAll(results.getResults());
                total = results.getTotal();
                currentPage++;
            } while ((long) currentPage * rSize < total);
        }

        output.size(total);

        return switch (runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE)) {
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var fileOutput = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                    Flux<LogEntry> flux = Flux.fromIterable(logs);
                    FileSerde.writeAll(fileOutput, flux).block();
                }
                yield output.uri(runContext.storage().putFile(tempFile)).build();
            }
            case FETCH -> output.rows(Collections.singletonList(logs)).build();
            case FETCH_ONE -> {
                if (!logs.isEmpty()) {
                    output.row(Map.of("0", logs.getFirst()));
                }
                yield output.build();
            }
            default -> output.build();
        };
    }

    private PagedResultsLogEntry executeSearch(
        RunContext runContext,
        KestraClient kestraClient,
        Integer page,
        Integer size
    ) throws IllegalVariableEvaluationException, ApiException {
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rNamespace = runContext.render(this.namespace).as(String.class).orElse(null);
        String rFlowId = runContext.render(this.flowId).as(String.class).orElse(null);
        String rTriggerId = runContext.render(this.triggerId).as(String.class).orElse(null);
        Level rMinLevel = runContext.render(this.minLevel).as(Level.class).orElse(null);
        ZonedDateTime rStartDate = runContext.render(this.startDate).as(ZonedDateTime.class).orElse(null);
        ZonedDateTime rEndDate = runContext.render(this.endDate).as(ZonedDateTime.class).orElse(null);
        String rQuery = runContext.render(this.query).as(String.class).orElse(null);

        return kestraClient.logs().searchLogs(
            page,
            size,
            tId,
            null, // sort parameter - can be added if needed
            null,
            rQuery,
            rNamespace,
            rFlowId,
            rTriggerId,
            rMinLevel,
            rStartDate != null ? rStartDate.toOffsetDateTime() : null,
            rEndDate != null ? rEndDate.toOffsetDateTime() : null
        );
    }
}