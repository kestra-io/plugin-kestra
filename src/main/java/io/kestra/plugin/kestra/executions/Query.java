package io.kestra.plugin.kestra.executions;

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
import java.time.Duration;
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
    title = "Search for Kestra Executions"
)
@Plugin(
    examples = {
        @Example(
            title = "Search for executions with a specific label",
            full = true,
            code = """
                id: search_executions_by_label
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.kestra.executions.Query
                    kestraUrl: http://localhost:8080
                    labels:
                      key: value
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    fetchType: STORE # Store the results in a file
                """
        ),
        @Example(
            title = "Search for successful executions in the last 10 hours",
            full = true,
            code = """
                id: search_successful_executions
                namespace: company.team

                tasks:
                  - id: search_executions
                    type: io.kestra.plugin.kestra.executions.Query
                    kestraUrl: http://localhost:8080
                    timeRange: PT10H # In the last 10 hours
                    states:
                      - SUCCESS
                    auth:
                      username: admin@kestra.io # pass your Kestra username as secret or KV pair
                      password: Admin1234 # pass your Kestra password as secret or KV pair
                    fetchType: FETCH # Fetch the results directly in the task output
                """
        )
    }
)
public class Query extends AbstractKestraTask implements RunnableTask<FetchOutput> {
    @Nullable
    @Schema(title = "If not provided, all pages are fetched",
        description = "To efficiently fetch only the first 10 API results, you can use `page: 1` along with `size: 10`.")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(title = "The number of results to return per page.")
    private Property<Integer> size = Property.ofValue(10);

    @Nullable
    @Builder.Default
    @Schema(title = "The way the fetched data will be stored.")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Nullable
    @Schema(title = "Can be set to USER to fetch only user-created executions, or to SYSTEM to fetch only system executions. By default, the task will handle both.")
    private Property<List<FlowScope>> flowScopes;

    @Nullable
    @Schema(title = "To list only executions from a given namespace.")
    private Property<String> namespace;

    @Nullable
    @Schema(title = "To list only executions of a given flow.")
    private Property<String> flowId;

    @Nullable
    @Schema(title = "To list only executions created after a given start date.")
    private Property<ZonedDateTime> startDate;

    @Nullable
    @Schema(title = "To list only executions created before a given end date.")
    private Property<ZonedDateTime> endDate;

    @Nullable
    @Schema(title = "To list only executions created within a given time range duration.")
    private Property<Duration> timeRange;

    @Nullable
    @Schema(title = "To list only executions in given states.")
    private Property<List<StateType>> states;

    @Nullable
    @Schema(title = "To list only executions with given labels.")
    private Property<Map<String, String>> labels;

    @Nullable
    @Schema(title = "To list all downstream executions started from a given execution id.")
    private Property<String> triggerExecutionId;

    @Nullable
    @Schema(title = "To list only child executions of a given flow.")
    private Property<ExecutionRepositoryInterfaceChildFilter> childFilter;

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {

        KestraClient kestraClient = kestraClient(runContext);
        FetchOutput.FetchOutputBuilder output = FetchOutput.builder();
        Integer rPage = runContext.render(this.page).as(Integer.class).orElse(null);
        Integer rSize = runContext.render(this.size).as(Integer.class).orElse(10);

        List<Execution> executions = new java.util.ArrayList<>(List.of());
        long total;

        if (rPage != null) {
            PagedResultsExecution results = executeSearch(runContext, kestraClient, rPage, rSize);
            executions.addAll(results.getResults());
            total = results.getTotal();
        } else {
            int currentPage = 1;
            do {
                PagedResultsExecution results = executeSearch(runContext, kestraClient, currentPage, rSize);
                executions.addAll(results.getResults());
                total = results.getTotal();
                currentPage++;
            } while ((long) currentPage * rSize < total);
        }

        output.size(total);

        return switch (runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE)) {
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var fileOutput = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                    Flux<Execution> flux = Flux.fromIterable(executions);
                    FileSerde.writeAll(fileOutput, flux).block();
                }
                yield output.uri(runContext.storage().putFile(tempFile)).build();
            }
            case FETCH -> output.rows(Collections.singletonList(executions)).build();
            case FETCH_ONE -> {
                if (!executions.isEmpty()) {
                    output.row(Map.of("0", executions.getFirst()));
                }
                yield output.build();
            }
            default -> output.build();
        };

    }


    private PagedResultsExecution executeSearch(
        RunContext runContext,
        KestraClient kestraClient,
        Integer page,
        Integer size
    ) throws IllegalVariableEvaluationException, ApiException {
        List<FlowScope> rFlowScopes = runContext.render(this.flowScopes).asList(FlowScope.class);
        String tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rNamespace = runContext.render(this.namespace).as(String.class).orElse(null);
        String rFlowId = runContext.render(this.flowId).as(String.class).orElse(null);
        ZonedDateTime rStartDate = runContext.render(this.startDate).as(ZonedDateTime.class).orElse(null);
        ZonedDateTime rEndDate = runContext.render(this.endDate).as(ZonedDateTime.class).orElse(null);
        Duration rTimerange = runContext.render(this.timeRange).as(Duration.class).orElse(null);
        List<StateType> rState = runContext.render(this.states).asList(StateType.class);
        Map<String, String> rLabels = runContext.render(this.labels).asMap(String.class, String.class);
        String rTriggerExecutionId = runContext.render(this.triggerExecutionId).as(String.class).orElse(null);
        ExecutionRepositoryInterfaceChildFilter rChildFilter = runContext.render(this.childFilter).as(ExecutionRepositoryInterfaceChildFilter.class).orElse(null);

        return kestraClient.executions().searchExecutions(
            page,
            size,
            tId,
            null, // TODO: implement something for sorting?
            null, // Filters is not working correctly in the SDK yet
            null,
            rFlowScopes,
            rNamespace,
            rFlowId,
            rStartDate != null ? rStartDate.toOffsetDateTime() : null,
            rEndDate != null ? rEndDate.toOffsetDateTime() : null,
            rTimerange != null ? rTimerange.toString() : null,
            rState,
            rLabels.entrySet().stream().map(label -> label.getKey() + ":" + label.getValue()).toList(),
            rTriggerExecutionId,
            rChildFilter
        );
    }

}
