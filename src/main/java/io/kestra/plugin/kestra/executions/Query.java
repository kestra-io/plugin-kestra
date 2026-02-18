package io.kestra.plugin.kestra.executions;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.FlowScope;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.repositories.ExecutionRepositoryInterface;
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
import java.util.Objects;
import java.util.stream.Stream;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Search executions with filters",
    description = "Queries executions by namespace, flow, labels, states, date range, or scope. Defaults to fetch all pages with page size 10 and stores results to internal storage unless fetchType overrides."
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
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
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
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                    timeRange: PT10H # In the last 10 hours
                    states:
                      - SUCCESS
                    fetchType: FETCH # Fetch the results directly in the task output
                """
        )
    }
)
public class Query extends AbstractKestraTask implements RunnableTask<FetchOutput> {
    @Nullable
    @Schema(title = "Page number", description = "When null, iterates through all pages. Combine with size to limit requests.")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(title = "Page size", description = "Defaults to 10.")
    private Property<Integer> size = Property.ofValue(10);

    @Nullable
    @Builder.Default
    @Schema(title = "Fetch strategy", description = "Defaults to STORE (writes to internal storage and returns URI). FETCH returns rows in output; FETCH_ONE returns the first execution.")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Nullable
    @Schema(title = "Flow scope filter", description = "USER for user-created executions, SYSTEM for system executions; defaults to both.")
    private Property<List<FlowScope>> flowScopes;

    @Nullable
    @Schema(title = "Namespace filter")
    private Property<String> namespace;

    @Nullable
    @Schema(title = "Flow id filter")
    private Property<String> flowId;

    @Nullable
    @Schema(title = "Start date (inclusive)")
    private Property<ZonedDateTime> startDate;

    @Nullable
    @Schema(title = "End date (inclusive)")
    private Property<ZonedDateTime> endDate;

    @Nullable
    @Schema(title = "Relative time range", description = "Duration back from now. Cannot be used with both startDate and endDate.")
    private Property<Duration> timeRange;

    @Nullable
    @Schema(title = "Execution states")
    private Property<List<StateType>> states;

    @Nullable
    @Schema(title = "Labels filter", description = "Matches executions containing the provided key/value pairs.")
    private Property<Map<String, String>> labels;

    @Nullable
    @Schema(title = "Downstream of execution ID")
    private Property<String> triggerExecutionId;

    @Nullable
    @Schema(title = "Child filter", description = "Limits results to child execution context when set.")
    private Property<ExecutionRepositoryInterface.ChildFilter> childFilter;

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

        if (rTimerange != null) {
            if (rStartDate != null && rEndDate != null) {
                throw new IllegalVariableEvaluationException("`timeRange` cannot be used together with both `startDate` and `endDate`.");
            } else if (rStartDate == null && rEndDate == null) {
                ZonedDateTime now = ZonedDateTime.now();
                rEndDate = now;
                rStartDate = now.minus(rTimerange);
            }
        }

        List<StateType> rState = runContext.render(this.states).asList(StateType.class);
        Map<String, String> rLabels = runContext.render(this.labels).asMap(String.class, String.class);
        String rTriggerExecutionId = runContext.render(this.triggerExecutionId).as(String.class).orElse(null);
        ExecutionRepositoryInterface.ChildFilter rChildFilter = runContext.render(this.childFilter).as(ExecutionRepositoryInterface.ChildFilter.class).orElse(null);

        List<QueryFilter> filters = new java.util.ArrayList<>(Stream.of(
            rNamespace != null ? new QueryFilter().field(QueryFilterField.NAMESPACE).operation(QueryFilterOp.EQUALS).value(rNamespace) : null,
            rFlowId != null ? new QueryFilter().field(QueryFilterField.FLOW_ID).operation(QueryFilterOp.EQUALS).value(rFlowId) : null,
            rStartDate != null ? new QueryFilter().field(QueryFilterField.START_DATE).operation(QueryFilterOp.LESS_THAN_OR_EQUAL_TO).value(rStartDate) : null,
            rEndDate != null ? new QueryFilter().field(QueryFilterField.START_DATE).operation(QueryFilterOp.GREATER_THAN_OR_EQUAL_TO).value(rEndDate) : null,
            rTriggerExecutionId != null ? new QueryFilter().field(QueryFilterField.TRIGGER_EXECUTION_ID).operation(QueryFilterOp.EQUALS).value(rTriggerExecutionId) : null,
            rChildFilter != null ? new QueryFilter().field(QueryFilterField.CHILD_FILTER).operation(QueryFilterOp.EQUALS).value(rChildFilter) : null
        ).filter(Objects::nonNull).toList());

        if (rFlowScopes != null && !rFlowScopes.isEmpty()) {
            rFlowScopes.forEach(flowScope -> {
                filters.add(
                    new QueryFilter()
                        .field(QueryFilterField.SCOPE)
                        .operation(QueryFilterOp.EQUALS)
                        .value(flowScope)
                );
            });
        }

        if (rState != null && !rState.isEmpty()) {
            rState.forEach(state -> {
                filters.add(
                    new QueryFilter()
                        .field(QueryFilterField.STATE)
                        .operation(QueryFilterOp.EQUALS)
                        .value(state)
                );
            });
        }

        if (rLabels != null && !rLabels.isEmpty()) {
            rLabels.forEach((key, value) -> {
                filters.add(
                    new QueryFilter()
                        .field(QueryFilterField.STATE)
                        .operation(QueryFilterOp.EQUALS)
                        .value(key + ":" + value)
                );
            });
        }

        return kestraClient.executions().searchExecutions(
            page,
            size,
            tId,
            null,
            filters
        );
    }

}
