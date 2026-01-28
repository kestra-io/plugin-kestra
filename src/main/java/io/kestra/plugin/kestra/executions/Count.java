package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.PagedResultsExecution;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;
import io.kestra.sdk.model.StateType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Schema(
    title = "Count Kestra executions",
    description = "Counts executions matching filters and evaluates an optional expression against the count."
)
@Plugin(
    examples = {
        @Example(
            title = "Count successful executions in a namespace",
            full = true,
            code = """
                id: count_success_executions
                namespace: company.team

                tasks:
                  - id: count_success
                    type: io.kestra.plugin.kestra.executions.Count
                    kestraUrl: http://localhost:8080
                    auth:
                      username: "{{ secrets('KESTRA_USERNAME') }}"
                      password: "{{ secrets('KESTRA_PASSWORD') }}"
                    namespaces:
                    - company.team
                    states:
                    - SUCCESS
                    startDate: "{{ now() | dateAdd(-7, 'DAYS') }}"
                    endDate: "{{ now() }}"
                    expression: "{{ count >= 0 }}"

                  - id: log_result
                    type: io.kestra.plugin.core.log.Log
                    message: |
                        Execution count check completed.
                        Matching executions: {{ outputs.count_success.count }}
                """
        )
    }
)
public class Count extends AbstractKestraTask implements RunnableTask<Count.Output> {
    @Schema(
        title = "To count only executions from given namespaces"
    )
    private Property<List<String>> namespaces;

    @Schema(
        title = "A list of flows to be filtered"
    )
    private Property<String> flowId;

    @Schema(
        title = "A list of states to be filtered"
    )
    private Property<List<StateType>> states;

    @Schema(
        title = "Start date",
        description = "Counts executions starting from this date."
    )
    private Property<String> startDate;

    @Schema(
        title = "End date",
        description = "Counts executions up to this date."
    )
    private Property<String> endDate;

    @PluginProperty(dynamic = true) // we cannot use `Property` as we render it multiple time with different variables, which is an issue for the property cache
    @Schema(
        title = "The expression to check against each flow",
        description = "The expression is such that the expression must return `true` in order to keep the current line.\n" +
            "Some examples: \n" +
            "- ```yaml {{ eq count 0 }} ```: no execution found\n" +
            "- ```yaml {{ gte count 5 }} ```: more than 5 executions\n"
    )
    protected String expression;

    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient client = kestraClient(runContext);
        List<QueryFilter> filters = new ArrayList<>();

        String rTenantId = runContext.render(this.tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());

        List<String> rNamespaces = runContext.render(this.namespaces).asList(String.class);
        if (rNamespaces != null) {
            for (String namespace : rNamespaces) {
                filters.add(
                    new QueryFilter()
                        .field(QueryFilterField.NAMESPACE)
                        .operation(QueryFilterOp.EQUALS)
                        .value(namespace)
                );
            }
        }

        String rFlowId = runContext.render(this.flowId).as(String.class).orElse(null);
        if (rFlowId != null) {
            filters.add(
                new QueryFilter()
                    .field(QueryFilterField.FLOW_ID)
                    .operation(QueryFilterOp.EQUALS)
                    .value(rFlowId)
            );

        }

        List<StateType> rStates = runContext.render(this.states).asList(StateType.class);
        if (rStates != null) {
            for (StateType state : rStates) {
               filters.add(
                    new QueryFilter()
                        .field(QueryFilterField.STATE)
                        .operation(QueryFilterOp.EQUALS)
                        .value(state)
                );
            }
        }

        var rStartDate = runContext.render(this.startDate).as(String.class).map(ZonedDateTime::parse).map(ZonedDateTime::toInstant).orElse(null);
        if (rStartDate != null) {
            filters.add(
                new QueryFilter()
                    .field(QueryFilterField.START_DATE)
                    .operation(QueryFilterOp.GREATER_THAN_OR_EQUAL_TO)
                    .value(rStartDate)
            );
        }

        var rEndDate = runContext.render(this.endDate).as(String.class).map(ZonedDateTime::parse).map(ZonedDateTime::toInstant).orElse(null);
        if (rEndDate != null) {
            filters.add(
                new QueryFilter()
                    .field(QueryFilterField.END_DATE)
                    .operation(QueryFilterOp.LESS_THAN_OR_EQUAL_TO)
                    .value(rEndDate)
            );
        }

        PagedResultsExecution results = client.executions().searchExecutions(
            1,
            1,
            rTenantId,
            null,
            filters
        );

        long count = results.getTotal();
        runContext.logger().info("Found {} matching executions", count);

        if (expression != null) {
            String evaluated = runContext.render(expression, Map.of("count", count));

            if (!"true".equalsIgnoreCase(evaluated)) {
                count = 0L;
            }
        }

        return Output.builder()
            .count(count)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Execution count",
            description = "The total number of executions."
        )
        private final Long count;
    }
}
