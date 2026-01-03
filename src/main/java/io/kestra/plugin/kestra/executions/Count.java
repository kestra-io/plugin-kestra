package io.kestra.plugin.kestra.executions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Count Kestra executions.")
@Plugin(
    examples = {
        @Example(
            title = "Count successful executions for a flow",
            full = true,
            code = """
                id: count_executions
                namespace: company.team

                tasks:
                  - id: count
                    type: io.kestra.plugin.kestra.executions.Count
                    kestraUrl: http://localhost:8080
                    namespace: company.team
                    flowId: my-flow
                    states:
                      - SUCCESS
                    auth:
                      username: admin@kestra.io
                      password: Admin1234
                """
        )
    }
)
public class Count extends AbstractKestraTask implements RunnableTask<Count.Output> {

    @Nullable
    @Schema(title = "Namespace to filter executions")
    private Property<String> namespace;

    @Nullable
    @Schema(title = "Flow ID to filter executions")
    private Property<String> flowId;

    @Nullable
    @Schema(title = "Execution states to filter")
    private Property<List<StateType>> states;

    @Override
    public Output run(RunContext runContext) throws Exception {
        KestraClient kestraClient = kestraClient(runContext);

        String rTenantId = runContext.render(this.tenantId)
            .as(String.class)
            .orElse(runContext.flowInfo().tenantId());

        String rNamespace = runContext.render(this.namespace).as(String.class).orElse(null);
        String rFlowId = runContext.render(this.flowId).as(String.class).orElse(null);
        List<StateType> rStates = runContext.render(this.states).asList(StateType.class);

        List<QueryFilter> filters = new ArrayList<>(
            Stream.of(
                rNamespace != null ? new QueryFilter().field(QueryFilterField.NAMESPACE).operation(QueryFilterOp.EQUALS).value(rNamespace) : null,
                rFlowId != null ? new QueryFilter().field(QueryFilterField.FLOW_ID).operation(QueryFilterOp.EQUALS).value(rFlowId) : null
            ).filter(Objects::nonNull).toList()
        );

        if (rStates != null && !rStates.isEmpty()) {
            rStates.forEach(state ->
                filters.add(
                    new QueryFilter()
                        .field(QueryFilterField.STATE)
                        .operation(QueryFilterOp.EQUALS)
                        .value(state)
                )
            );
        }

        PagedResultsExecution results = kestraClient.executions().searchExecutions(
            1,
            1,
            rTenantId,
            null,
            filters
        );

        runContext.logger().info("Found {} matching executions", results.getTotal());

        return new Output(results.getTotal());
    }


    @Getter
    @AllArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of matching executions")
        private Long count;
    }

}
