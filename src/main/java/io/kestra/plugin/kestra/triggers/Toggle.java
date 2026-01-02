package io.kestra.plugin.kestra.triggers;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.*;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Toggle a trigger: enable or disable it."
)
@Plugin(
    examples = {
        @Example(
            title = "Toggle a trigger on flow input.",
            full = true,
            code = """
                id: trigger_toggle
                namespace: company.team

                inputs:
                  - id: toggle
                    type: BOOL
                    defaults: true

                tasks:
                  - id: if
                    type: io.kestra.plugin.core.flow.If
                    condition: "{{inputs.toggle}}"
                    then:
                      - id: enable
                        type: io.kestra.plugin.kestra.triggers.Toggle
                        trigger: schedule
                        enabled: true
                    else:
                      - id: disable
                        type: io.kestra.plugin.kestra.triggers.Toggle
                        trigger: schedule
                        enabled: false
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: Hello World

                triggers:
                  - id: schedule
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "* * * * *"
                """
        )
    }
)
public class Toggle extends AbstractKestraTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "The flow identifier of the trigger to toggle",
        description = "If not set, the current flow identifier will be used."
    )
    private Property<String> flowId;

    @Schema(title = "The namespace to list flows from, if null, defaults to the namespace of the current flow.")
    private Property<String> namespace;

    @Schema(title = "The identifier of the trigger to toggle")
    @NotNull
    private Property<String> trigger;

    @Schema(title = "Whether to enable or disable the trigger")
    @NotNull
    @Builder.Default
    private Property<Boolean> enabled = Property.ofValue(false);

    @SuppressWarnings("unchecked")
    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var kestraClient = kestraClient(runContext);
        var tId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rNamespace = runContext.render(namespace).as(String.class).orElseGet(() -> runContext.flowInfo().namespace());
        var rFlowId = runContext.render(flowId).as(String.class).orElse(null);
        var rTriggerId = runContext.render(trigger).as(String.class).orElseThrow();

        List<QueryFilter> filters = new java.util.ArrayList<>(Stream.of(
            rNamespace != null ? new QueryFilter().field(QueryFilterField.NAMESPACE).operation(QueryFilterOp.EQUALS).value(rNamespace) : null,
            rFlowId != null ? new QueryFilter().field(QueryFilterField.FLOW_ID).operation(QueryFilterOp.EQUALS).value(rFlowId) : null,
            new QueryFilter().field(QueryFilterField.TRIGGER_ID).operation(QueryFilterOp.EQUALS).value(rTriggerId)
        ).filter(Objects::nonNull).toList());

        var disabledTriggers = JacksonMapper.ofJson().convertValue(kestraClient.triggers().disabledTriggersByQuery(!runContext.render(enabled).as(Boolean.class).orElse(false), tId, filters), TriggerResponse.class);

        runContext.logger().info("{} triggers found to Toggle", disabledTriggers.getCount());

        return null;
    }

    @Builder
    @Getter
    public static class TriggerResponse {
        private long count;
    }
}