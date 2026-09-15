package io.kestra.plugin.kestra.ee.notifications;

import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.model.NotificationControllerNotificationFromTaskRequest;
import io.kestra.sdk.model.Subjects;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an in-app notification for Kestra users.",
    description = """
        Adds a notification to the notification sidebar of every recipient, typically from an `errors:` task \
        so that whoever owns a flow is told when it fails. The notification links back to the execution that \
        created it.

        Notifications are an Enterprise Edition feature, so this task fails against an Open Source instance."""
)
@Plugin(
    examples = {
        @Example(
            title = "Notify a user and a group when a flow fails.",
            full = true,
            code = """
                id: order_sync
                namespace: company.team

                tasks:
                  - id: sync
                    type: io.kestra.plugin.core.execution.Fail

                errors:
                  - id: notify_owners
                    type: io.kestra.plugin.kestra.ee.notifications.CreateNotification
                    title: "{{ flow.id }} failed"
                    recipients:
                      users:
                        - admin@kestra.io
                      groups:
                        - Platform
                """
        )
    }
)
public class CreateNotification extends AbstractKestraTask implements RunnableTask<CreateNotification.Output> {

    @NotNull
    @Schema(title = "Notification title", description = "The text shown in the notification sidebar.")
    @PluginProperty(group = "main")
    private Property<String> title;

    @NotNull
    @Schema(
        title = "Who to notify",
        description = "Users by email, groups by name, or both. Group members are resolved by Kestra, and at least one user or group is required."
    )
    @PluginProperty(group = "main")
    private RecipientsProperty recipients;

    @Schema(
        title = "The execution the notification links to",
        description = "Defaults to the execution this task runs in. Set it to `{{ trigger.executionId }}` when the task runs in a flow started by a Flow trigger, so that the upstream execution is linked instead of the triggered flow's own."
    )
    @PluginProperty(group = "main")
    private Property<String> executionId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rTenantId = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        String rTitle = runContext.render(title).as(String.class).orElseThrow(() -> new IllegalArgumentException("title is required"));
        Subjects rRecipients = renderRecipients(runContext);

        if (rRecipients.getUsers().isEmpty() && rRecipients.getGroups().isEmpty()) {
            throw new IllegalArgumentException("recipients must contain at least one user or group");
        }

        String rExecutionId = this.executionId != null
            ? runContext.render(this.executionId).as(String.class).orElse(null)
            : runContext.taskRunInfo().executionId();

        NotificationControllerNotificationFromTaskRequest request = new NotificationControllerNotificationFromTaskRequest()
            .title(rTitle)
            .recipients(rRecipients)
            .executionId(rExecutionId);

        Map<String, Object> result = kestraClient(runContext).notifications().createFromTask(rTenantId, request);
        List<String> notificationIds = notificationIds(result);

        return Output.builder()
            .notificationIds(notificationIds)
            .count(notificationIds.size())
            .build();
    }

    private Subjects renderRecipients(RunContext runContext) throws Exception {
        if (recipients == null) {
            return new Subjects().users(List.of()).groups(List.of());
        }

        List<String> users = recipients.getUsers() != null ? runContext.render(recipients.getUsers()).asList(String.class) : List.of();
        List<String> groups = recipients.getGroups() != null ? runContext.render(recipients.getGroups()).asList(String.class) : List.of();
        return new Subjects().users(users).groups(groups);
    }

    @SuppressWarnings("unchecked")
    private static List<String> notificationIds(Map<String, Object> result) {
        return (List<String>) result.getOrDefault("notificationIds", List.of());
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @ToString
    public static class RecipientsProperty {
        @Schema(title = "User emails")
        private Property<List<String>> users;

        @Schema(title = "Group names")
        private Property<List<String>> groups;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The ids of the created notifications")
        private List<String> notificationIds;

        @Schema(title = "How many notifications were created", description = "One per recipient, after the members of every group are resolved.")
        private Integer count;
    }
}
