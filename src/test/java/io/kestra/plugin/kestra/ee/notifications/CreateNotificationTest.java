package io.kestra.plugin.kestra.ee.notifications;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class CreateNotificationTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void failsWhenEveryRecipientListIsEmpty() {
        RunContext runContext = runContextFactory.of();

        CreateNotification task = CreateNotification.builder()
            .tenantId(Property.ofValue("main"))
            .title(Property.ofValue("order_sync failed"))
            .recipients(
                CreateNotification.RecipientsProperty.builder()
                    .users(Property.ofValue(List.of()))
                    .groups(Property.ofValue(List.of()))
                    .build()
            )
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("recipients must contain at least one user or group");
    }
}
