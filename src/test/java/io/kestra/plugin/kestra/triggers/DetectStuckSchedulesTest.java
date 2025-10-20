package io.kestra.plugin.kestra.triggers;

import io.kestra.plugin.kestra.triggers.DetectStuckSchedules;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class DetectStuckSchedulesTest extends AbstractKestraContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    public void shouldDetectSchedulesSuccessfully() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        // Create instance of the custom task
        DetectStuckSchedules task = DetectStuckSchedules.builder()
            .thresholdMinutes(Property.ofValue(60))
            .build();

        // Run the task
        DetectStuckSchedules.Output output = task.run(runContext);

        // Validate output
        assertThat(output, notNullValue());
        assertThat(output.getTotalChecked(), greaterThanOrEqualTo(0));
        assertThat(output.getStuckTriggers(), notNullValue());
        assertThat(output.getMisconfiguredTriggers(), notNullValue());

        runContext.logger().info("✅ Test executed successfully with output: {}", output.toMap());
    }
}
