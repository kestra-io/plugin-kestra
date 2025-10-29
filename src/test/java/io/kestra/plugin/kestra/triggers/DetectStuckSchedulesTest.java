package io.kestra.plugin.kestra.triggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.triggers.DetectStuckOrDisabledSchedules.Output;
import io.kestra.core.models.property.Property;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class DetectStuckSchedulesTest extends AbstractKestraContainerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRunSuccessfully() throws Exception {
        // Create a real RunContext from factory
        RunContext runContext = runContextFactory.of();

        // Build the task using its builder
        DetectStuckOrDisabledSchedules task = DetectStuckOrDisabledSchedules.builder()
            .namespace("company.team")
            .threshold(Property.ofValue(Duration.ofMinutes(1))) // matches your plugin definition
            .build();
        Output output = task.run(runContext);


        // Validate output

        assertThat(output, notNullValue());
        assertThat(output.getDisabledTriggers(), notNullValue());
        assertThat(output.getStuckTriggers(), notNullValue());
        assertThat(output.getTotalFound(), greaterThanOrEqualTo(0));

        System.out.println("Test completed successfully. Total issues found: " + output.getTotalFound());
    }
}