package io.kestra.plugin.kestra.triggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.triggers.DetectStuckOrDisabledSchedules.Output;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class DetectStuckSchedulesTest extends AbstractKestraContainerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRunSuccessfully() throws Exception {
        // ✅ Create a real RunContext from factory
        RunContext runContext = runContextFactory.of();

        // ✅ Instantiate your task
        DetectStuckOrDisabledSchedules task = new DetectStuckOrDisabledSchedules();

        // ✅ Set namespace reflectively (same as your version)
        var field = task.getClass().getDeclaredField("namespace");
        field.setAccessible(true);
        field.set(task, "company.team");

        // ✅ Run the task with real context
        Output output = task.run(runContext);
        assertThat(output, notNullValue());

        // ✅ Access fields reflectively
        var issuesField = output.getClass().getDeclaredField("issues");
        issuesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> issues = (List<String>) issuesField.get(output);

        var totalField = output.getClass().getDeclaredField("totalFound");
        totalField.setAccessible(true);
        Integer totalFound = (Integer) totalField.get(output);

        // ✅ Validate results
        assertThat(issues, notNullValue());
        assertThat(totalFound, greaterThanOrEqualTo(0));

        System.out.println("✅ Test completed successfully. Found " + totalFound + " issues: " + issues);
    }
}
