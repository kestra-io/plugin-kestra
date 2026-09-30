package io.kestra.plugin.kestra.ee.cases;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Offline unit test for the {@code url} extraction in {@link CreateCase#toOutput}. */
class CreateCaseOutputTest {

    @Test
    void populatesUrlWhenPresentInTheResponse() {
        CreateCase.Output output = CreateCase.toOutput(
            Map.of(
                "caseId", "case-123",
                "created", true,
                "url", "http://localhost:8080/ui/main/cases/case-123"
            )
        );

        assertThat(output.getUrl()).isEqualTo("http://localhost:8080/ui/main/cases/case-123");
    }

    @Test
    void leavesUrlNullWhenAbsentFromTheResponse() {
        CreateCase.Output output = CreateCase.toOutput(Map.of("caseId", "case-123", "created", true));

        assertThat(output.getUrl()).isNull();
    }
}
