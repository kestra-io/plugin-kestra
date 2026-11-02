package io.kestra.plugin.kestra.ee.cases;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kestra.core.runners.RunContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Offline unit test for the {@code labels.system.caseId} label-resolution path (Correction 1):
 * runs with no Kestra instance, so it stays green regardless of {@link SetTicketTest} being red.
 */
@ExtendWith(MockitoExtension.class)
class SetTicketCaseIdFromLabelTest {

    @Mock
    private RunContext runContext;

    @Test
    void resolvesTheCaseIdFromTheTwoHopNestedLabelMap() {
        Mockito.doReturn(Map.of("labels", Map.of("system", Map.of("caseId", "case-123")))).when(runContext).getVariables();

        assertThat(new SetTicket().caseIdFromLabel(runContext)).isEqualTo("case-123");
    }

    @Test
    void returnsNullWhenThereIsNoLabelsVariable() {
        Mockito.doReturn(Map.of()).when(runContext).getVariables();

        assertThat(new SetTicket().caseIdFromLabel(runContext)).isNull();
    }

    @Test
    void returnsNullWhenLabelsHasNoSystemMap() {
        Mockito.doReturn(Map.of("labels", Map.of("other", "value"))).when(runContext).getVariables();

        assertThat(new SetTicket().caseIdFromLabel(runContext)).isNull();
    }

    @Test
    void returnsNullWhenTheLabelIsAFlatDottedKeyInsteadOfNested() {
        Mockito.doReturn(Map.of("labels", Map.of("system.caseId", "case-123"))).when(runContext).getVariables();

        assertThat(new SetTicket().caseIdFromLabel(runContext)).isNull();
    }
}
