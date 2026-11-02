package io.kestra.plugin.kestra.ee.cases;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Offline unit test for {@link SetTicket#caseNotFoundOrEndpointMissingMessage}. */
class SetTicketNotFoundMessageTest {

    @Test
    void namesBothPossibleCausesAlongWithTheCaseAndTenant() {
        String message = SetTicket.caseNotFoundOrEndpointMissingMessage("main", "case-123");

        assertThat(message)
            .contains("/api/v1/main/cases/case-123/ticket")
            .contains("case 'case-123' does not exist in tenant 'main'")
            .contains("does not expose the case-ticket endpoint");
    }
}
