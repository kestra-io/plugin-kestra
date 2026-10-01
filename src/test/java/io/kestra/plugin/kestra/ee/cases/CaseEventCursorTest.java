package io.kestra.plugin.kestra.ee.cases;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class CaseEventCursorTest {

    @Test
    void shouldOrderByIdWhenTwoCursorsShareATimestamp() {
        Instant sameInstant = Instant.parse("2026-09-16T10:00:00Z");
        assertThat(new CaseEventCursor(sameInstant, "aaa").isBefore(new CaseEventCursor(sameInstant, "bbb")), is(true));
        assertThat(new CaseEventCursor(sameInstant, "bbb").isBefore(new CaseEventCursor(sameInstant, "aaa")), is(false));
        assertThat(new CaseEventCursor(sameInstant, "aaa").isBefore(new CaseEventCursor(sameInstant, "aaa")), is(false));
    }

    @Test
    void shouldRoundTripThroughItsSerialisedForm() {
        CaseEventCursor cursor = new CaseEventCursor(Instant.parse("2026-09-16T10:00:00.123Z"), "evt-1");
        assertThat(CaseEventCursor.parse(CaseEventCursor.serialise(cursor)), is(Optional.of(cursor)));
    }

    @Test
    void shouldReturnEmptyWhenTheStoredCursorIsUnreadable() {
        assertThat(CaseEventCursor.parse("not-a-cursor"), is(Optional.empty()));
    }

    @Test
    void shouldReturnEmptyWhenTheCursorIsMissingId() {
        assertThat(CaseEventCursor.parse("{\"created\":\"2026-01-01T00:00:00Z\"}"), is(Optional.empty()));
    }

    @Test
    void shouldReturnEmptyWhenTheCursorIsMissingCreated() {
        assertThat(CaseEventCursor.parse("{\"id\":\"evt-1\"}"), is(Optional.empty()));
    }
}
