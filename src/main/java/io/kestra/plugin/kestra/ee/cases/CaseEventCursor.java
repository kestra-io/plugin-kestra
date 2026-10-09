package io.kestra.plugin.kestra.ee.cases;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;

/**
 * A position in the case-event timeline, persisted to the namespace KV store so a {@link CaseEvent}
 * resumes from where it left off across a worker restart instead of replaying or skipping history.
 */
public record CaseEventCursor(Instant created, String id) {

    public static String serialise(CaseEventCursor cursor) {
        try {
            return JacksonMapper.ofJson().writeValueAsString(cursor);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialise the case-event cursor '%s'.".formatted(cursor), e);
        }
    }

    public static Optional<CaseEventCursor> parse(String raw) {
        try {
            CaseEventCursor cursor = JacksonMapper.ofJson().readValue(raw, CaseEventCursor.class);
            if (cursor.created() == null || cursor.id() == null) {
                return Optional.empty();
            }
            return Optional.of(cursor);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static Optional<CaseEventCursor> read(RunContext runContext, String key) throws IOException {
        Optional<KVValue> stored;
        try {
            stored = runContext.namespaceKv(runContext.flowInfo().namespace()).getValue(key);
        } catch (ResourceExpiredException e) {
            return Optional.empty();
        }
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
            Optional.of(stored.get().value())
                .filter(String.class::isInstance)
                .flatMap(raw -> parse((String) raw))
                .orElseThrow(() -> new IllegalStateException("The persisted case-event cursor under key '%s' is unreadable.".formatted(key)))
        );
    }

    public static void write(RunContext runContext, String key, CaseEventCursor cursor, Duration ttl) {
        try {
            runContext.namespaceKv(runContext.flowInfo().namespace())
                .put(key, new KVValueAndMetadata(new KVMetadata("CaseEvent cursor", ttl), serialise(cursor)));
        } catch (Exception e) {
            runContext.logger().warn("Failed to persist the case-event cursor for key '{}': {}", key, e.getMessage());
        }
    }

    public static String defaultKey(String namespace, String flowId, String triggerId) {
        return "kestra_case_event_cursor_" + String.join("_", namespace, flowId, triggerId);
    }
}
