package io.kestra.plugin.kestra.ee.cases;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;

/**
 * A position in the case-event timeline, persisted to the namespace KV store so a {@link CaseEvent}
 * resumes from where it left off across a worker restart instead of replaying or skipping history.
 */
public record CaseEventCursor(Instant created, String id) {

    public boolean isBefore(CaseEventCursor other) {
        int comparison = this.created.compareTo(other.created);
        return comparison != 0 ? comparison < 0 : this.id.compareTo(other.id) < 0;
    }

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

    public static Optional<CaseEventCursor> read(RunContext runContext, String key) {
        try {
            return runContext.namespaceKv(runContext.flowInfo().namespace())
                .getValue(key)
                .flatMap(kv -> kv.value() instanceof String raw ? parse(raw) : Optional.empty());
        } catch (Exception e) {
            runContext.logger().warn("Failed to read the persisted case-event cursor for key '{}': {}", key, e.getMessage());
            return Optional.empty();
        }
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
