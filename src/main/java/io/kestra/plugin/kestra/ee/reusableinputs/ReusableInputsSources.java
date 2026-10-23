package io.kestra.plugin.kestra.ee.reusableinputs;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.sdk.internal.ApiException;

import jakarta.annotation.Nullable;

/**
 * Edits the YAML source of a set of reusable inputs the way the {@link Set} task describes it, so that the server
 * validates the result exactly as it validates a save from the editor.
 */
final class ReusableInputsSources {
    private static final ObjectMapper YAML_MAPPER = JacksonMapper.ofYaml();
    private static final ObjectMapper JSON_MAPPER = JacksonMapper.ofJson();

    private ReusableInputsSources() {
    }

    @FunctionalInterface
    interface RowsReader {
        List<Map<String, Object>> read(URI uri) throws IOException;
    }

    /**
     * @param currentSource the source of the latest revision in the exact namespace, or null when the set doesn't exist there
     * @param inputs when set, replaces every input of the set
     * @param defaults when set, changes only the defaults of existing inputs, keyed by input id ({@code form.child} for a FORM child)
     * @param rowsReader reads the rows of a TABLE whose default is a {@code kestra://} URI
     * @throws IllegalArgumentException when creating a set without inputs, or when a default targets an input the set doesn't have
     */
    static String apply(
        @Nullable String currentSource,
        String namespace,
        String id,
        @Nullable String description,
        @Nullable List<Map<String, Object>> inputs,
        @Nullable Map<String, Object> defaults,
        RowsReader rowsReader) throws IOException {
        if (currentSource == null && inputs == null) {
            throw new IllegalArgumentException(
                "Reusable inputs '%s' don't exist in namespace '%s', so `inputs` must be set to create them.".formatted(id, namespace)
            );
        }

        Map<String, Object> set = new LinkedHashMap<>();
        set.put("id", id);
        set.put("namespace", namespace);
        if (currentSource != null) {
            Map<String, Object> current = YAML_MAPPER.readValue(currentSource, new TypeReference<LinkedHashMap<String, Object>>() {
            });
            if (current != null) {
                set.putAll(current);
            }
            set.put("id", id);
            set.put("namespace", namespace);
        }

        if (description != null) {
            set.put("description", description);
        }
        if (inputs != null) {
            set.put("inputs", JSON_MAPPER.convertValue(inputs, new TypeReference<List<LinkedHashMap<String, Object>>>() {
            }));
        }
        if (defaults != null) {
            for (Map.Entry<String, Object> entry : defaults.entrySet()) {
                setDefault(id, inputsOf(set), entry.getKey(), entry.getValue(), rowsReader);
            }
        }

        return YAML_MAPPER.writeValueAsString(set);
    }

    /** The problem detail the API rejected a request with, which carries the same message as the editor. */
    static String errorMessage(ApiException e) {
        try {
            JsonNode detail = JSON_MAPPER.readTree(e.getResponseBody()).get("detail");
            if (detail != null && detail.isTextual()) {
                return detail.asText();
            }
        } catch (IOException | IllegalArgumentException ignored) {
            // not a problem document, the exception message is all there is
        }
        return e.getMessage();
    }

    private static void setDefault(String setId, List<Map<String, Object>> inputs, String key, @Nullable Object value, RowsReader rowsReader)
        throws IOException {
        Map<String, Object> input = find(inputs, key);
        if (input == null) {
            throw new IllegalArgumentException("`%s` is not an input of reusable inputs '%s'.".formatted(key, setId));
        }
        if (value == null) {
            input.remove("defaults");
        } else if ("TABLE".equals(input.get("type"))) {
            input.put("defaults", tableRows(input, value, rowsReader));
        } else {
            input.put("defaults", value);
        }
    }

    /** Rows read from a file, e.g. a CSV turned into ION, hold text: each cell is typed after its column, and an empty one is left out. */
    private static Object tableRows(Map<String, Object> table, Object value, RowsReader rowsReader) throws IOException {
        Object rows = value instanceof String uri && uri.startsWith("kestra://") ? rowsReader.read(URI.create(uri)) : value;
        if (!(rows instanceof List<?> list)) {
            return rows;
        }
        Map<String, Object> columnTypes = new LinkedHashMap<>();
        inputsOf(table, "columns").forEach(column -> columnTypes.put(String.valueOf(column.get("id")), column.get("type")));
        return list.stream()
            .map(row -> row instanceof Map<?, ?> cells ? typedRow(cells, columnTypes) : row)
            .toList();
    }

    private static Map<Object, Object> typedRow(Map<?, ?> cells, Map<String, Object> columnTypes) {
        Map<Object, Object> row = new LinkedHashMap<>();
        cells.forEach((column, cell) -> row.put(column, cell instanceof String text ? typedCell(text, columnTypes.get(String.valueOf(column))) : cell));
        return row;
    }

    @Nullable
    private static Object typedCell(String text, @Nullable Object type) {
        if (text.isBlank()) {
            return null;
        }
        String trimmed = text.trim();
        try {
            return switch (String.valueOf(type)) {
                case "INT" -> Long.parseLong(trimmed);
                case "FLOAT" -> Double.parseDouble(trimmed);
                case "BOOL", "BOOLEAN" -> switch (trimmed.toLowerCase()) {
                    case "true" -> true;
                    case "false" -> false;
                    default -> text;
                };
                default -> text;
            };
        } catch (NumberFormatException e) {
            return text;
        }
    }

    /** An exact id wins, since an input id may itself contain dots, before {@code form.child} is tried. */
    @Nullable
    private static Map<String, Object> find(List<Map<String, Object>> inputs, String key) {
        for (Map<String, Object> input : inputs) {
            if (key.equals(input.get("id"))) {
                return input;
            }
        }
        int dot = key.indexOf('.');
        if (dot < 0) {
            return null;
        }
        Map<String, Object> form = find(inputs, key.substring(0, dot));
        return form == null ? null : find(inputsOf(form), key.substring(dot + 1));
    }

    private static List<Map<String, Object>> inputsOf(Map<String, Object> parent) {
        return inputsOf(parent, "inputs");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> inputsOf(Map<String, Object> parent, String key) {
        return parent.get(key) instanceof List<?> list ? (List<Map<String, Object>>) list : List.of();
    }
}
