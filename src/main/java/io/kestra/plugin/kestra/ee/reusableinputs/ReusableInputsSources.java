package io.kestra.plugin.kestra.ee.reusableinputs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.sdk.internal.ApiException;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
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
    interface FileOpener {
        InputStream open(URI uri) throws IOException;
    }

    /**
     * @param currentSource the source of the latest revision in the exact namespace, or null when the set doesn't exist there
     * @param inputs when set, replaces every input of the set
     * @param defaults when set, changes only the defaults of existing inputs, keyed by input id ({@code form.child} for a FORM child)
     * @param files opens the CSV or ION file of a TABLE whose default is a {@code kestra://} URI
     * @throws IllegalArgumentException when creating a set without inputs, when a default targets an input the set doesn't have,
     *         or when a row of a TABLE has a cell that isn't one of its columns
     */
    static String apply(
        @Nullable String currentSource,
        String namespace,
        String id,
        @Nullable String description,
        @Nullable List<Map<String, Object>> inputs,
        @Nullable Map<String, Object> defaults,
        FileOpener files) throws IOException {
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
                setDefault(id, inputsOf(set), entry.getKey(), entry.getValue(), files);
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

    private static void setDefault(String setId, List<Map<String, Object>> inputs, String key, @Nullable Object value, FileOpener files)
        throws IOException {
        Map<String, Object> input = find(inputs, key);
        if (input == null) {
            throw new IllegalArgumentException("`%s` is not an input of reusable inputs '%s'.".formatted(key, setId));
        }
        if (value == null) {
            input.remove("defaults");
        } else if ("TABLE".equals(input.get("type"))) {
            input.put("defaults", tableRows(setId, key, input, value, files));
        } else {
            input.put("defaults", value);
        }
    }

    /** Rows read from a file hold text: each cell is typed after its column, and an empty one is left out. */
    private static Object tableRows(String setId, String tableId, Map<String, Object> table, Object value, FileOpener files) throws IOException {
        Object rows = value instanceof String uri && uri.startsWith("kestra://") ? readRows(setId, tableId, URI.create(uri), files) : value;
        if (!(rows instanceof List<?> list)) {
            return rows;
        }
        Map<String, Object> columnTypes = new LinkedHashMap<>();
        inputsOf(table, "columns").forEach(column -> columnTypes.put(String.valueOf(column.get("id")), column.get("type")));
        list.stream()
            .filter(Map.class::isInstance)
            .flatMap(row -> ((Map<?, ?>) row).keySet().stream())
            .map(String::valueOf)
            .filter(column -> !columnTypes.containsKey(column))
            .findFirst()
            .ifPresent(column ->
            {
                throw new IllegalArgumentException(
                    "`%s` is not a column of `%s` in reusable inputs '%s'. Its columns are: %s."
                        .formatted(column, tableId, setId, String.join(", ", columnTypes.keySet()))
                );
            });
        return list.stream()
            .map(row -> row instanceof Map<?, ?> cells ? typedRow(cells, columnTypes) : row)
            .toList();
    }

    private static List<Map<String, Object>> readRows(String setId, String tableId, URI uri, FileOpener files) throws IOException {
        try (InputStream in = files.open(uri)) {
            if (uri.getPath().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                try {
                    return csvRows(decoded(in.readAllBytes()));
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                        "The CSV file of `%s` in reusable inputs '%s' can't be read: %s".formatted(tableId, setId, e.getMessage()),
                        e
                    );
                }
            }
            return FileSerde.readAll(in, new TypeReference<Map<String, Object>>() {
            }).collectList().block();
        }
    }

    /** UTF-8, or windows-1252 when the bytes aren't UTF-8, as Excel writes a "CSV (comma delimited)" file on Windows. */
    private static String decoded(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
        } catch (CharacterCodingException e) {
            return new String(bytes, Charset.forName("windows-1252"));
        }
    }

    /** The separator is the one the header uses most among comma, semicolon (a French Excel export) and tab. */
    private static List<Map<String, Object>> csvRows(String text) throws IOException {
        String csv = text.startsWith("\uFEFF") ? text.substring(1) : text;
        String header = csv.lines().findFirst().orElse("");
        char separator = Stream.of(',', ';', '\t')
            .max((a, b) -> Long.compare(header.chars().filter(c -> c == a).count(), header.chars().filter(c -> c == b).count()))
            .orElse(',');
        try (CsvReader<CsvRecord> reader = CsvReader.builder().fieldSeparator(separator).skipEmptyLines(true).ofCsvRecord(csv)) {
            List<List<String>> records = reader.stream().map(CsvRecord::getFields).toList();
            if (records.isEmpty()) {
                throw new IllegalArgumentException("the file has no header line");
            }
            List<String> columns = records.getFirst().stream().map(String::trim).toList();
            columns.stream()
                .filter(column -> !column.isEmpty() && columns.indexOf(column) != columns.lastIndexOf(column))
                .findFirst()
                .ifPresent(column ->
                {
                    throw new IllegalArgumentException("`%s` appears more than once in the header".formatted(column));
                });
            return records.stream()
                .skip(1)
                .map(fields ->
                {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 0; i < columns.size(); i++) {
                        if (!columns.get(i).isEmpty()) {
                            row.put(columns.get(i), fields.get(i));
                        }
                    }
                    return row;
                })
                .toList();
        }
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
                case "FLOAT" -> Double.parseDouble(trimmed.matches("-?\\d+,\\d+") ? trimmed.replace(',', '.') : trimmed);
                case "BOOL", "BOOLEAN" -> switch (trimmed.toLowerCase()) {
                    case "true" -> true;
                    case "false" -> false;
                    default -> text;
                };
                case "MULTISELECT" -> trimmed.startsWith("[")
                    ? JSON_MAPPER.readValue(trimmed, new TypeReference<List<Object>>() {
                    })
                    : Arrays.stream(trimmed.split(",")).map(String::trim).filter(value -> !value.isEmpty()).toList();
                default -> text;
            };
        } catch (NumberFormatException | JsonProcessingException e) {
            return text;
        }
    }

    /** An exact id wins, since an input id may itself contain dots, before {@code form.child} is tried at each dot. */
    @Nullable
    private static Map<String, Object> find(List<Map<String, Object>> inputs, String key) {
        for (Map<String, Object> input : inputs) {
            if (key.equals(input.get("id"))) {
                return input;
            }
        }
        for (int dot = key.indexOf('.'); dot >= 0; dot = key.indexOf('.', dot + 1)) {
            Map<String, Object> form = find(inputs, key.substring(0, dot));
            Map<String, Object> child = form == null ? null : find(inputsOf(form), key.substring(dot + 1));
            if (child != null) {
                return child;
            }
        }
        return null;
    }

    private static List<Map<String, Object>> inputsOf(Map<String, Object> parent) {
        return inputsOf(parent, "inputs");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> inputsOf(Map<String, Object> parent, String key) {
        return parent.get(key) instanceof List<?> list ? (List<Map<String, Object>>) list : List.of();
    }
}
