package io.kestra.plugin.kestra.ee.reusableinputs;

import java.io.IOException;
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

    /**
     * @param currentSource the source of the latest revision in the exact namespace, or null when the set doesn't exist there
     * @param inputs when set, replaces every input of the set
     * @param defaults when set, changes only the defaults of existing inputs, keyed by input id ({@code form.child} for a FORM child)
     * @throws IllegalArgumentException when creating a set without inputs, or when a default targets an input the set doesn't have
     */
    static String apply(
        @Nullable String currentSource,
        String namespace,
        String id,
        @Nullable String description,
        @Nullable List<Map<String, Object>> inputs,
        @Nullable Map<String, Object> defaults) throws IOException {
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
            defaults.forEach((key, value) -> setDefault(id, inputsOf(set), key, value));
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

    private static void setDefault(String setId, List<Map<String, Object>> inputs, String key, @Nullable Object value) {
        Map<String, Object> input = find(inputs, key);
        if (input == null) {
            throw new IllegalArgumentException("`%s` is not an input of reusable inputs '%s'.".formatted(key, setId));
        }
        if (value == null) {
            input.remove("defaults");
        } else {
            input.put("defaults", value);
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

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> inputsOf(Map<String, Object> parent) {
        return parent.get("inputs") instanceof List<?> list ? (List<Map<String, Object>>) list : List.of();
    }
}
