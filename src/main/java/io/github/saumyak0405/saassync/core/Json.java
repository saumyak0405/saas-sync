package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Shared Jackson setup and small helpers. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            // Exact decimals: never let an amount pass through a double, and keep "1.50" as 1.50.
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);

    private Json() {
    }

    public static ObjectNode obj() {
        return MAPPER.createObjectNode();
    }

    public static JsonNode parse(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid JSON", e);
        }
    }

    public static ObjectNode parseObject(String text) {
        JsonNode node = parse(text);
        if (!(node instanceof ObjectNode objectNode)) {
            throw new IllegalArgumentException("Expected a JSON object");
        }
        return objectNode;
    }

    public static String write(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Removes NUL characters ({@code \u0000}) from every string in the tree. APIs occasionally
     * return them in user-entered text, and Postgres rejects them in both TEXT and JSONB, which
     * would fail the whole batch. Returns the node itself when there is nothing to clean.
     */
    public static ObjectNode withoutNulChars(ObjectNode node) {
        if (!containsNul(node)) {
            return node;
        }
        ObjectNode copy = node.deepCopy();
        stripNul(copy);
        return copy;
    }

    private static boolean containsNul(JsonNode node) {
        if (node.isTextual()) {
            return node.textValue().indexOf('\u0000') >= 0;
        }
        for (JsonNode child : node) {
            if (containsNul(child)) {
                return true;
            }
        }
        return false;
    }

    private static void stripNul(JsonNode node) {
        if (node instanceof ObjectNode object) {
            List<String> fields = new ArrayList<>();
            object.fieldNames().forEachRemaining(fields::add);
            for (String field : fields) {
                JsonNode child = object.get(field);
                if (child.isTextual()) {
                    object.put(field, child.textValue().replace("\u0000", ""));
                } else {
                    stripNul(child);
                }
            }
        } else if (node instanceof ArrayNode array) {
            for (int i = 0; i < array.size(); i++) {
                JsonNode child = array.get(i);
                if (child.isTextual()) {
                    array.set(i, child.textValue().replace("\u0000", ""));
                } else {
                    stripNul(child);
                }
            }
        }
    }
}
