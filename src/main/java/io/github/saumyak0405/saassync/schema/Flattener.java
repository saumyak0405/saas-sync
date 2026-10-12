package io.github.saumyak0405.saassync.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Turns a nested JSON record into flat columns.
 *
 * <p>{@code {"billing": {"address": {"city": "Agra"}}}} becomes a column
 * {@code billing__address__city}. Objects nested deeper than {@code maxDepth}, arrays, and
 * fields listed as JSON fields are kept whole in a JSONB column.
 *
 * <p>Column names are made safe for Postgres: lowercased, non-alphanumerics replaced with
 * {@code _}, and cut to Postgres's 63-byte identifier limit with a hash suffix so two long
 * names that share a prefix don't collapse into the same column.
 */
public final class Flattener {

    public static final String SEPARATOR = "__";
    /** Columns the engine adds to every table; source fields with these names are renamed. */
    public static final Set<String> RESERVED = Set.of("_raw_json", "_is_deleted", "_synced_at");
    static final int MAX_IDENTIFIER_LENGTH = 63;

    private final int maxDepth;

    public Flattener(int maxDepth) {
        if (maxDepth < 1) {
            throw new IllegalArgumentException("maxDepth must be >= 1");
        }
        this.maxDepth = maxDepth;
    }

    /**
     * @return column name to value, where a value is a {@link Long}, {@link BigDecimal},
     *         {@link Boolean}, {@link String}, {@link JsonNode} (for JSONB) or {@code null}
     */
    public Map<String, Object> flatten(ObjectNode record, Set<String> jsonFields) {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, String> pathByColumn = new LinkedHashMap<>();
        flattenInto(out, pathByColumn, "", record, 1, jsonFields);
        return out;
    }

    private void flattenInto(Map<String, Object> out, Map<String, String> pathByColumn, String prefix,
                             JsonNode node, int depth, Set<String> jsonFields) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String path = prefix.isEmpty() ? field.getKey() : prefix + SEPARATOR + field.getKey();
            JsonNode value = field.getValue();
            boolean keepWhole = depth == 1 && jsonFields.contains(field.getKey());
            if (value.isObject() && !value.isEmpty() && depth < maxDepth && !keepWhole) {
                flattenInto(out, pathByColumn, path, value, depth + 1, jsonFields);
            } else {
                String column = uniqueColumn(path, pathByColumn);
                out.put(column, toValue(value));
            }
        }
    }

    private static String uniqueColumn(String path, Map<String, String> pathByColumn) {
        String column = columnName(path);
        String owner = pathByColumn.putIfAbsent(column, path);
        if (owner == null || owner.equals(path)) {
            return column;
        }
        // Two different fields sanitize to the same name (e.g. "+1" and "-1"): disambiguate deterministically.
        String disambiguated = withHash(column, path);
        pathByColumn.put(disambiguated, path);
        return disambiguated;
    }

    /** Postgres-safe column name for a field path. */
    public static String columnName(String path) {
        String name = path.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
        if (name.isEmpty() || Character.isDigit(name.charAt(0))) {
            name = "_" + name;
        }
        if (RESERVED.contains(name)) {
            name = name + "_src";
        }
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            name = withHash(name, path);
        }
        return name;
    }

    private static String withHash(String name, String path) {
        String suffix = "_" + shortHash(path);
        int keep = Math.min(name.length(), MAX_IDENTIFIER_LENGTH - suffix.length());
        return name.substring(0, keep) + suffix;
    }

    private static String shortHash(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 4);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static Object toValue(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        if (value.isBoolean()) {
            return value.booleanValue();
        }
        if (value.isIntegralNumber()) {
            return value.canConvertToLong() ? (Object) value.longValue() : new BigDecimal(value.bigIntegerValue());
        }
        if (value.isNumber()) {
            return value.decimalValue();
        }
        if (value.isTextual()) {
            return value.textValue();
        }
        return value; // arrays, objects at max depth, empty objects
    }
}
