package io.github.saumyak0405.saassync.schema;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.Locale;

/**
 * The column types the engine creates, and how they widen when a field's values disagree.
 *
 * <p>APIs are loosely typed: a field that was always an integer can arrive as a decimal, and an
 * "expandable" Stripe field can be a string ID in one record and an object in the next. Instead of
 * failing the sync, a column is widened to a type that holds both, and existing values are converted.
 */
public enum ColumnType {
    BIGINT("bigint"),
    NUMERIC("numeric"),
    BOOLEAN("boolean"),
    TEXT("text"),
    JSONB("jsonb");

    private final String sql;

    ColumnType(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }

    /** Type for a flattened value, or {@code null} for SQL NULL (which says nothing about the type). */
    public static ColumnType of(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return BIGINT;
        }
        if (value instanceof BigDecimal) {
            return NUMERIC;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof String) {
            return TEXT;
        }
        if (value instanceof JsonNode) {
            return JSONB;
        }
        throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
    }

    /**
     * Smallest type that can hold values of both types: bigint + numeric = numeric; anything with
     * jsonb = jsonb (a scalar is valid JSON); any other mix = text.
     */
    public ColumnType widen(ColumnType other) {
        if (other == null || other == this) {
            return this;
        }
        if ((this == BIGINT && other == NUMERIC) || (this == NUMERIC && other == BIGINT)) {
            return NUMERIC;
        }
        if (this == JSONB || other == JSONB) {
            return JSONB;
        }
        return TEXT;
    }

    /** SQL expression converting an existing column of this type to {@code target}. */
    public String conversionTo(ColumnType target, String quotedColumn) {
        return switch (target) {
            case JSONB -> "to_jsonb(" + quotedColumn + ")";
            case TEXT -> quotedColumn + "::text";
            case NUMERIC -> quotedColumn + "::numeric";
            default -> throw new IllegalArgumentException("Cannot convert " + this + " to " + target);
        };
    }

    /** Maps {@code information_schema.columns.data_type} back to a ColumnType. */
    public static ColumnType fromInformationSchema(String dataType) {
        return switch (dataType.toLowerCase(Locale.ROOT)) {
            case "bigint" -> BIGINT;
            case "numeric" -> NUMERIC;
            case "boolean" -> BOOLEAN;
            case "jsonb" -> JSONB;
            default -> TEXT;
        };
    }
}
