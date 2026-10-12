package io.github.saumyak0405.saassync.schema;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.saumyak0405.saassync.core.Json;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlattenerTest {

    private final Flattener flattener = new Flattener(3);

    @Test
    void flattensNestedObjectsIntoDoubleUnderscoreColumns() {
        Map<String, Object> flat = flattener.flatten(Json.parseObject("""
                {"id": "ch_1", "billing": {"address": {"city": "Agra", "geo": {"lat": 27.1}}}}
                """), Set.of());

        assertEquals("ch_1", flat.get("id"));
        assertEquals("Agra", flat.get("billing__address__city"));
        // Depth 3 reached: the rest stays as JSON instead of producing ever-longer column names.
        assertEquals(Json.parse("{\"lat\": 27.1}"), flat.get("billing__address__geo"));
    }

    @Test
    void keepsListedFieldsAndArraysAsJson() {
        Map<String, Object> flat = flattener.flatten(Json.parseObject("""
                {"metadata": {"order-id": "42", "Plan": "pro"}, "labels": [{"name": "bug"}]}
                """), Set.of("metadata"));

        assertEquals(Set.of("metadata", "labels"), flat.keySet());
        assertTrue(flat.get("metadata") instanceof JsonNode);
        assertTrue(flat.get("labels") instanceof JsonNode);
    }

    @Test
    void mapsJsonScalarsToExactJavaTypes() {
        Map<String, Object> flat = flattener.flatten(Json.parseObject("""
                {"amount": 1999, "rate": 0.10, "huge": 123456789012345678901234567890, "paid": true, "note": null}
                """), Set.of());

        assertEquals(1999L, flat.get("amount"));
        assertEquals(new BigDecimal("0.10"), flat.get("rate"));
        assertEquals(new BigDecimal("123456789012345678901234567890"), flat.get("huge"));
        assertEquals(Boolean.TRUE, flat.get("paid"));
        assertNull(flat.get("note"));
        assertTrue(flat.containsKey("note"));
    }

    @Test
    void makesColumnNamesSafeForPostgres() {
        assertEquals("weird_key_name", Flattener.columnName("Weird-Key.Name"));
        assertEquals("_1st_place", Flattener.columnName("1st_place"));
        assertEquals("_raw_json_src", Flattener.columnName("_raw_json"));
    }

    @Test
    void truncatesLongNamesWithoutCollisions() {
        String prefix = "a".repeat(70);
        String first = Flattener.columnName(prefix + "_one");
        String second = Flattener.columnName(prefix + "_two");

        assertTrue(first.length() <= 63, first);
        assertTrue(second.length() <= 63, second);
        assertNotEquals(first, second);
    }

    @Test
    void disambiguatesFieldsThatSanitizeToTheSameName() {
        Map<String, Object> flat = flattener.flatten(Json.parseObject("{\"+1\": 3, \"-1\": 1}"), Set.of());

        assertEquals(2, flat.size());
        assertEquals(3L, flat.get("_1"));
        assertTrue(flat.containsValue(1L));
    }

    @Test
    void columnTypesWidenInsteadOfFailing() {
        assertEquals(ColumnType.NUMERIC, ColumnType.BIGINT.widen(ColumnType.NUMERIC));
        assertEquals(ColumnType.TEXT, ColumnType.BIGINT.widen(ColumnType.TEXT));
        assertEquals(ColumnType.TEXT, ColumnType.BOOLEAN.widen(ColumnType.BIGINT));
        assertEquals(ColumnType.JSONB, ColumnType.TEXT.widen(ColumnType.JSONB));
        assertEquals(ColumnType.BIGINT, ColumnType.BIGINT.widen(null));
    }

    @Test
    void stripsNulCharactersThatPostgresRejects() {
        var dirty = Json.parseObject("{\"name\": \"a\\u0000b\", \"tags\": [\"x\\u0000\"], \"ok\": \"fine\"}");

        var clean = Json.withoutNulChars(dirty);

        assertEquals("ab", clean.path("name").asText());
        assertEquals("x", clean.path("tags").get(0).asText());
        assertEquals("a\u0000b", dirty.path("name").asText(), "the original is left untouched");
    }
}
