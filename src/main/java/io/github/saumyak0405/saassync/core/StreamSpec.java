package io.github.saumyak0405.saassync.core;

import java.util.Objects;
import java.util.Set;

/**
 * Describes one stream (one destination table) a source can produce.
 *
 * @param name       stream name, also the destination table name
 * @param primaryKey top-level field that uniquely identifies a record; used for upserts
 * @param jsonFields top-level fields stored as a single JSONB column instead of being
 *                   flattened, e.g. user-defined {@code metadata} maps whose keys would
 *                   otherwise turn into an unbounded number of columns
 */
public record StreamSpec(String name, String primaryKey, Set<String> jsonFields) {

    public StreamSpec {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(primaryKey, "primaryKey");
        jsonFields = Set.copyOf(jsonFields);
    }

    public static StreamSpec of(String name, String primaryKey, String... jsonFields) {
        return new StreamSpec(name, primaryKey, Set.of(jsonFields));
    }
}
