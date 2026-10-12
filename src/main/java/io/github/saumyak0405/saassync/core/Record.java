package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * One row emitted by a source.
 *
 * <p>A tombstone ({@code deleted = true}) is still written to the destination, with its
 * {@code _deleted} flag set, so downstream queries can filter deleted rows instead of the
 * row silently vanishing.
 */
public record Record(ObjectNode data, boolean deleted) {

    public Record {
        Objects.requireNonNull(data, "data");
    }

    public static Record upsert(ObjectNode data) {
        return new Record(data, false);
    }

    public static Record tombstone(ObjectNode data) {
        return new Record(data, true);
    }
}
