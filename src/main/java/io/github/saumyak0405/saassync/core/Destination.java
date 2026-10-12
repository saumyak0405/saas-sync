package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;

/** Where synced records and sync state are stored. */
public interface Destination extends AutoCloseable {

    /** Returns the saved state for a stream, or an empty object if it has never synced. */
    ObjectNode loadState(String source, String stream);

    /**
     * Upserts {@code records} and saves {@code state} atomically: both land or neither does.
     *
     * @param state the new state, or {@code null} to leave the saved state unchanged
     */
    void write(String source, StreamSpec stream, List<Record> records, ObjectNode state);

    /** Forgets a stream's state, so the next run does a full reload. */
    void resetState(String source, String stream);

    /** Saved state per stream for one source. */
    Map<String, ObjectNode> states(String source);

    @Override
    default void close() {
    }
}
