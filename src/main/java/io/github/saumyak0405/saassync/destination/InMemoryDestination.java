package io.github.saumyak0405.saassync.destination;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Destination;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Record;
import io.github.saumyak0405.saassync.core.StreamSpec;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Keeps everything in memory. Used for {@code --dry-run} and in tests; it follows the same
 * upsert-by-primary-key and tombstone rules as the Postgres destination.
 */
public final class InMemoryDestination implements Destination {

    private final Map<String, Map<String, Record>> rows = new TreeMap<>();
    private final Map<String, ObjectNode> states = new TreeMap<>();
    private int writes;

    @Override
    public synchronized ObjectNode loadState(String source, String stream) {
        ObjectNode state = states.get(key(source, stream));
        return state == null ? Json.obj() : state.deepCopy();
    }

    @Override
    public synchronized void write(String source, StreamSpec stream, List<Record> records, ObjectNode state) {
        Map<String, Record> table = rows.computeIfAbsent(key(source, stream.name()), k -> new LinkedHashMap<>());
        for (Record record : records) {
            String id = record.data().path(stream.primaryKey()).asText();
            Record existing = table.get(id);
            if (record.deleted() && existing != null) {
                // Deletion flags the row and keeps its last known values.
                table.put(id, Record.tombstone(existing.data()));
            } else {
                table.put(id, record);
            }
        }
        if (state != null) {
            states.put(key(source, stream.name()), state.deepCopy());
        }
        writes++;
    }

    @Override
    public synchronized void resetState(String source, String stream) {
        states.remove(key(source, stream));
    }

    @Override
    public synchronized Map<String, ObjectNode> states(String source) {
        Map<String, ObjectNode> result = new LinkedHashMap<>();
        states.forEach((k, v) -> {
            if (k.startsWith(source + ".")) {
                result.put(k.substring(source.length() + 1), v.deepCopy());
            }
        });
        return result;
    }

    /** Current rows of a stream, by primary key. */
    public synchronized Map<String, Record> rows(String source, String stream) {
        return new LinkedHashMap<>(rows.getOrDefault(key(source, stream), Map.of()));
    }

    /** Number of write calls (each one is a transaction in a real destination). */
    public synchronized int writeCount() {
        return writes;
    }

    private static String key(String source, String stream) {
        return source + "." + stream;
    }
}
