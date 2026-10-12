package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.destination.InMemoryDestination;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncEngineTest {

    /** Serves 3 pages of 2 records and checkpoints {"page": n} after each; can be told to crash. */
    static final class PagedSource implements Source {
        int crashAfterPage = Integer.MAX_VALUE;
        Set<String> failingStreams = Set.of();
        final List<ObjectNode> statesSeen = new ArrayList<>();

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public List<StreamSpec> streams() {
            return List.of(StreamSpec.of("a", "id"), StreamSpec.of("b", "id"));
        }

        @Override
        public void read(StreamSpec stream, ObjectNode state, Emitter out) throws IOException {
            statesSeen.add(state.deepCopy());
            if (failingStreams.contains(stream.name())) {
                throw new IOException("connection reset");
            }
            for (int page = state.path("page").asInt(0) + 1; page <= 3; page++) {
                if (page > crashAfterPage) {
                    throw new IOException("network went away on page " + page);
                }
                for (int i = 1; i <= 2; i++) {
                    out.emit(Record.upsert(Json.obj().put("id", stream.name() + "-" + page + "-" + i)));
                }
                out.checkpoint(Json.obj().put("page", page));
            }
        }
    }

    /** Records every write so tests can check what was committed together. */
    static final class RecordingDestination implements Destination {
        final InMemoryDestination inner = new InMemoryDestination();
        final List<String> writes = new ArrayList<>();

        @Override
        public ObjectNode loadState(String source, String stream) {
            return inner.loadState(source, stream);
        }

        @Override
        public void write(String source, StreamSpec stream, List<Record> records, ObjectNode state) {
            writes.add(records.size() + " records, state " + (state == null ? "unchanged" : state.toString()));
            inner.write(source, stream, records, state);
        }

        @Override
        public void resetState(String source, String stream) {
            inner.resetState(source, stream);
        }

        @Override
        public java.util.Map<String, ObjectNode> states(String source) {
            return inner.states(source);
        }
    }

    private PagedSource source;
    private RecordingDestination destination;

    @BeforeEach
    void setUp() {
        Log.setQuiet(true);
        source = new PagedSource();
        destination = new RecordingDestination();
    }

    @Test
    void commitsEachCheckpointTogetherWithTheRecordsItCovers() {
        new SyncEngine(destination, 100).run(source, List.of("a"));

        assertEquals(List.of(
                "2 records, state {\"page\":1}",
                "2 records, state {\"page\":2}",
                "2 records, state {\"page\":3}"), destination.writes);
        assertEquals(6, destination.inner.rows("fake", "a").size());
    }

    @Test
    void flushesFullBatchesWithoutAdvancingTheState() {
        new SyncEngine(destination, 1).run(source, List.of("a"));

        // Batch size 1: each record is written as soon as it arrives, but the state only moves
        // at the source's checkpoints.
        assertEquals("1 records, state unchanged", destination.writes.get(0));
        assertEquals("0 records, state {\"page\":1}", destination.writes.get(2));
    }

    @Test
    void aCrashResumesFromTheLastCheckpointWithoutLosingRecords() {
        source.crashAfterPage = 2;
        StreamResult first = new SyncEngine(destination, 100).run(source, List.of("a")).get(0);

        assertFalse(first.succeeded());
        assertTrue(first.error().contains("network went away on page 3"), first.error());
        assertEquals(4, first.records());
        assertEquals(Json.obj().put("page", 2), destination.loadState("fake", "a"));

        source.crashAfterPage = Integer.MAX_VALUE;
        StreamResult second = new SyncEngine(destination, 100).run(source, List.of("a")).get(0);

        assertTrue(second.succeeded());
        assertEquals(Json.obj().put("page", 2), source.statesSeen.get(1), "second run starts where the first stopped");
        assertEquals(2, second.records(), "only page 3 is read again");
        assertEquals(6, destination.inner.rows("fake", "a").size());
    }

    @Test
    void oneFailingStreamDoesNotStopTheOthers() {
        source.failingStreams = Set.of("a");

        List<StreamResult> results = new SyncEngine(destination, 100).run(source, List.of());

        assertEquals(2, results.size());
        assertFalse(results.get(0).succeeded());
        assertTrue(results.get(1).succeeded());
        assertEquals(6, destination.inner.rows("fake", "b").size());
    }

    @Test
    void rejectsUnknownStreamNames() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new SyncEngine(destination, 100).run(source, List.of("a", "nope")));

        assertTrue(e.getMessage().contains("nope"), e.getMessage());
    }

    @Test
    void upsertsByPrimaryKeySoReplaysDoNotDuplicate() {
        SyncEngine engine = new SyncEngine(destination, 100);
        engine.run(source, List.of("a"));
        destination.resetState("fake", "a");
        engine.run(source, List.of("a"));

        assertEquals(6, destination.inner.rows("fake", "a").size());
    }
}
