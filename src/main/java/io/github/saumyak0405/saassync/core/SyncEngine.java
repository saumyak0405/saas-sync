package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runs a source against a destination: loads each stream's saved state, lets the source read
 * from there, and writes records in batches together with the source's checkpoints.
 *
 * <p>Streams are isolated: a failure in one stream is reported and the remaining streams still
 * sync. Whatever a failed stream committed before failing stays committed, and its next run
 * resumes from its last checkpoint.
 */
public final class SyncEngine {

    private static final String LOG = "engine";

    private final Destination destination;
    private final int batchSize;

    public SyncEngine(Destination destination, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.destination = destination;
        this.batchSize = batchSize;
    }

    /** Syncs the named streams, or all streams when {@code only} is empty. */
    public List<StreamResult> run(Source source, Collection<String> only) {
        List<StreamSpec> selected = select(source, only);
        List<StreamResult> results = new ArrayList<>();
        for (StreamSpec spec : selected) {
            results.add(runStream(source, spec));
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        }
        return results;
    }

    static List<StreamSpec> select(Source source, Collection<String> only) {
        if (only == null || only.isEmpty()) {
            return source.streams();
        }
        Set<String> known = source.streams().stream().map(StreamSpec::name).collect(Collectors.toSet());
        List<String> unknown = only.stream().filter(s -> !known.contains(s)).toList();
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Unknown stream(s) for " + source.name() + ": " + unknown
                    + ". Available: " + source.streams().stream().map(StreamSpec::name).toList());
        }
        return source.streams().stream().filter(s -> only.contains(s.name())).toList();
    }

    private StreamResult runStream(Source source, StreamSpec spec) {
        String label = source.name() + "." + spec.name();
        long started = System.nanoTime();
        ObjectNode state = destination.loadState(source.name(), spec.name());
        Log.info(LOG, "%s: starting (%s)", label, state.isEmpty() ? "first run, full load" : "resuming from " + state);

        BatchingEmitter emitter = new BatchingEmitter(source.name(), spec);
        try {
            source.read(spec, state.deepCopy(), emitter);
            emitter.flushRemaining();
            Duration took = Duration.ofNanos(System.nanoTime() - started);
            Log.info(LOG, "%s: done, %d records (%d deleted) in %d ms",
                    label, emitter.committed, emitter.committedTombstones, took.toMillis());
            return StreamResult.ok(spec.name(), emitter.committed, emitter.committedTombstones, took);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failed(label, spec, emitter, started, "interrupted");
        } catch (Exception e) {
            return failed(label, spec, emitter, started, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private StreamResult failed(String label, StreamSpec spec, BatchingEmitter emitter, long started, String error) {
        Duration took = Duration.ofNanos(System.nanoTime() - started);
        Log.error(LOG, "%s: failed after %d committed records: %s (next run resumes from the last checkpoint)",
                label, emitter.committed, error);
        return StreamResult.failed(spec.name(), emitter.committed, emitter.committedTombstones, took, error);
    }

    /**
     * Buffers records and writes them when the buffer fills or the source checkpoints.
     * A checkpoint is only ever saved in the same transaction as the records it covers.
     */
    private final class BatchingEmitter implements Emitter {
        private final String sourceName;
        private final StreamSpec spec;
        private final List<Record> buffer = new ArrayList<>();
        long committed;
        long committedTombstones;

        BatchingEmitter(String sourceName, StreamSpec spec) {
            this.sourceName = sourceName;
            this.spec = spec;
        }

        @Override
        public void emit(Record record) {
            buffer.add(record);
            if (buffer.size() >= batchSize) {
                // Records past the last checkpoint: write them, keep the old state.
                flush(null);
            }
        }

        @Override
        public void checkpoint(ObjectNode state) {
            flush(state.deepCopy());
        }

        void flushRemaining() {
            if (!buffer.isEmpty()) {
                flush(null);
            }
        }

        private void flush(ObjectNode state) {
            List<Record> batch = List.copyOf(buffer);
            destination.write(sourceName, spec, batch, state);
            buffer.clear();
            committed += batch.size();
            committedTombstones += batch.stream().filter(Record::deleted).count();
        }
    }
}
