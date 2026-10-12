package io.github.saumyak0405.saassync.testutil;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Emitter;
import io.github.saumyak0405.saassync.core.Record;

import java.util.ArrayList;
import java.util.List;

/** Captures what a source emits, for testing sources without a destination. */
public final class RecordingEmitter implements Emitter {

    public final List<Record> records = new ArrayList<>();
    public final List<ObjectNode> checkpoints = new ArrayList<>();

    @Override
    public void emit(Record record) {
        records.add(record);
    }

    @Override
    public void checkpoint(ObjectNode state) {
        checkpoints.add(state.deepCopy());
    }

    public ObjectNode lastCheckpoint() {
        return checkpoints.isEmpty() ? null : checkpoints.get(checkpoints.size() - 1);
    }

    public List<String> ids(String field) {
        return records.stream().map(r -> r.data().path(field).asText()).toList();
    }
}
