package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Where a source sends its output while reading a stream. */
public interface Emitter {

    void emit(Record record);

    /**
     * Declares that every record emitted so far is covered by {@code state}.
     *
     * <p>The engine persists the records and this state in one transaction, so after a crash
     * the next run resumes from the last checkpoint and never skips data. Records emitted
     * after the last checkpoint may be read again on the next run, which is safe because the
     * destination upserts by primary key.
     */
    void checkpoint(ObjectNode state);
}
