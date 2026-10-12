package io.github.saumyak0405.saassync.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;

/**
 * A connector for one SaaS application.
 *
 * <p>Everything app-specific lives behind this interface: authentication, pagination style,
 * and how to find what changed since the last run. Scheduling, batching, retries of the
 * whole run, checkpoint storage and writing to the warehouse belong to the engine, so adding
 * a new application means implementing this one interface.
 */
public interface Source {

    /** Short identifier, also used as the destination schema name, e.g. {@code stripe}. */
    String name();

    List<StreamSpec> streams();

    /**
     * Reads {@code stream} starting from {@code state} (empty on the first run) and sends
     * records and checkpoints to {@code out}. Implementations must call
     * {@link Emitter#checkpoint} at least once at the end of a successful read.
     */
    void read(StreamSpec stream, ObjectNode state, Emitter out) throws IOException, InterruptedException;
}
