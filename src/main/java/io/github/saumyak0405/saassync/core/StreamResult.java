package io.github.saumyak0405.saassync.core;

import java.time.Duration;

/** Outcome of syncing one stream. */
public record StreamResult(String stream, boolean succeeded, long records, long tombstones,
                           Duration duration, String error) {

    public static StreamResult ok(String stream, long records, long tombstones, Duration duration) {
        return new StreamResult(stream, true, records, tombstones, duration, null);
    }

    public static StreamResult failed(String stream, long records, long tombstones, Duration duration, String error) {
        return new StreamResult(stream, false, records, tombstones, duration, error);
    }
}
