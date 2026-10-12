package io.github.saumyak0405.saassync.http;

import java.time.Duration;

/** The API asked us to wait longer than we are willing to block. The next run will continue. */
public final class RateLimitedException extends ApiException {

    private final Duration requestedWait;

    public RateLimitedException(int status, Duration requestedWait, String url) {
        super(status, "Rate limited on " + url + "; server asked to wait " + requestedWait.toSeconds()
                + "s, more than the configured maximum. Stopping this stream; the next run resumes from the last checkpoint.");
        this.requestedWait = requestedWait;
    }

    public Duration requestedWait() {
        return requestedWait;
    }
}
