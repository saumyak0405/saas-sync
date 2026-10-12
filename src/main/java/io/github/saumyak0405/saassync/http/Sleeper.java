package io.github.saumyak0405.saassync.http;

import java.time.Duration;

/** Waiting, abstracted so tests can verify backoff without actually sleeping. */
@FunctionalInterface
public interface Sleeper {

    Sleeper REAL = Thread::sleep;

    void sleep(Duration duration) throws InterruptedException;
}
