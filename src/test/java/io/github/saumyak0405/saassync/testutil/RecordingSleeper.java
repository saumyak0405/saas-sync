package io.github.saumyak0405.saassync.testutil;

import io.github.saumyak0405.saassync.http.Sleeper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Records requested waits instead of sleeping, so retry tests run instantly. */
public final class RecordingSleeper implements Sleeper {

    public final List<Duration> sleeps = new ArrayList<>();

    @Override
    public void sleep(Duration duration) {
        sleeps.add(duration);
    }
}
