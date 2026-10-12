package io.github.saumyak0405.saassync.http;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryAndRateLimitTest {

    @Test
    void backoffDoublesUpToTheCap() {
        RetryPolicy policy = new RetryPolicy(10, Duration.ofMillis(100), Duration.ofMillis(1000),
                Duration.ofMinutes(1), 0, () -> 0);

        List<Long> delays = new ArrayList<>();
        for (int attempt = 1; attempt <= 6; attempt++) {
            delays.add(policy.backoff(attempt).toMillis());
        }

        assertEquals(List.of(100L, 200L, 400L, 800L, 1000L, 1000L), delays);
    }

    @Test
    void jitterOnlyEverShortensTheDelay() {
        RetryPolicy maxJitter = new RetryPolicy(10, Duration.ofMillis(100), Duration.ofMillis(1000),
                Duration.ofMinutes(1), 0.5, () -> 1.0);
        RetryPolicy noJitter = new RetryPolicy(10, Duration.ofMillis(100), Duration.ofMillis(1000),
                Duration.ofMinutes(1), 0.5, () -> 0.0);

        // Second retry: 200 ms before jitter; 50% jitter can take off up to half of it.
        assertEquals(100, maxJitter.backoff(2).toMillis());
        assertEquals(200, noJitter.backoff(2).toMillis());
    }

    @Test
    void tokenBucketAllowsABurstThenPacesRequests() throws Exception {
        AtomicLong nanos = new AtomicLong();
        List<Duration> waits = new ArrayList<>();
        RateLimiter limiter = new RateLimiter(2, 1, nanos::get, d -> {
            waits.add(d);
            nanos.addAndGet(d.toNanos());
        });

        limiter.acquire();
        limiter.acquire();
        assertTrue(waits.isEmpty(), "burst of 2 should not wait");

        limiter.acquire();
        assertEquals(List.of(Duration.ofSeconds(1)), waits);

        nanos.addAndGet(Duration.ofSeconds(10).toNanos());
        limiter.acquire();
        limiter.acquire();
        assertEquals(1, waits.size(), "an idle period refills the bucket, but only up to its capacity");
        limiter.acquire();
        assertEquals(2, waits.size());
    }
}
