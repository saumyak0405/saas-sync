package io.github.saumyak0405.saassync.http;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Token bucket: allows bursts up to {@code capacity} requests, refilling at
 * {@code perSecond} tokens per second.
 *
 * <p>Staying under the API's limit on our side is cheaper than hitting it and backing off,
 * since a 429 costs a round trip and often a penalty wait.
 */
public final class RateLimiter {

    private final double capacity;
    private final double perSecond;
    private final LongSupplier nanoTime;
    private final Sleeper sleeper;
    private double tokens;
    private long lastRefill;

    public RateLimiter(double capacity, double perSecond, LongSupplier nanoTime, Sleeper sleeper) {
        if (capacity < 1 || perSecond <= 0) {
            throw new IllegalArgumentException("capacity must be >= 1 and perSecond > 0");
        }
        this.capacity = capacity;
        this.perSecond = perSecond;
        this.nanoTime = nanoTime;
        this.sleeper = sleeper;
        this.tokens = capacity;
        this.lastRefill = nanoTime.getAsLong();
    }

    public static RateLimiter perSecond(double rate) {
        return new RateLimiter(Math.max(1, rate), rate, System::nanoTime, Sleeper.REAL);
    }

    public static RateLimiter unlimited() {
        return new RateLimiter(Double.MAX_VALUE, Double.MAX_VALUE, System::nanoTime, Sleeper.REAL);
    }

    /** Blocks until a request may be sent. */
    public synchronized void acquire() throws InterruptedException {
        refill();
        while (tokens < 1) {
            double missing = 1 - tokens;
            long waitNanos = (long) Math.ceil(missing / perSecond * 1_000_000_000L);
            sleeper.sleep(Duration.ofNanos(waitNanos));
            refill();
        }
        tokens -= 1;
    }

    private void refill() {
        long now = nanoTime.getAsLong();
        double elapsedSeconds = (now - lastRefill) / 1_000_000_000.0;
        tokens = Math.min(capacity, tokens + elapsedSeconds * perSecond);
        lastRefill = now;
    }
}
