package io.github.saumyak0405.saassync.http;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with jitter.
 *
 * <p>The delay doubles with each attempt up to {@code maxDelay}. Jitter spreads retries out
 * so that many clients failing at the same moment don't all retry at the same moment too.
 *
 * @param maxAttempts   total attempts including the first one
 * @param baseDelay     delay before the first retry
 * @param maxDelay      upper bound for a computed backoff delay
 * @param maxServerWait longest wait accepted from a server hint (Retry-After, rate-limit reset).
 *                      Anything longer fails the stream fast instead of blocking; the saved
 *                      checkpoint lets the next scheduled run continue.
 * @param jitter        fraction of the delay that is randomized, 0 (none) to 1 (full jitter)
 */
public record RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay, Duration maxServerWait,
                          double jitter, DoubleSupplier random) {

    public static RetryPolicy defaults() {
        return new RetryPolicy(6, Duration.ofMillis(500), Duration.ofSeconds(30), Duration.ofMinutes(15),
                0.5, () -> ThreadLocalRandom.current().nextDouble());
    }

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (jitter < 0 || jitter > 1) {
            throw new IllegalArgumentException("jitter must be between 0 and 1");
        }
    }

    /** Backoff before retry number {@code attempt} (1 = first retry). */
    public Duration backoff(int attempt) {
        long exp = baseDelay.toMillis() << Math.min(attempt - 1, 20);
        long capped = Math.min(exp, maxDelay.toMillis());
        long jittered = Math.round(capped * (1 - jitter * random.getAsDouble()));
        return Duration.ofMillis(Math.max(jittered, 0));
    }
}
