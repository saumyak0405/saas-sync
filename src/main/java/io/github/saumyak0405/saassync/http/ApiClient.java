package io.github.saumyak0405.saassync.http;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JSON-over-HTTP client shared by all connectors.
 *
 * <p>Handles the failure modes every SaaS API has in common:
 * <ul>
 *   <li>network errors and timeouts: retried with exponential backoff</li>
 *   <li>429 and 5xx responses: retried, honouring {@code Retry-After} when the server sends it</li>
 *   <li>GitHub-style rate limits (403 with {@code x-ratelimit-remaining: 0}): waits until
 *       {@code x-ratelimit-reset}, or fails fast if that is too far away</li>
 *   <li>other 4xx responses: not retried, since repeating a bad request cannot fix it</li>
 * </ul>
 * A client-side token bucket keeps the request rate under the API's limit in the first place.
 */
public final class ApiClient {

    private static final String LOG = "http";

    private final String baseUrl;
    private final Authenticator auth;
    private final Map<String, String> defaultHeaders;
    private final RetryPolicy retry;
    private final RateLimiter limiter;
    private final Sleeper sleeper;
    private final Clock clock;
    private final Duration timeout;
    private final HttpClient http;
    private final AtomicLong requestCount = new AtomicLong();

    private ApiClient(Builder b) {
        this.baseUrl = b.baseUrl.endsWith("/") ? b.baseUrl.substring(0, b.baseUrl.length() - 1) : b.baseUrl;
        this.auth = b.auth;
        this.defaultHeaders = Map.copyOf(b.headers);
        this.retry = b.retry;
        this.limiter = b.limiter;
        this.sleeper = b.sleeper;
        this.clock = b.clock;
        this.timeout = b.timeout;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public static Builder builder(String baseUrl) {
        return new Builder(baseUrl);
    }

    /** Number of HTTP requests sent, including retries. */
    public long requestCount() {
        return requestCount.get();
    }

    public ApiResponse get(String pathOrUrl, Query query) throws IOException, InterruptedException {
        return get(pathOrUrl, query, Map.of());
    }

    /**
     * Sends a GET and returns the response for 2xx and 304.
     *
     * @param pathOrUrl a path relative to the base URL, or an absolute URL (e.g. a Link header's next page)
     * @throws ApiException          for non-retryable errors, or when retries are exhausted
     * @throws RateLimitedException  when the server asks for a wait longer than {@link RetryPolicy#maxServerWait()}
     */
    public ApiResponse get(String pathOrUrl, Query query, Map<String, String> extraHeaders)
            throws IOException, InterruptedException {
        URI uri = resolve(pathOrUrl, query);
        for (int attempt = 1; ; attempt++) {
            limiter.acquire();
            HttpRequest.Builder request = HttpRequest.newBuilder(uri).timeout(timeout).GET()
                    .header("Accept", "application/json");
            defaultHeaders.forEach(request::setHeader);
            extraHeaders.forEach(request::setHeader);
            auth.apply(request);

            HttpResponse<String> response;
            try {
                requestCount.incrementAndGet();
                response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            } catch (IOException e) {
                if (attempt >= retry.maxAttempts()) {
                    throw new IOException("GET " + uri + " failed after " + attempt + " attempts: " + e, e);
                }
                Duration wait = retry.backoff(attempt);
                Log.warn(LOG, "GET %s: %s; retry %d of %d in %d ms",
                        uri, e.getClass().getSimpleName(), attempt, retry.maxAttempts() - 1, wait.toMillis());
                sleeper.sleep(wait);
                continue;
            }

            int status = response.statusCode();
            if (status / 100 == 2 || status == 304) {
                return new ApiResponse(status, response.headers(), parseBody(response.body()));
            }

            boolean rateLimited = status == 429 || (status == 403 && isRateLimited(response.headers()));
            boolean retryable = rateLimited || status == 408 || (status >= 500 && status != 501);
            String detail = errorMessage(response.body());
            if (!retryable) {
                throw new ApiException(status, "GET " + uri + " returned " + status + ": " + detail);
            }

            Optional<Duration> serverWait = serverWait(response.headers());
            if (serverWait.isPresent() && serverWait.get().compareTo(retry.maxServerWait()) > 0) {
                throw new RateLimitedException(status, serverWait.get(), uri.toString());
            }
            if (attempt >= retry.maxAttempts()) {
                throw new ApiException(status, "GET " + uri + " returned " + status + " after "
                        + attempt + " attempts: " + detail);
            }
            final int retryNumber = attempt;
            Duration wait = serverWait.orElseGet(() -> retry.backoff(retryNumber));
            Log.warn(LOG, "GET %s returned %d%s; retry %d of %d in %d ms",
                    uri, status, rateLimited ? " (rate limited)" : "", attempt, retry.maxAttempts() - 1, wait.toMillis());
            sleeper.sleep(wait);
        }
    }

    private URI resolve(String pathOrUrl, Query query) {
        String q = query == null ? "" : query.encode();
        if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) {
            if (q.isEmpty()) {
                return URI.create(pathOrUrl);
            }
            return URI.create(pathOrUrl + (pathOrUrl.contains("?") ? "&" : "?") + q);
        }
        String path = pathOrUrl.startsWith("/") ? pathOrUrl : "/" + pathOrUrl;
        return URI.create(baseUrl + path + (q.isEmpty() ? "" : "?" + q));
    }

    private static boolean isRateLimited(HttpHeaders headers) {
        return headers.firstValue("x-ratelimit-remaining").map("0"::equals).orElse(false)
                || headers.firstValue("retry-after").isPresent();
    }

    /** How long the server asked us to wait, from Retry-After or a rate-limit reset time. */
    Optional<Duration> serverWait(HttpHeaders headers) {
        Optional<String> retryAfter = headers.firstValue("retry-after");
        if (retryAfter.isPresent()) {
            try {
                return Optional.of(Duration.ofSeconds(Math.max(0, Long.parseLong(retryAfter.get().trim()))));
            } catch (NumberFormatException ignored) {
                // HTTP-date form; fall back to our own backoff.
            }
        }
        boolean exhausted = headers.firstValue("x-ratelimit-remaining").map("0"::equals).orElse(false);
        Optional<String> reset = headers.firstValue("x-ratelimit-reset");
        if (exhausted && reset.isPresent()) {
            try {
                long seconds = Long.parseLong(reset.get().trim()) - clock.instant().getEpochSecond() + 1;
                return Optional.of(Duration.ofSeconds(Math.max(1, seconds)));
            } catch (NumberFormatException ignored) {
                // Fall back to our own backoff.
            }
        }
        return Optional.empty();
    }

    private static JsonNode parseBody(String body) {
        if (body == null || body.isBlank()) {
            return Json.MAPPER.missingNode();
        }
        return Json.parse(body);
    }

    /** Pulls the human-readable message out of Stripe ({@code error.message}) or GitHub ({@code message}) errors. */
    private static String errorMessage(String body) {
        if (body == null || body.isBlank()) {
            return "(empty body)";
        }
        try {
            JsonNode json = Json.parse(body);
            if (json.path("error").path("message").isTextual()) {
                return json.path("error").path("message").asText();
            }
            if (json.path("message").isTextual()) {
                return json.path("message").asText();
            }
        } catch (RuntimeException notJson) {
            // fall through
        }
        return body.length() > 200 ? body.substring(0, 200) + "..." : body;
    }

    public static final class Builder {
        private final String baseUrl;
        private Authenticator auth = Authenticator.none();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private RetryPolicy retry = RetryPolicy.defaults();
        private RateLimiter limiter = RateLimiter.unlimited();
        private Sleeper sleeper = Sleeper.REAL;
        private Clock clock = Clock.systemUTC();
        private Duration timeout = Duration.ofSeconds(30);

        private Builder(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public Builder auth(Authenticator value) {
            this.auth = value;
            return this;
        }

        public Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public Builder retry(RetryPolicy value) {
            this.retry = value;
            return this;
        }

        public Builder rateLimiter(RateLimiter value) {
            this.limiter = value;
            return this;
        }

        public Builder sleeper(Sleeper value) {
            this.sleeper = value;
            return this;
        }

        public Builder clock(Clock value) {
            this.clock = value;
            return this;
        }

        public Builder timeout(Duration value) {
            this.timeout = value;
            return this;
        }

        public ApiClient build() {
            return new ApiClient(this);
        }
    }
}
