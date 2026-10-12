package io.github.saumyak0405.saassync.http;

import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.testutil.MockApi;
import io.github.saumyak0405.saassync.testutil.MockApi.Response;
import io.github.saumyak0405.saassync.testutil.RecordingSleeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiClientTest {

    private static final Instant NOW = Instant.parse("2026-10-12T10:00:00Z");

    private MockApi api;
    private RecordingSleeper sleeper;
    private ApiClient client;

    @BeforeEach
    void setUp() {
        Log.setQuiet(true);
        api = new MockApi();
        sleeper = new RecordingSleeper();
        client = ApiClient.builder(api.baseUrl())
                .auth(Authenticator.bearer("secret-token"))
                .retry(new RetryPolicy(4, Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofMinutes(5), 0, () -> 0))
                .sleeper(sleeper)
                .clock(Clock.fixed(NOW, ZoneOffset.UTC))
                .build();
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    @Test
    void retriesTooManyRequestsHonouringRetryAfter() throws Exception {
        api.sequence("/things",
                Response.status(429, "{\"error\":{\"message\":\"slow down\"}}").withHeader("Retry-After", "7"),
                Response.json("{\"ok\":true}"));

        ApiResponse response = client.get("/things", Query.of());

        assertTrue(response.body().path("ok").asBoolean());
        assertEquals(List.of(Duration.ofSeconds(7)), sleeper.sleeps);
        assertEquals(2, client.requestCount());
    }

    @Test
    void retriesServerErrorsWithExponentialBackoff() throws Exception {
        api.sequence("/things",
                Response.status(503, ""),
                Response.status(502, ""),
                Response.json("{\"ok\":true}"));

        client.get("/things", Query.of());

        assertEquals(List.of(Duration.ofMillis(100), Duration.ofMillis(200)), sleeper.sleeps);
    }

    @Test
    void doesNotRetryClientErrorsAndSurfacesTheApiMessage() {
        api.on("/things", r -> Response.status(400, "{\"error\":{\"message\":\"No such customer: cus_x\"}}"));

        ApiException e = assertThrows(ApiException.class, () -> client.get("/things", Query.of()));

        assertEquals(400, e.status());
        assertTrue(e.getMessage().contains("No such customer: cus_x"), e.getMessage());
        assertEquals(1, api.requestCount());
        assertTrue(sleeper.sleeps.isEmpty());
    }

    @Test
    void waitsUntilGitHubRateLimitResets() throws Exception {
        long reset = NOW.getEpochSecond() + 30;
        api.sequence("/things",
                Response.status(403, "{\"message\":\"API rate limit exceeded\"}")
                        .withHeader("x-ratelimit-remaining", "0")
                        .withHeader("x-ratelimit-reset", Long.toString(reset)),
                Response.json("[]"));

        client.get("/things", Query.of());

        assertEquals(List.of(Duration.ofSeconds(31)), sleeper.sleeps);
    }

    @Test
    void failsFastWhenTheRateLimitResetIsTooFarAway() {
        long reset = NOW.getEpochSecond() + 3600;
        api.on("/things", r -> Response.status(403, "{\"message\":\"API rate limit exceeded\"}")
                .withHeader("x-ratelimit-remaining", "0")
                .withHeader("x-ratelimit-reset", Long.toString(reset)));

        RateLimitedException e = assertThrows(RateLimitedException.class, () -> client.get("/things", Query.of()));

        assertEquals(Duration.ofSeconds(3601), e.requestedWait());
        assertEquals(1, api.requestCount());
    }

    @Test
    void treatsPlainForbiddenAsAPermissionErrorNotARateLimit() {
        api.on("/things", r -> Response.status(403, "{\"message\":\"Resource not accessible by integration\"}")
                .withHeader("x-ratelimit-remaining", "4999"));

        ApiException e = assertThrows(ApiException.class, () -> client.get("/things", Query.of()));

        assertEquals(403, e.status());
        assertEquals(1, api.requestCount());
    }

    @Test
    void givesUpAfterMaxAttempts() {
        api.on("/things", r -> Response.status(500, "{\"message\":\"boom\"}"));

        ApiException e = assertThrows(ApiException.class, () -> client.get("/things", Query.of()));

        assertEquals(500, e.status());
        assertEquals(4, api.requestCount());
        assertEquals(3, sleeper.sleeps.size());
    }

    @Test
    void sendsCredentialsInHeadersAndEncodesRepeatedParams() throws Exception {
        api.on("/v1/events", r -> Response.json("{}"));

        client.get("/v1/events", Query.of().add("types[]", "a.created").add("types[]", "a.updated").add("missing", null));

        MockApi.Request request = api.requests("/v1/events").get(0);
        assertEquals("Bearer secret-token", request.header("Authorization"));
        assertEquals(List.of("a.created", "a.updated"), request.params("types[]"));
        assertNull(request.param("missing"));
    }

    @Test
    void returnsNotModifiedAndPassesConditionalHeaders() throws Exception {
        api.on("/things", r -> "W/\"abc\"".equals(r.header("If-None-Match"))
                ? Response.status(304, "")
                : Response.json("[]"));

        ApiResponse response = client.get("/things", Query.of(), Map.of("If-None-Match", "W/\"abc\""));

        assertTrue(response.notModified());
    }

    @Test
    void parsesTheNextLinkFromALinkHeader() {
        String header = "<https://api.github.com/repositories/1/issues?page=2>; rel=\"next\", "
                + "<https://api.github.com/repositories/1/issues?page=5>; rel=\"last\"";

        assertEquals("https://api.github.com/repositories/1/issues?page=2", ApiResponse.parseNextLink(header).orElseThrow());
        assertTrue(ApiResponse.parseNextLink("<https://x/y?page=1>; rel=\"prev\"").isEmpty());
    }
}
