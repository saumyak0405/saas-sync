package io.github.saumyak0405.saassync.sources.stripe;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.Record;
import io.github.saumyak0405.saassync.core.StreamSpec;
import io.github.saumyak0405.saassync.http.ApiClient;
import io.github.saumyak0405.saassync.http.Authenticator;
import io.github.saumyak0405.saassync.testutil.MockApi;
import io.github.saumyak0405.saassync.testutil.MockApi.Request;
import io.github.saumyak0405.saassync.testutil.MockApi.Response;
import io.github.saumyak0405.saassync.testutil.RecordingEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StripeSourceTest {

    private static final Instant NOW = Instant.parse("2026-10-12T10:00:00Z");
    private static final long T = NOW.getEpochSecond();
    private static final long LAG = StripeSource.VISIBILITY_LAG.toSeconds();
    private static final StreamSpec CUSTOMERS = StreamSpec.of("customers", "id", "metadata");

    private MockApi api;
    private StripeSource source;
    private RecordingEmitter out;

    @BeforeEach
    void setUp() {
        Log.setQuiet(true);
        api = new MockApi();
        ApiClient client = ApiClient.builder(api.baseUrl())
                .auth(Authenticator.basic("sk_test_123", ""))
                .header("Stripe-Version", StripeSource.DEFAULT_API_VERSION)
                .build();
        source = new StripeSource(client, Clock.fixed(NOW, ZoneOffset.UTC));
        out = new RecordingEmitter();

        // Two pages of customers, newest first, like the real list endpoint.
        api.on("/v1/customers", r -> r.param("starting_after") == null
                ? Response.json(list(true, customer("cus_3", "Asha"), customer("cus_2", "Ravi")))
                : Response.json(list(false, customer("cus_1", "Meera"))));
        api.on("/v1/events", r -> Response.json(list(false)));
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    @Test
    void firstRunLoadsEverythingThenSwitchesToTheEventsApi() throws Exception {
        source.read(CUSTOMERS, Json.obj(), out);

        assertEquals(List.of("cus_3", "cus_2", "cus_1"), out.ids("id"));
        List<Request> listCalls = api.requests("/v1/customers");
        assertEquals(2, listCalls.size());
        assertEquals("100", listCalls.get(0).param("limit"));
        assertEquals("cus_2", listCalls.get(1).param("starting_after"));

        // Checkpoint after every page of the full load, so a crash resumes mid-load.
        assertEquals("full", out.checkpoints.get(0).path("phase").asText());
        assertEquals("cus_2", out.checkpoints.get(0).path("starting_after").asText());

        // Changes made while the full load ran are read back from the Events API.
        Request events = api.requests("/v1/events").get(0);
        assertEquals(Long.toString(T - LAG), events.param("created[gte]"));
        assertEquals(List.of("customer.created", "customer.updated", "customer.deleted"), events.params("types[]"));
        assertEquals(state("incremental", T - LAG), out.lastCheckpoint());
    }

    @Test
    void authenticatesWithTheSecretKeyAsBasicAuthUsernameAndPinsTheApiVersion() throws Exception {
        source.read(CUSTOMERS, Json.obj(), out);

        Request first = api.requests("/v1/customers").get(0);
        String expected = "Basic " + Base64.getEncoder().encodeToString("sk_test_123:".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, first.header("Authorization"));
        assertEquals(StripeSource.DEFAULT_API_VERSION, first.header("Stripe-Version"));
    }

    @Test
    void appliesEventsOldestFirstSoTheLatestSnapshotWins() throws Exception {
        // Newest first, split over two pages.
        api.on("/v1/events", r -> r.param("starting_after") == null
                ? Response.json(list(true,
                        event("evt_3", "customer.updated", T - 100, customer("cus_1", "Meera K")),
                        event("evt_2", "customer.updated", T - 200, customer("cus_1", "Meera"))))
                : Response.json(list(false,
                        event("evt_1", "customer.created", T - 300, customer("cus_9", "Dev")))));

        source.read(CUSTOMERS, state("incremental", T - 3600), out);

        assertEquals(List.of("cus_9", "cus_1", "cus_1"), out.ids("id"));
        assertEquals("Meera K", out.records.get(2).data().path("name").asText());
        assertEquals("evt_2", api.requests("/v1/events").get(1).param("starting_after"));
        assertEquals(Long.toString(T - 3600), api.requests("/v1/events").get(0).param("created[gte]"));
        assertTrue(api.requests("/v1/customers").isEmpty(), "incremental runs never re-list");
    }

    @Test
    void deletionEventsBecomeTombstones() throws Exception {
        api.on("/v1/events", r -> Response.json(list(false,
                event("evt_2", "customer.deleted", T - 100, customer("cus_1", "Meera")))));

        source.read(CUSTOMERS, state("incremental", T - 3600), out);

        Record record = out.records.get(0);
        assertTrue(record.deleted());
        assertEquals("cus_1", record.data().path("id").asText());
    }

    @Test
    void skipsEventsWhoseObjectBelongsToAnotherStream() throws Exception {
        ObjectNode card = Json.obj().put("id", "card_1").put("object", "card");
        api.on("/v1/events", r -> Response.json(list(false, event("evt_1", "customer.updated", T - 100, card))));

        source.read(CUSTOMERS, state("incremental", T - 3600), out);

        assertTrue(out.records.isEmpty());
    }

    @Test
    void resumesAnInterruptedFullLoadAfterTheLastSavedId() throws Exception {
        ObjectNode interrupted = Json.obj().put("phase", "full").put("started_at", T - 1000).put("starting_after", "cus_2");

        source.read(CUSTOMERS, interrupted, out);

        assertEquals("cus_2", api.requests("/v1/customers").get(0).param("starting_after"));
        assertEquals(List.of("cus_1"), out.ids("id"));
        // The event window starts at the original load start, not now.
        assertEquals(Long.toString(T - 1000), api.requests("/v1/events").get(0).param("created[gte]"));
    }

    @Test
    void reloadsEverythingWhenTheCursorIsOlderThanEventRetention() throws Exception {
        long stale = T - Duration.ofDays(31).toSeconds();

        source.read(CUSTOMERS, state("incremental", stale), out);

        assertNull(api.requests("/v1/customers").get(0).param("starting_after"));
        assertEquals(3, out.records.size());
        assertEquals(Long.toString(T - LAG), api.requests("/v1/events").get(0).param("created[gte]"));
    }

    @Test
    void quietAccountsStillAdvanceTheCursor() throws Exception {
        source.read(CUSTOMERS, state("incremental", T - Duration.ofDays(20).toSeconds()), out);

        // No events, but everything up to (now - lag) has been checked.
        assertEquals(state("incremental", T - LAG), out.lastCheckpoint());
    }

    @Test
    void subscriptionsAskForAllStatusesSoCanceledOnesAreIncluded() throws Exception {
        api.on("/v1/subscriptions", r -> Response.json(list(false)));

        source.read(StreamSpec.of("subscriptions", "id"), Json.obj(), out);

        assertEquals("all", api.requests("/v1/subscriptions").get(0).param("status"));
    }

    @Test
    void stopsPagingWhenAPageIsEmptyEvenIfHasMoreIsTrue() throws Exception {
        api.on("/v1/customers", r -> Response.json(list(true)));

        source.read(CUSTOMERS, Json.obj(), out);

        assertEquals(1, api.requests("/v1/customers").size());
        assertFalse(out.checkpoints.isEmpty());
    }

    private static ObjectNode state(String phase, long cursor) {
        return Json.obj().put("phase", phase).put("events_cursor", cursor);
    }

    private static ObjectNode customer(String id, String name) {
        ObjectNode c = Json.obj().put("id", id).put("object", "customer").put("name", name);
        c.putObject("metadata").put("plan", "pro");
        return c;
    }

    private static ObjectNode event(String id, String type, long created, ObjectNode object) {
        ObjectNode e = Json.obj().put("id", id).put("object", "event").put("type", type).put("created", created);
        e.putObject("data").set("object", object);
        return e;
    }

    private static String list(boolean hasMore, ObjectNode... items) {
        ObjectNode body = Json.obj().put("object", "list").put("has_more", hasMore);
        var data = body.putArray("data");
        for (ObjectNode item : items) {
            data.add(item);
        }
        return Json.write(body);
    }
}
