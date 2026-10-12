package io.github.saumyak0405.saassync.sources.stripe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Emitter;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.Record;
import io.github.saumyak0405.saassync.core.Source;
import io.github.saumyak0405.saassync.core.StreamSpec;
import io.github.saumyak0405.saassync.http.ApiClient;
import io.github.saumyak0405.saassync.http.ApiResponse;
import io.github.saumyak0405.saassync.http.Authenticator;
import io.github.saumyak0405.saassync.http.Query;
import io.github.saumyak0405.saassync.http.RateLimiter;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stripe connector: customers, charges, refunds and subscriptions.
 *
 * <h2>How it finds changes</h2>
 * Stripe's list endpoints can filter by {@code created} but not by "last updated", so a list
 * call can't tell us that an old charge was refunded yesterday. Instead:
 * <ol>
 *   <li><b>Full load</b> (first run): page through the list endpoint with
 *       {@code starting_after}, checkpointing after every page so a crash resumes mid-load.
 *       The load's start time is recorded first.</li>
 *   <li><b>Incremental</b> (every later run): read the Events API from the saved cursor. Each
 *       event carries a full snapshot of the object after the change, so applying events
 *       oldest-first leaves every row at its latest state. Changes that happened during the
 *       full load are picked up here because the cursor starts at the load's start time.</li>
 * </ol>
 * Stripe keeps events for 30 days. If the cursor is older than that, the gap can't be
 * replayed, so the stream falls back to a fresh full load rather than silently missing changes.
 *
 * <h2>State</h2>
 * {@code {"phase":"full","started_at":<epoch s>,"starting_after":"cus_..."}} while loading, then
 * {@code {"phase":"incremental","events_cursor":<epoch s>}}.
 */
public final class StripeSource implements Source {

    public static final String DEFAULT_BASE_URL = "https://api.stripe.com";
    /** Pinned so the shape of responses doesn't change when the account's default version is upgraded. */
    public static final String DEFAULT_API_VERSION = "2024-06-20";

    /** Stripe retains events for 30 days; keep a day of margin. */
    static final Duration EVENT_RETENTION = Duration.ofDays(29);
    /**
     * Events can become visible slightly after their {@code created} time. Each run re-reads this
     * much overlap, which is harmless because writes are idempotent upserts.
     */
    static final Duration VISIBILITY_LAG = Duration.ofMinutes(5);
    static final int PAGE_SIZE = 100;

    private static final String LOG = "stripe";

    /** Per-stream knowledge: where to list it and which events change it. */
    record StreamDef(StreamSpec spec, String listPath, Map<String, String> listParams, String objectType,
                     List<String> eventTypes, Set<String> tombstoneEvents) {
    }

    static final List<StreamDef> STREAMS = List.of(
            new StreamDef(StreamSpec.of("customers", "id", "metadata"), "/v1/customers", Map.of(), "customer",
                    List.of("customer.created", "customer.updated", "customer.deleted"),
                    Set.of("customer.deleted")),
            new StreamDef(StreamSpec.of("charges", "id", "metadata"), "/v1/charges", Map.of(), "charge",
                    List.of("charge.captured", "charge.expired", "charge.failed", "charge.pending",
                            "charge.refunded", "charge.succeeded", "charge.updated"),
                    Set.of()),
            new StreamDef(StreamSpec.of("refunds", "id", "metadata"), "/v1/refunds", Map.of(), "refund",
                    List.of("refund.created", "refund.updated", "refund.failed", "charge.refund.updated"),
                    Set.of()),
            // The subscriptions list hides canceled subscriptions unless asked for status=all.
            // A canceled subscription is still a row (status=canceled), not a deletion.
            new StreamDef(StreamSpec.of("subscriptions", "id", "metadata", "items"), "/v1/subscriptions",
                    Map.of("status", "all"), "subscription",
                    List.of("customer.subscription.created", "customer.subscription.updated",
                            "customer.subscription.deleted", "customer.subscription.paused",
                            "customer.subscription.resumed"),
                    Set.of()));

    private final ApiClient client;
    private final Clock clock;

    public StripeSource(ApiClient client, Clock clock) {
        this.client = client;
        this.clock = clock;
    }

    /** Production setup: Basic auth with the secret key, pinned API version, stays under test mode's 25 req/s. */
    public static StripeSource create(String apiKey, String apiVersion, String baseUrl) {
        ApiClient client = ApiClient.builder(baseUrl)
                .auth(Authenticator.basic(apiKey, ""))
                .header("Stripe-Version", apiVersion)
                .rateLimiter(RateLimiter.perSecond(20))
                .build();
        return new StripeSource(client, Clock.systemUTC());
    }

    @Override
    public String name() {
        return "stripe";
    }

    @Override
    public List<StreamSpec> streams() {
        return STREAMS.stream().map(StreamDef::spec).toList();
    }

    @Override
    public void read(StreamSpec stream, ObjectNode state, Emitter out) throws IOException, InterruptedException {
        StreamDef def = STREAMS.stream().filter(d -> d.spec().name().equals(stream.name())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown stream " + stream.name()));
        String label = name() + "." + stream.name();
        long now = clock.instant().getEpochSecond();

        String phase = state.path("phase").asText("");
        long cursor = phase.equals("incremental") ? state.path("events_cursor").asLong()
                : phase.equals("full") ? state.path("started_at").asLong() : now;
        if (!phase.isEmpty() && now - cursor > EVENT_RETENTION.toSeconds()) {
            Log.warn(LOG, "%s: last sync point %s is older than Stripe's 30-day event retention; "
                    + "changes since then can't be replayed, so reloading everything", label, Instant.ofEpochSecond(cursor));
            phase = "";
        }

        if (!phase.equals("incremental")) {
            boolean resuming = phase.equals("full");
            long startedAt = resuming ? state.path("started_at").asLong() : now - VISIBILITY_LAG.toSeconds();
            String startingAfter = resuming && state.path("starting_after").isTextual()
                    ? state.path("starting_after").asText() : null;
            fullLoad(def, label, startedAt, startingAfter, out);
            cursor = startedAt;
            out.checkpoint(incrementalState(cursor));
        }

        incremental(def, label, cursor, out);
    }

    private void fullLoad(StreamDef def, String label, long startedAt, String startingAfter, Emitter out)
            throws IOException, InterruptedException {
        Log.info(LOG, "%s: full load%s", label, startingAfter == null ? "" : ", resuming after " + startingAfter);
        String after = startingAfter;
        long pages = 0;
        long records = 0;
        while (true) {
            Query query = Query.of().add("limit", PAGE_SIZE);
            def.listParams().forEach(query::add);
            query.add("starting_after", after);
            ApiResponse response = client.get(def.listPath(), query);
            JsonNode data = response.body().path("data");
            for (JsonNode item : data) {
                out.emit(Record.upsert((ObjectNode) item));
            }
            pages++;
            records += data.size();
            if (data.isEmpty()) {
                break;
            }
            after = data.get(data.size() - 1).path("id").asText();
            out.checkpoint(Json.obj().put("phase", "full").put("started_at", startedAt).put("starting_after", after));
            if (!response.body().path("has_more").asBoolean(false)) {
                break;
            }
        }
        Log.info(LOG, "%s: full load read %d records in %d pages", label, records, pages);
    }

    private void incremental(StreamDef def, String label, long cursor, Emitter out)
            throws IOException, InterruptedException {
        long runStartedAt = clock.instant().getEpochSecond();

        // The Events API returns newest first. Collect everything since the cursor, then apply
        // oldest first so that when an object changed several times, its latest snapshot wins.
        List<JsonNode> events = new ArrayList<>();
        String after = null;
        while (true) {
            Query query = Query.of().add("limit", PAGE_SIZE).add("created[gte]", cursor);
            def.eventTypes().forEach(t -> query.add("types[]", t));
            query.add("starting_after", after);
            ApiResponse response = client.get("/v1/events", query);
            JsonNode data = response.body().path("data");
            data.forEach(events::add);
            if (data.isEmpty() || !response.body().path("has_more").asBoolean(false)) {
                break;
            }
            after = data.get(data.size() - 1).path("id").asText();
        }
        Collections.reverse(events);

        long applied = 0;
        long deleted = 0;
        for (JsonNode event : events) {
            JsonNode object = event.path("data").path("object");
            // Some event types (e.g. charge.refunded) carry a different object than this stream's.
            if (!(object instanceof ObjectNode snapshot) || !def.objectType().equals(object.path("object").asText())) {
                continue;
            }
            boolean tombstone = def.tombstoneEvents().contains(event.path("type").asText());
            out.emit(tombstone ? Record.tombstone(snapshot) : Record.upsert(snapshot));
            applied++;
            if (tombstone) {
                deleted++;
            }
        }

        // Everything created before (run start - lag) has been read, so the cursor can move up to
        // there even when no events arrived. Without this, a quiet account would drift past the
        // 30-day retention window and force needless full reloads.
        long newCursor = Math.max(cursor, runStartedAt - VISIBILITY_LAG.toSeconds());
        out.checkpoint(incrementalState(newCursor));
        Log.info(LOG, "%s: applied %d change events (%d deletions) since %s",
                label, applied, deleted, Instant.ofEpochSecond(cursor));
    }

    private static ObjectNode incrementalState(long cursor) {
        return Json.obj().put("phase", "incremental").put("events_cursor", cursor);
    }
}
