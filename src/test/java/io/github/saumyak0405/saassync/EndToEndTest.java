package io.github.saumyak0405.saassync;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.StreamResult;
import io.github.saumyak0405.saassync.core.SyncEngine;
import io.github.saumyak0405.saassync.destination.PostgresDestination;
import io.github.saumyak0405.saassync.schema.Flattener;
import io.github.saumyak0405.saassync.sources.stripe.StripeSource;
import io.github.saumyak0405.saassync.http.ApiClient;
import io.github.saumyak0405.saassync.http.Authenticator;
import io.github.saumyak0405.saassync.testutil.MockApi;
import io.github.saumyak0405.saassync.testutil.MockApi.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Mock Stripe API -> engine -> real Postgres, across three runs. Skipped unless PG_TEST_URL is set. */
class EndToEndTest {

    private static final String URL = System.getenv("PG_TEST_URL");
    private static final String USER = System.getenv().getOrDefault("PG_TEST_USER", "postgres");
    private static final String PASSWORD = System.getenv().getOrDefault("PG_TEST_PASSWORD", "postgres");

    private MockApi stripe;
    private PostgresDestination destination;
    private Connection check;

    @BeforeEach
    void setUp() throws SQLException {
        assumeTrue(URL != null, "PG_TEST_URL not set");
        Log.setQuiet(true);
        stripe = new MockApi();
        check = DriverManager.getConnection(URL, USER, PASSWORD);
        cleanUp();
        destination = new PostgresDestination(DriverManager.getConnection(URL, USER, PASSWORD), new Flattener(3),
                Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (check != null) {
            cleanUp();
            check.close();
        }
        if (destination != null) {
            destination.close();
        }
        if (stripe != null) {
            stripe.close();
        }
    }

    @Test
    void fullLoadThenIncrementalChangesThenAnIdempotentReplay() throws SQLException {
        AtomicReference<String> events = new AtomicReference<>(list());
        stripe.on("/v1/customers", r -> Response.json(list(customer("cus_2", "Ravi"), customer("cus_1", "Asha"))));
        stripe.on("/v1/events", r -> Response.json(events.get()));
        StripeSource source = new StripeSource(ApiClient.builder(stripe.baseUrl())
                .auth(Authenticator.basic("sk_test_x", "")).build(), Clock.systemUTC());
        SyncEngine engine = new SyncEngine(destination, 500);

        // Run 1: full load.
        assertTrue(engine.run(source, List.of("customers")).get(0).succeeded());
        assertEquals(List.of("cus_1 Asha live", "cus_2 Ravi live"), customers());

        // Run 2: Asha is renamed, then Ravi is deleted. Only the Events API is read.
        events.set(list(
                event("evt_2", "customer.deleted", customer("cus_2", "Ravi")),
                event("evt_1", "customer.updated", customer("cus_1", "Asha Kumar"))));
        StreamResult second = engine.run(source, List.of("customers")).get(0);
        assertEquals(2, second.records());
        assertEquals(1, second.tombstones());
        assertEquals(List.of("cus_1 Asha Kumar live", "cus_2 Ravi deleted"), customers());
        assertEquals(1, stripe.requests("/v1/customers").size(), "the list endpoint is only used for the first load");

        // Run 3: the overlap window returns the same events again. Nothing changes.
        engine.run(source, List.of("customers"));
        assertEquals(List.of("cus_1 Asha Kumar live", "cus_2 Ravi deleted"), customers());
        assertEquals("incremental", destination.loadState("stripe", "customers").path("phase").asText());
    }

    private List<String> customers() throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = check.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT id, name, _is_deleted FROM stripe.customers ORDER BY id")) {
            while (rs.next()) {
                rows.add(rs.getString(1) + " " + rs.getString(2) + " " + (rs.getBoolean(3) ? "deleted" : "live"));
            }
        }
        return rows;
    }

    private void cleanUp() throws SQLException {
        try (Statement st = check.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS stripe CASCADE");
            st.execute("CREATE SCHEMA IF NOT EXISTS _saas_sync");
            st.execute("CREATE TABLE IF NOT EXISTS _saas_sync.state (source TEXT NOT NULL, stream TEXT NOT NULL, "
                    + "state JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (source, stream))");
            st.execute("DELETE FROM _saas_sync.state WHERE source = 'stripe'");
        }
    }

    private static ObjectNode customer(String id, String name) {
        return Json.obj().put("id", id).put("object", "customer").put("name", name).put("balance", 0);
    }

    private static ObjectNode event(String id, String type, ObjectNode object) {
        ObjectNode e = Json.obj().put("id", id).put("type", type).put("created", 1);
        e.putObject("data").set("object", object);
        return e;
    }

    private static String list(ObjectNode... items) {
        ObjectNode body = Json.obj().put("object", "list").put("has_more", false);
        ArrayNode data = body.putArray("data");
        for (ObjectNode item : items) {
            data.add(item);
        }
        return Json.write(body);
    }
}
