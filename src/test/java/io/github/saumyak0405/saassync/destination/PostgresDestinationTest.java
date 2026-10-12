package io.github.saumyak0405.saassync.destination;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.Record;
import io.github.saumyak0405.saassync.core.StreamSpec;
import io.github.saumyak0405.saassync.schema.Flattener;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs against a real Postgres. Skipped unless PG_TEST_URL is set, e.g.
 * {@code PG_TEST_URL=jdbc:postgresql://localhost:5432/warehouse mvn test} with the docker-compose database.
 */
class PostgresDestinationTest {

    static final String URL = System.getenv("PG_TEST_URL");
    static final String USER = System.getenv().getOrDefault("PG_TEST_USER", "postgres");
    static final String PASSWORD = System.getenv().getOrDefault("PG_TEST_PASSWORD", "postgres");

    private static final StreamSpec CUSTOMERS = StreamSpec.of("customers", "id", "metadata");

    private String source;
    private PostgresDestination destination;
    private Connection check;

    @BeforeEach
    void setUp() throws SQLException {
        assumeTrue(URL != null, "PG_TEST_URL not set");
        Log.setQuiet(true);
        source = "t_" + UUID.randomUUID().toString().substring(0, 8);
        destination = new PostgresDestination(DriverManager.getConnection(URL, USER, PASSWORD), new Flattener(3),
                Clock.systemUTC());
        check = DriverManager.getConnection(URL, USER, PASSWORD);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (check != null) {
            try (Statement st = check.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS " + source + " CASCADE");
                st.execute("DELETE FROM _saas_sync.state WHERE source = '" + source + "'");
            }
            check.close();
        }
        if (destination != null) {
            destination.close();
        }
    }

    @Test
    void upsertsByPrimaryKeyAndKeepsTheRawRecord() throws SQLException {
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "Asha"), customer("cus_2", "Ravi")), null);
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "Asha K")), null);

        assertEquals(List.of(row("cus_1", "Asha K"), row("cus_2", "Ravi")),
                query("SELECT id, name FROM %s.customers ORDER BY id"));
        assertEquals("Asha K", query("SELECT _raw_json->>'name' FROM %s.customers WHERE id = 'cus_1'").get(0).get(0));
        assertEquals("{\"plan\": \"pro\"}", query("SELECT metadata::text FROM %s.customers WHERE id = 'cus_1'").get(0).get(0));
    }

    @Test
    void keepsTheLastVersionWhenABatchHasTheSameKeyTwice() throws SQLException {
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "old"), customer("cus_1", "new")), null);

        assertEquals(List.of(row("cus_1", "new")), query("SELECT id, name FROM %s.customers"));
    }

    @Test
    void addsColumnsWhenNewFieldsAppear() throws SQLException {
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "Asha")), null);
        ObjectNode withAddress = customerData("cus_2", "Ravi");
        withAddress.putObject("address").put("city", "Agra");
        destination.write(source, CUSTOMERS, List.of(Record.upsert(withAddress)), null);

        assertEquals(List.of(row("cus_1", null), row("cus_2", "Agra")),
                query("SELECT id, address__city FROM %s.customers ORDER BY id"));
    }

    @Test
    void widensColumnTypesInsteadOfFailing() throws SQLException {
        destination.write(source, CUSTOMERS, List.of(Record.upsert(Json.obj().put("id", "a").put("code", 5))), null);
        assertEquals("bigint", columnType("code"));

        destination.write(source, CUSTOMERS, List.of(Record.upsert(Json.obj().put("id", "b").put("code", "five"))), null);
        assertEquals("text", columnType("code"));
        assertEquals(List.of(row("a", "5"), row("b", "five")), query("SELECT id, code FROM %s.customers ORDER BY id"));

        ObjectNode withArray = Json.obj().put("id", "c");
        withArray.putArray("code").add("x").add("y");
        destination.write(source, CUSTOMERS, List.of(Record.upsert(withArray)), null);
        assertEquals("jsonb", columnType("code"));
        assertEquals("\"five\"", query("SELECT code::text FROM %s.customers WHERE id = 'b'").get(0).get(0));
    }

    @Test
    void rowsMirrorTheLatestSnapshot() throws SQLException {
        ObjectNode first = customerData("cus_1", "Asha");
        first.put("phone", "+91 99999 00000");
        destination.write(source, CUSTOMERS, List.of(Record.upsert(first)), null);
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "Asha")), null);

        assertNull(query("SELECT phone FROM %s.customers").get(0).get(0), "a field that disappeared is cleared");
    }

    @Test
    void tombstonesFlagTheRowAndKeepItsLastValues() throws SQLException {
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "Asha")), null);
        destination.write(source, CUSTOMERS,
                List.of(Record.tombstone(Json.obj().put("id", "cus_1").put("deleted", true))), null);

        assertEquals(List.of(row("Asha", "deleted")),
                query("SELECT name, CASE WHEN _is_deleted THEN 'deleted' ELSE 'live' END FROM %s.customers"));
    }

    @Test
    void rollsBackRowsSchemaChangesAndStateTogether() throws SQLException {
        destination.write(source, CUSTOMERS, List.of(customer("cus_1", "Asha")), Json.obj().put("cursor", 1));

        // Make the database reject one specific row; any database-side error would do.
        try (Statement st = check.createStatement()) {
            st.execute("ALTER TABLE " + source + ".customers ADD CONSTRAINT no_rejects CHECK (name <> 'reject me')");
        }

        ObjectNode good = customerData("cus_2", "Ravi");
        good.put("extra", "new column");
        ObjectNode bad = customerData("cus_3", "reject me");
        assertThrows(DestinationException.class, () -> destination.write(source, CUSTOMERS,
                List.of(Record.upsert(good), Record.upsert(bad)), Json.obj().put("cursor", 2)));

        assertEquals(List.of(row("cus_1")), query("SELECT id FROM %s.customers"));
        assertNull(columnType("extra"), "the ALTER TABLE was rolled back too");
        assertEquals(Json.obj().put("cursor", 1), destination.loadState(source, "customers"));

        // The column cache was invalidated, so the next write re-adds the column.
        destination.write(source, CUSTOMERS, List.of(Record.upsert(good)), Json.obj().put("cursor", 2));
        assertEquals(List.of(row("cus_2", "new column")), query("SELECT id, extra FROM %s.customers WHERE extra IS NOT NULL"));
    }

    @Test
    void stripsNulCharactersPostgresWouldReject() throws SQLException {
        ObjectNode dirty = customerData("cus_1", "As\u0000ha");

        destination.write(source, CUSTOMERS, List.of(Record.upsert(dirty)), null);

        assertEquals("Asha", query("SELECT name FROM %s.customers").get(0).get(0));
    }

    @Test
    void storesAndResetsStatePerStream() {
        destination.write(source, CUSTOMERS, List.of(), Json.obj().put("phase", "incremental").put("events_cursor", 42));
        destination.write(source, StreamSpec.of("charges", "id"), List.of(), Json.obj().put("phase", "full"));

        Map<String, ObjectNode> states = destination.states(source);
        assertEquals(List.of("charges", "customers"), new ArrayList<>(states.keySet()));
        assertEquals(42, destination.loadState(source, "customers").path("events_cursor").asInt());

        destination.resetState(source, "customers");
        assertTrue(destination.loadState(source, "customers").isEmpty());
        assertFalse(destination.loadState(source, "charges").isEmpty());
    }

    private static Record customer(String id, String name) {
        return Record.upsert(customerData(id, name));
    }

    private static ObjectNode customerData(String id, String name) {
        ObjectNode c = Json.obj().put("id", id).put("object", "customer").put("name", name);
        c.putObject("metadata").put("plan", "pro");
        return c;
    }

    private static List<String> row(String... values) {
        return Arrays.asList(values);
    }

    private String columnType(String column) throws SQLException {
        List<List<String>> rows = query("SELECT data_type FROM information_schema.columns "
                + "WHERE table_schema = '%s' AND table_name = 'customers' AND column_name = '" + column + "'");
        return rows.isEmpty() ? null : rows.get(0).get(0);
    }

    private List<List<String>> query(String sqlTemplate) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Statement st = check.createStatement(); ResultSet rs = st.executeQuery(sqlTemplate.formatted(source))) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    row.add(rs.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }
}
