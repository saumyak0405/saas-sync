package io.github.saumyak0405.saassync.destination;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Destination;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.Record;
import io.github.saumyak0405.saassync.core.StreamSpec;
import io.github.saumyak0405.saassync.schema.ColumnType;
import io.github.saumyak0405.saassync.schema.Flattener;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Writes each stream to its own table ({@code <source>.<stream>}, e.g. {@code stripe.charges})
 * and keeps sync state in {@code _saas_sync.state}.
 *
 * <ul>
 *   <li><b>Idempotent:</b> rows are upserted by primary key, so re-reading data after a crash or
 *       an overlapping incremental window never creates duplicates.</li>
 *   <li><b>Atomic checkpoints:</b> a batch of rows and the checkpoint covering it are committed in
 *       one transaction. There is no window where the state says "done" but the rows are missing.</li>
 *   <li><b>Schema evolution:</b> a new field adds a column; a field whose type changes widens its
 *       column (see {@link ColumnType#widen}). DDL is transactional in Postgres, so it rolls back
 *       with the batch if anything fails.</li>
 *   <li><b>Rows mirror the latest snapshot:</b> every upsert sets every column, so a field that
 *       became null or disappeared doesn't leave a stale value behind. The full original record
 *       is kept in {@code _raw_json}.</li>
 * </ul>
 */
public final class PostgresDestination implements Destination {

    private static final String LOG = "postgres";
    private static final String STATE_TABLE = "\"_saas_sync\".\"state\"";

    private final Connection conn;
    private final Flattener flattener;
    private final Clock clock;
    /** Known data columns per table, so we don't query information_schema for every batch. */
    private final Map<String, Map<String, ColumnType>> columnsByTable = new HashMap<>();

    public PostgresDestination(Connection conn, Flattener flattener, Clock clock) {
        this.conn = conn;
        this.flattener = flattener;
        this.clock = clock;
        try {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS \"_saas_sync\"");
                st.execute("CREATE TABLE IF NOT EXISTS " + STATE_TABLE + " ("
                        + "source TEXT NOT NULL, stream TEXT NOT NULL, state JSONB NOT NULL, "
                        + "updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (source, stream))");
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new DestinationException("Could not initialise the state table", e);
        }
    }

    public static PostgresDestination connect(String url, String user, String password) {
        try {
            return new PostgresDestination(DriverManager.getConnection(url, user, password), new Flattener(3),
                    Clock.systemUTC());
        } catch (SQLException e) {
            throw new DestinationException("Could not connect to " + url + ": " + e.getMessage(), e);
        }
    }

    @Override
    public ObjectNode loadState(String source, String stream) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT state::text FROM " + STATE_TABLE + " WHERE source = ? AND stream = ?")) {
            ps.setString(1, source);
            ps.setString(2, stream);
            try (ResultSet rs = ps.executeQuery()) {
                ObjectNode state = rs.next() ? Json.parseObject(rs.getString(1)) : Json.obj();
                conn.commit();
                return state;
            }
        } catch (SQLException e) {
            rollbackQuietly();
            throw new DestinationException("Could not load state for " + source + "." + stream, e);
        }
    }

    @Override
    public Map<String, ObjectNode> states(String source) {
        Map<String, ObjectNode> states = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT stream, state::text FROM " + STATE_TABLE + " WHERE source = ? ORDER BY stream")) {
            ps.setString(1, source);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    states.put(rs.getString(1), Json.parseObject(rs.getString(2)));
                }
            }
            conn.commit();
            return states;
        } catch (SQLException e) {
            rollbackQuietly();
            throw new DestinationException("Could not read state for " + source, e);
        }
    }

    @Override
    public void resetState(String source, String stream) {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + STATE_TABLE + " WHERE source = ? AND stream = ?")) {
            ps.setString(1, source);
            ps.setString(2, stream);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new DestinationException("Could not reset state for " + source + "." + stream, e);
        }
    }

    @Override
    public void write(String source, StreamSpec stream, List<Record> records, ObjectNode state) {
        String tableKey = source + "." + stream.name();
        try {
            if (!records.isEmpty()) {
                upsert(source, stream, records);
            }
            if (state != null) {
                saveState(source, stream.name(), state);
            }
            conn.commit();
        } catch (SQLException | RuntimeException e) {
            rollbackQuietly();
            // Any ALTER TABLE in this transaction was rolled back too; re-read columns next time.
            columnsByTable.remove(tableKey);
            String detail = e instanceof SQLException sql ? sql.getMessage() : e.toString();
            throw new DestinationException("Write to " + tableKey + " failed and was rolled back: " + detail, e);
        }
    }

    private void upsert(String source, StreamSpec stream, List<Record> records) throws SQLException {
        String schema = quote(Flattener.columnName(source));
        String table = schema + "." + quote(Flattener.columnName(stream.name()));
        String tableKey = source + "." + stream.name();
        String pkColumn = Flattener.columnName(stream.primaryKey());

        Map<String, ColumnType> columns = ensureTable(tableKey, schema, table, pkColumn);

        // Postgres rejects an INSERT ... ON CONFLICT that touches the same key twice, so keep only
        // the last version of each record in the batch (which is also the newest).
        Map<String, Record> latest = new LinkedHashMap<>();
        for (Record record : records) {
            String key = primaryKey(record.data(), stream.primaryKey(), tableKey);
            latest.remove(key);
            latest.put(key, new Record(Json.withoutNulChars(record.data()), record.deleted()));
        }

        List<Map.Entry<String, Map<String, Object>>> upserts = new ArrayList<>();
        List<Map.Entry<String, Record>> tombstones = new ArrayList<>();
        Map<String, ColumnType> batchTypes = new LinkedHashMap<>();
        for (Map.Entry<String, Record> entry : latest.entrySet()) {
            Record record = entry.getValue();
            if (record.deleted()) {
                tombstones.add(entry);
                continue;
            }
            Map<String, Object> flat = flattener.flatten(record.data(), stream.jsonFields());
            flat.remove(pkColumn);
            flat.forEach((column, value) -> {
                ColumnType type = ColumnType.of(value);
                if (type != null) {
                    batchTypes.merge(column, type, ColumnType::widen);
                }
            });
            upserts.add(Map.entry(entry.getKey(), flat));
        }

        evolveSchema(tableKey, table, columns, batchTypes);
        Timestamp syncedAt = Timestamp.from(clock.instant());
        if (!upserts.isEmpty()) {
            writeUpserts(table, pkColumn, columns, upserts, latest, syncedAt);
        }
        if (!tombstones.isEmpty()) {
            writeTombstones(table, pkColumn, tombstones, syncedAt);
        }
    }

    private Map<String, ColumnType> ensureTable(String tableKey, String schema, String table, String pkColumn)
            throws SQLException {
        Map<String, ColumnType> cached = columnsByTable.get(tableKey);
        if (cached != null) {
            return cached;
        }
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            st.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + quote(pkColumn) + " TEXT PRIMARY KEY, "
                    + "\"_raw_json\" JSONB NOT NULL, "
                    + "\"_is_deleted\" BOOLEAN NOT NULL DEFAULT false, "
                    + "\"_synced_at\" TIMESTAMPTZ NOT NULL)");
        }
        Map<String, ColumnType> columns = new LinkedHashMap<>();
        String[] parts = tableKey.split("\\.", 2);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, Flattener.columnName(parts[0]));
            ps.setString(2, Flattener.columnName(parts[1]));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (!name.equals(pkColumn) && !Flattener.RESERVED.contains(name)) {
                        columns.put(name, ColumnType.fromInformationSchema(rs.getString(2)));
                    }
                }
            }
        }
        columnsByTable.put(tableKey, columns);
        return columns;
    }

    private void evolveSchema(String tableKey, String table, Map<String, ColumnType> columns,
                              Map<String, ColumnType> batchTypes) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (Map.Entry<String, ColumnType> entry : batchTypes.entrySet()) {
                String column = entry.getKey();
                ColumnType incoming = entry.getValue();
                ColumnType existing = columns.get(column);
                if (existing == null) {
                    st.execute("ALTER TABLE " + table + " ADD COLUMN IF NOT EXISTS " + quote(column) + " " + incoming.sql());
                    columns.put(column, incoming);
                    continue;
                }
                ColumnType widened = existing.widen(incoming);
                if (widened != existing) {
                    Log.info(LOG, "%s.%s: widening column from %s to %s (saw a %s value)",
                            tableKey, column, existing.sql(), widened.sql(), incoming.sql());
                    st.execute("ALTER TABLE " + table + " ALTER COLUMN " + quote(column) + " TYPE " + widened.sql()
                            + " USING " + existing.conversionTo(widened, quote(column)));
                    columns.put(column, widened);
                }
            }
        }
    }

    private void writeUpserts(String table, String pkColumn, Map<String, ColumnType> columns,
                              List<Map.Entry<String, Map<String, Object>>> upserts, Map<String, Record> latest,
                              Timestamp syncedAt) throws SQLException {
        List<String> dataColumns = new ArrayList<>(columns.keySet());
        StringJoiner names = new StringJoiner(", ");
        StringJoiner placeholders = new StringJoiner(", ");
        StringJoiner updates = new StringJoiner(", ");
        names.add(quote(pkColumn)).add("\"_raw_json\"").add("\"_is_deleted\"").add("\"_synced_at\"");
        placeholders.add("?").add("?::jsonb").add("false").add("?");
        updates.add("\"_raw_json\" = EXCLUDED.\"_raw_json\"").add("\"_is_deleted\" = false")
                .add("\"_synced_at\" = EXCLUDED.\"_synced_at\"");
        for (String column : dataColumns) {
            names.add(quote(column));
            placeholders.add(columns.get(column) == ColumnType.JSONB ? "?::jsonb" : "?");
            updates.add(quote(column) + " = EXCLUDED." + quote(column));
        }
        String sql = "INSERT INTO " + table + " (" + names + ") VALUES (" + placeholders + ") "
                + "ON CONFLICT (" + quote(pkColumn) + ") DO UPDATE SET " + updates;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Map.Entry<String, Map<String, Object>> upsert : upserts) {
                int i = 1;
                ps.setString(i++, upsert.getKey());
                ps.setString(i++, Json.write(latest.get(upsert.getKey()).data()));
                ps.setTimestamp(i++, syncedAt);
                for (String column : dataColumns) {
                    bind(ps, i++, columns.get(column), upsert.getValue().get(column));
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** A deletion only flags the row; the last known values stay queryable. */
    private void writeTombstones(String table, String pkColumn, List<Map.Entry<String, Record>> tombstones,
                                 Timestamp syncedAt) throws SQLException {
        String sql = "INSERT INTO " + table + " (" + quote(pkColumn) + ", \"_raw_json\", \"_is_deleted\", \"_synced_at\") "
                + "VALUES (?, ?::jsonb, true, ?) ON CONFLICT (" + quote(pkColumn) + ") DO UPDATE SET "
                + "\"_is_deleted\" = true, \"_synced_at\" = EXCLUDED.\"_synced_at\"";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Map.Entry<String, Record> tombstone : tombstones) {
                ps.setString(1, tombstone.getKey());
                ps.setString(2, Json.write(tombstone.getValue().data()));
                ps.setTimestamp(3, syncedAt);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void bind(PreparedStatement ps, int index, ColumnType type, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, switch (type) {
                case BIGINT -> Types.BIGINT;
                case NUMERIC -> Types.NUMERIC;
                case BOOLEAN -> Types.BOOLEAN;
                case TEXT, JSONB -> Types.VARCHAR;
            });
            return;
        }
        switch (type) {
            case BIGINT -> ps.setLong(index, (Long) value);
            case NUMERIC -> ps.setBigDecimal(index, value instanceof Long l ? BigDecimal.valueOf(l) : (BigDecimal) value);
            case BOOLEAN -> ps.setBoolean(index, (Boolean) value);
            case TEXT -> ps.setString(index, value instanceof JsonNode node ? Json.write(node) : value.toString());
            case JSONB -> ps.setString(index, Json.write(value instanceof JsonNode node ? node : Json.MAPPER.valueToTree(value)));
        }
    }

    private void saveState(String source, String stream, ObjectNode state) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + STATE_TABLE + " (source, stream, state, updated_at) VALUES (?, ?, ?::jsonb, ?) "
                        + "ON CONFLICT (source, stream) DO UPDATE SET state = EXCLUDED.state, updated_at = EXCLUDED.updated_at")) {
            ps.setString(1, source);
            ps.setString(2, stream);
            ps.setString(3, Json.write(state));
            ps.setTimestamp(4, Timestamp.from(clock.instant()));
            ps.executeUpdate();
        }
    }

    private static String primaryKey(ObjectNode data, String field, String tableKey) {
        JsonNode value = data.get(field);
        if (value == null || value.isNull() || value.isContainerNode()) {
            throw new IllegalArgumentException("Record in " + tableKey + " has no usable primary key '" + field + "'");
        }
        return value.asText();
    }

    static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private void rollbackQuietly() {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // The original error is more useful than this one.
        }
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException ignored) {
            // Nothing useful to do on shutdown.
        }
    }
}
