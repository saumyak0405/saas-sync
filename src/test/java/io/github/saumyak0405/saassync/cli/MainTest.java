package io.github.saumyak0405.saassync.cli;

import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.testutil.MockApi;
import io.github.saumyak0405.saassync.testutil.MockApi.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8);

    @BeforeEach
    void setUp() {
        Log.setQuiet(true);
    }

    @Test
    void printsUsageWithoutArguments() {
        assertEquals(Main.USAGE, Main.run(new String[0], Map.of(), out));
        assertTrue(output().contains("Usage:"));
    }

    @Test
    void refusesALiveStripeKeyUnlessExplicitlyAllowed() {
        int code = Main.run(new String[]{"sync", "stripe", "--dry-run"}, Map.of("STRIPE_API_KEY", "sk_live_abc"), out);

        assertEquals(Main.USAGE, code);
        assertTrue(output().contains("live-mode key"), output());
    }

    @Test
    void reportsMissingConfiguration() {
        assertEquals(Main.USAGE, Main.run(new String[]{"sync", "github"}, Map.of(), out));
        assertTrue(output().contains("GITHUB_REPO is not set"), output());
    }

    @Test
    void dryRunSyncsAgainstTheApiWithoutADatabase() {
        try (MockApi github = new MockApi()) {
            github.on("/repos/acme/widgets/issues", r -> Response.json(
                    "[{\"id\": 1, \"number\": 1, \"updated_at\": \"2026-01-01T00:00:00Z\"}]"));

            int code = Main.run(new String[]{"sync", "github", "--streams", "issues", "--dry-run"},
                    Map.of("GITHUB_REPO", "acme/widgets", "GITHUB_BASE_URL", github.baseUrl()), out);

            assertEquals(Main.OK, code, output());
            assertTrue(output().contains("github.issues"), output());
            assertTrue(output().matches("(?s).*github\\.issues\\s+ok\\s+1\\s.*"), output());
        }
    }

    @Test
    void exitsNonZeroWhenAStreamFails() {
        try (MockApi github = new MockApi()) {
            github.on("/repos/acme/widgets/issues", r -> Response.status(404, "{\"message\":\"Not Found\"}"));

            int code = Main.run(new String[]{"sync", "github", "--streams", "issues", "--dry-run"},
                    Map.of("GITHUB_REPO", "acme/widgets", "GITHUB_BASE_URL", github.baseUrl()), out);

            assertEquals(Main.SYNC_FAILED, code);
            assertTrue(output().contains("FAILED"), output());
            assertTrue(output().contains("Not Found"), output());
        }
    }

    private String output() {
        return buffer.toString(StandardCharsets.UTF_8);
    }
}
