package io.github.saumyak0405.saassync.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Destination;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.Source;
import io.github.saumyak0405.saassync.core.StreamResult;
import io.github.saumyak0405.saassync.core.StreamSpec;
import io.github.saumyak0405.saassync.core.SyncEngine;
import io.github.saumyak0405.saassync.destination.InMemoryDestination;
import io.github.saumyak0405.saassync.destination.PostgresDestination;
import io.github.saumyak0405.saassync.sources.github.GitHubSource;
import io.github.saumyak0405.saassync.sources.stripe.StripeSource;

import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Command-line entry point. Configuration comes from environment variables so secrets stay out of shell history. */
public final class Main {

    static final int OK = 0;
    static final int SYNC_FAILED = 1;
    static final int USAGE = 2;

    private static final String USAGE_TEXT = """
            Usage:
              saas-sync sync    <stripe|github> [--streams a,b] [--dry-run] [--every <minutes>]
              saas-sync state   <stripe|github>
              saas-sync reset   <stripe|github> [--streams a,b]
              saas-sync streams <stripe|github>

            Environment:
              PG_URL             JDBC URL (default jdbc:postgresql://localhost:5432/warehouse)
              PG_USER            default postgres
              PG_PASSWORD        default postgres
              BATCH_SIZE         records per transaction (default 500)
              STRIPE_API_KEY     test-mode secret or restricted key (sk_test_... or rk_test_...)
              STRIPE_ALLOW_LIVE  set to true to allow a live-mode key
              GITHUB_REPO        owner/name
              GITHUB_TOKEN       optional; raises the rate limit from 60 to 5,000 requests/hour
            """;

    private Main() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.getenv(), System.out));
    }

    static int run(String[] args, Map<String, String> env, PrintStream out) {
        if (args.length < 2) {
            out.print(USAGE_TEXT);
            return USAGE;
        }
        String command = args[0];
        Options options;
        Source source;
        try {
            options = Options.parse(Arrays.copyOfRange(args, 2, args.length));
            source = createSource(args[1], env);
        } catch (IllegalArgumentException e) {
            out.println("error: " + e.getMessage());
            out.println();
            out.print(USAGE_TEXT);
            return USAGE;
        }

        if (command.equals("streams")) {
            for (StreamSpec spec : source.streams()) {
                out.printf("%-15s primary key: %s%n", spec.name(), spec.primaryKey());
            }
            return OK;
        }

        try (Destination destination = options.dryRun ? new InMemoryDestination() : connect(env)) {
            return switch (command) {
                case "sync" -> sync(source, destination, options, env, out);
                case "state" -> printState(source, destination, out);
                case "reset" -> reset(source, destination, options, out);
                default -> {
                    out.println("error: unknown command " + command);
                    out.print(USAGE_TEXT);
                    yield USAGE;
                }
            };
        } catch (IllegalArgumentException e) {
            out.println("error: " + e.getMessage());
            return USAGE;
        } catch (RuntimeException e) {
            out.println("error: " + e.getMessage());
            return SYNC_FAILED;
        }
    }

    static Source createSource(String name, Map<String, String> env) {
        return switch (name) {
            case "stripe" -> {
                String key = required(env, "STRIPE_API_KEY");
                boolean live = key.startsWith("sk_live_") || key.startsWith("rk_live_");
                if (live && !"true".equalsIgnoreCase(env.get("STRIPE_ALLOW_LIVE"))) {
                    throw new IllegalArgumentException(
                            "STRIPE_API_KEY is a live-mode key. Use a test-mode key, or set STRIPE_ALLOW_LIVE=true.");
                }
                yield StripeSource.create(key,
                        env.getOrDefault("STRIPE_API_VERSION", StripeSource.DEFAULT_API_VERSION),
                        env.getOrDefault("STRIPE_BASE_URL", StripeSource.DEFAULT_BASE_URL));
            }
            case "github" -> GitHubSource.create(required(env, "GITHUB_REPO"), env.get("GITHUB_TOKEN"),
                    env.getOrDefault("GITHUB_BASE_URL", GitHubSource.DEFAULT_BASE_URL));
            default -> throw new IllegalArgumentException("unknown source '" + name + "' (expected stripe or github)");
        };
    }

    private static Destination connect(Map<String, String> env) {
        return PostgresDestination.connect(
                env.getOrDefault("PG_URL", "jdbc:postgresql://localhost:5432/warehouse"),
                env.getOrDefault("PG_USER", "postgres"),
                env.getOrDefault("PG_PASSWORD", "postgres"));
    }

    private static int sync(Source source, Destination destination, Options options, Map<String, String> env,
                            PrintStream out) {
        int batchSize = Integer.parseInt(env.getOrDefault("BATCH_SIZE", "500"));
        SyncEngine engine = new SyncEngine(destination, batchSize);
        while (true) {
            List<StreamResult> results = engine.run(source, options.streams);
            printSummary(source, results, out);
            boolean allOk = results.stream().allMatch(StreamResult::succeeded);
            if (options.every == null || Thread.currentThread().isInterrupted()) {
                return allOk ? OK : SYNC_FAILED;
            }
            Log.info("cli", "next run in %d minutes", options.every.toMinutes());
            try {
                Thread.sleep(options.every);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return allOk ? OK : SYNC_FAILED;
            }
        }
    }

    static void printSummary(Source source, List<StreamResult> results, PrintStream out) {
        out.printf("%n%-22s %-7s %9s %8s %9s%n", "stream", "status", "records", "deleted", "time");
        for (StreamResult r : results) {
            out.printf("%-22s %-7s %9d %8d %8.1fs%n", source.name() + "." + r.stream(),
                    r.succeeded() ? "ok" : "FAILED", r.records(), r.tombstones(), r.duration().toMillis() / 1000.0);
        }
        results.stream().filter(r -> !r.succeeded())
                .forEach(r -> out.printf("  %s: %s%n", r.stream(), r.error()));
    }

    private static int printState(Source source, Destination destination, PrintStream out) {
        Map<String, ObjectNode> states = destination.states(source.name());
        for (StreamSpec spec : source.streams()) {
            ObjectNode state = states.get(spec.name());
            out.printf("%-15s %s%n", spec.name(), state == null ? "(never synced)" : Json.write(state));
        }
        return OK;
    }

    private static int reset(Source source, Destination destination, Options options, PrintStream out) {
        List<String> streams = options.streams.isEmpty()
                ? source.streams().stream().map(StreamSpec::name).toList()
                : options.streams;
        for (String stream : streams) {
            destination.resetState(source.name(), stream);
            out.printf("%s.%s: state cleared, next sync does a full reload%n", source.name(), stream);
        }
        return OK;
    }

    private static String required(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is not set");
        }
        return value;
    }

    record Options(List<String> streams, boolean dryRun, Duration every) {

        static Options parse(String[] args) {
            List<String> streams = new ArrayList<>();
            boolean dryRun = false;
            Duration every = null;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--streams" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--streams needs a value");
                        }
                        Arrays.stream(args[++i].split(",")).map(String::trim).filter(s -> !s.isEmpty())
                                .forEach(streams::add);
                    }
                    case "--dry-run" -> dryRun = true;
                    case "--every" -> {
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("--every needs a number of minutes");
                        }
                        long minutes = Long.parseLong(args[++i]);
                        if (minutes < 1) {
                            throw new IllegalArgumentException("--every must be at least 1 minute");
                        }
                        every = Duration.ofMinutes(minutes);
                    }
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            return new Options(List.copyOf(streams), dryRun, every);
        }
    }
}
