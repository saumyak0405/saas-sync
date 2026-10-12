package io.github.saumyak0405.saassync.testutil;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** A local HTTP server standing in for a SaaS API in tests. Routes match on exact path. */
public final class MockApi implements AutoCloseable {

    public record Request(String method, String path, Map<String, List<String>> query, Headers headers) {

        public String param(String name) {
            List<String> values = query.get(name);
            return values == null || values.isEmpty() ? null : values.get(0);
        }

        public List<String> params(String name) {
            return query.getOrDefault(name, List.of());
        }

        public String header(String name) {
            return headers.getFirst(name);
        }
    }

    public record Response(int status, String body, Map<String, String> headers) {

        public static Response json(String body) {
            return new Response(200, body, Map.of());
        }

        public static Response status(int status, String body) {
            return new Response(status, body, Map.of());
        }

        public Response withHeader(String name, String value) {
            Map<String, String> copy = new LinkedHashMap<>(headers);
            copy.put(name, value);
            return new Response(status, body, copy);
        }
    }

    private final HttpServer server;
    private final Map<String, Function<Request, Response>> routes = new ConcurrentHashMap<>();
    private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());

    public MockApi() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/", exchange -> {
            Request request = new Request(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                    parseQuery(exchange.getRequestURI().getRawQuery()), exchange.getRequestHeaders());
            requests.add(request);
            Function<Request, Response> route = routes.get(request.path());
            Response response = route == null
                    ? Response.status(404, "{\"message\":\"Not Found\"}")
                    : route.apply(request);
            response.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] body = response.body() == null ? new byte[0] : response.body().getBytes(StandardCharsets.UTF_8);
            if (response.status() == 304 || body.length == 0) {
                exchange.sendResponseHeaders(response.status(), -1);
            } else {
                exchange.sendResponseHeaders(response.status(), body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            }
            exchange.close();
        });
        server.start();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public MockApi on(String path, Function<Request, Response> handler) {
        routes.put(path, handler);
        return this;
    }

    /** Serves the given responses in order, repeating the last one. */
    public MockApi sequence(String path, Response... responses) {
        AtomicInteger next = new AtomicInteger();
        return on(path, r -> responses[Math.min(next.getAndIncrement(), responses.length - 1)]);
    }

    public List<Request> requests(String path) {
        synchronized (requests) {
            return requests.stream().filter(r -> r.path().equals(path)).toList();
        }
    }

    public int requestCount() {
        return requests.size();
    }

    private static Map<String, List<String>> parseQuery(String raw) {
        Map<String, List<String>> query = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return query;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            query.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return query;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
