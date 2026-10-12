package io.github.saumyak0405.saassync.http;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Ordered query parameters. Keys may repeat, e.g. Stripe's {@code types[]=a&types[]=b}. */
public final class Query {

    private final List<Map.Entry<String, String>> params = new ArrayList<>();

    public static Query of() {
        return new Query();
    }

    public Query add(String key, Object value) {
        if (value != null) {
            params.add(Map.entry(key, String.valueOf(value)));
        }
        return this;
    }

    public String encode() {
        return params.stream()
                .map(e -> enc(e.getKey()) + "=" + enc(e.getValue()))
                .collect(Collectors.joining("&"));
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
