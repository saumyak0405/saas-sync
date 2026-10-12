package io.github.saumyak0405.saassync.http;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.http.HttpHeaders;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A successful (2xx) or not-modified (304) response with its body parsed as JSON. */
public record ApiResponse(int status, HttpHeaders headers, JsonNode body) {

    private static final Pattern LINK = Pattern.compile("<([^>]+)>\\s*;\\s*rel=\"([^\"]+)\"");

    public boolean notModified() {
        return status == 304;
    }

    public Optional<String> header(String name) {
        return headers.firstValue(name);
    }

    /** The {@code rel="next"} URL from an RFC 8288 Link header, as used by GitHub. */
    public Optional<String> nextLink() {
        return header("Link").flatMap(ApiResponse::parseNextLink);
    }

    static Optional<String> parseNextLink(String linkHeader) {
        Matcher m = LINK.matcher(linkHeader);
        while (m.find()) {
            for (String rel : m.group(2).trim().split("\\s+")) {
                if (rel.equals("next")) {
                    return Optional.of(m.group(1));
                }
            }
        }
        return Optional.empty();
    }
}
