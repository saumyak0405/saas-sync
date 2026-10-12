package io.github.saumyak0405.saassync.http;

import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Adds credentials to a request. Credentials only ever go in headers, never in URLs,
 * so they cannot leak into logs.
 */
@FunctionalInterface
public interface Authenticator {

    void apply(HttpRequest.Builder request);

    static Authenticator none() {
        return request -> {
        };
    }

    /** HTTP Basic auth. Stripe uses this with the secret key as the username and no password. */
    static Authenticator basic(String username, String password) {
        String token = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return request -> request.header("Authorization", "Basic " + token);
    }

    static Authenticator bearer(String token) {
        return request -> request.header("Authorization", "Bearer " + token);
    }
}
