package io.github.saumyak0405.saassync.http;

/** An API call that failed and will not be retried. */
public class ApiException extends RuntimeException {

    private final int status;

    public ApiException(int status, String message) {
        super(message);
        this.status = status;
    }

    /** HTTP status, or 0 when the request never got a response. */
    public int status() {
        return status;
    }
}
