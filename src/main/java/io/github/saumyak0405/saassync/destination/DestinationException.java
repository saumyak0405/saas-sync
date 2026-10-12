package io.github.saumyak0405.saassync.destination;

/** A write to the destination failed; the transaction was rolled back. */
public final class DestinationException extends RuntimeException {

    public DestinationException(String message, Throwable cause) {
        super(message, cause);
    }
}
