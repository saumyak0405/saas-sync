package io.github.saumyak0405.saassync.core;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Minimal timestamped logging to stderr, so stdout stays clean for command output. */
public final class Log {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static volatile boolean quiet = false;

    private Log() {
    }

    public static void setQuiet(boolean value) {
        quiet = value;
    }

    public static void info(String component, String message, Object... args) {
        print("INFO ", component, message, args);
    }

    public static void warn(String component, String message, Object... args) {
        print("WARN ", component, message, args);
    }

    public static void error(String component, String message, Object... args) {
        print("ERROR", component, message, args);
    }

    private static void print(String level, String component, String message, Object... args) {
        if (quiet) {
            return;
        }
        String text = args.length == 0 ? message : String.format(message, args);
        System.err.printf("%s %s [%s] %s%n", LocalTime.now().format(TIME), level, component, text);
    }
}
