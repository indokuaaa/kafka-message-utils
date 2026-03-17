package io.github.indoku.kafka.exception;

public class KafkaUtilsException extends RuntimeException {

    public KafkaUtilsException(String message) {
        super(message);
    }

    public KafkaUtilsException(String message, Throwable cause) {
        super(message, cause);
    }
}
