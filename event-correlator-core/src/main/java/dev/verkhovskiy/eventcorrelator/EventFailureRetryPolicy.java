package dev.verkhovskiy.eventcorrelator;

import java.time.Duration;
import java.util.Objects;

/** Политика повторной обработки события после ошибки handler-а. */
public record EventFailureRetryPolicy(int maxAttempts, Duration retryDelay) {

  public EventFailureRetryPolicy {
    if (maxAttempts <= 0) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
    retryDelay = Objects.requireNonNull(retryDelay, "retryDelay must not be null");
    if (retryDelay.isNegative()) {
      throw new IllegalArgumentException("retryDelay must not be negative");
    }
  }

  public static EventFailureRetryPolicy noRetries() {
    return new EventFailureRetryPolicy(1, Duration.ZERO);
  }
}
