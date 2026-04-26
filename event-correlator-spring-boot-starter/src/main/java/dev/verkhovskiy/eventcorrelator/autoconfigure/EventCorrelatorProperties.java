package dev.verkhovskiy.eventcorrelator.autoconfigure;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Конфигурационные свойства event-correlator. */
@ConfigurationProperties(prefix = "event.correlator")
public class EventCorrelatorProperties {

  private boolean enabled = true;
  private int expirationBatchSize = 100;
  private int failedRetryBatchSize = 100;
  private int failureMaxAttempts = 1;
  private Duration failureRetryDelay = Duration.ofMinutes(1);

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getExpirationBatchSize() {
    return expirationBatchSize;
  }

  public void setExpirationBatchSize(int expirationBatchSize) {
    if (expirationBatchSize <= 0) {
      throw new IllegalArgumentException("expirationBatchSize must be positive");
    }
    this.expirationBatchSize = expirationBatchSize;
  }

  public int getFailedRetryBatchSize() {
    return failedRetryBatchSize;
  }

  public void setFailedRetryBatchSize(int failedRetryBatchSize) {
    if (failedRetryBatchSize <= 0) {
      throw new IllegalArgumentException("failedRetryBatchSize must be positive");
    }
    this.failedRetryBatchSize = failedRetryBatchSize;
  }

  public int getFailureMaxAttempts() {
    return failureMaxAttempts;
  }

  public void setFailureMaxAttempts(int failureMaxAttempts) {
    if (failureMaxAttempts <= 0) {
      throw new IllegalArgumentException("failureMaxAttempts must be positive");
    }
    this.failureMaxAttempts = failureMaxAttempts;
  }

  public Duration getFailureRetryDelay() {
    return failureRetryDelay;
  }

  public void setFailureRetryDelay(Duration failureRetryDelay) {
    if (failureRetryDelay == null || failureRetryDelay.isNegative()) {
      throw new IllegalArgumentException("failureRetryDelay must not be negative");
    }
    this.failureRetryDelay = failureRetryDelay;
  }
}
