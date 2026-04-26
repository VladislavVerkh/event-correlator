package dev.verkhovskiy.eventcorrelator.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Конфигурационные свойства event-correlator. */
@ConfigurationProperties(prefix = "event.correlator")
public class EventCorrelatorProperties {

  private boolean enabled = true;
  private int expirationBatchSize = 100;

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
}
