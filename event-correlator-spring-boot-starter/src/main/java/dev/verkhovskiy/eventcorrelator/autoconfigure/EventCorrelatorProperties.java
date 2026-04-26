package dev.verkhovskiy.eventcorrelator.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Конфигурационные свойства event-correlator. */
@ConfigurationProperties(prefix = "event.correlator")
public class EventCorrelatorProperties {

  private boolean enabled = true;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
}
