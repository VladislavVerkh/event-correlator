package dev.verkhovskiy.eventcorrelator;

/** Фильтр для чтения событий из durable inbox. */
public record EventInboxQuery(
    String flowName, String eventType, String correlationKey, EventStatus status, int limit) {

  public EventInboxQuery {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String flowName;
    private String eventType;
    private String correlationKey;
    private EventStatus status;
    private int limit = 100;

    public Builder flowName(String flowName) {
      this.flowName = flowName;
      return this;
    }

    public Builder eventType(String eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder correlationKey(String correlationKey) {
      this.correlationKey = correlationKey;
      return this;
    }

    public Builder status(EventStatus status) {
      this.status = status;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public EventInboxQuery build() {
      return new EventInboxQuery(flowName, eventType, correlationKey, status, limit);
    }
  }
}
