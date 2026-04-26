package dev.verkhovskiy.eventcorrelator;

/** Фильтр для чтения агрегированной статистики durable inbox. */
public record EventInboxStatisticsQuery(String flowName, String eventType, String correlationKey) {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String flowName;
    private String eventType;
    private String correlationKey;

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

    public EventInboxStatisticsQuery build() {
      return new EventInboxStatisticsQuery(flowName, eventType, correlationKey);
    }
  }
}
