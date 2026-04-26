package dev.verkhovskiy.eventcorrelator;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Входящее событие, нормализованное adapter-ом транспорта. */
public record IncomingEvent<T>(
    String flowName,
    String eventType,
    String eventId,
    String correlationKey,
    T payload,
    Map<String, Object> headers,
    Instant occurredAt,
    Instant receivedAt) {

  public IncomingEvent {
    flowName = requireText(flowName, "flowName");
    eventType = requireText(eventType, "eventType");
    eventId = requireText(eventId, "eventId");
    correlationKey = requireText(correlationKey, "correlationKey");
    payload = Objects.requireNonNull(payload, "payload must not be null");
    headers = Map.copyOf(Objects.requireNonNullElse(headers, Map.of()));
    receivedAt = Objects.requireNonNull(receivedAt, "receivedAt must not be null");
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static final class Builder<T> {
    private String flowName;
    private String eventType;
    private String eventId;
    private String correlationKey;
    private T payload;
    private Map<String, Object> headers = Map.of();
    private Instant occurredAt;
    private Instant receivedAt = Instant.now();

    public Builder<T> flowName(String flowName) {
      this.flowName = flowName;
      return this;
    }

    public Builder<T> eventType(String eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder<T> eventId(String eventId) {
      this.eventId = eventId;
      return this;
    }

    public Builder<T> correlationKey(String correlationKey) {
      this.correlationKey = correlationKey;
      return this;
    }

    public Builder<T> payload(T payload) {
      this.payload = payload;
      return this;
    }

    public Builder<T> headers(Map<String, Object> headers) {
      this.headers = Map.copyOf(Objects.requireNonNullElse(headers, Map.of()));
      return this;
    }

    public Builder<T> occurredAt(Instant occurredAt) {
      this.occurredAt = occurredAt;
      return this;
    }

    public Builder<T> receivedAt(Instant receivedAt) {
      this.receivedAt = receivedAt;
      return this;
    }

    public IncomingEvent<T> build() {
      return new IncomingEvent<>(
          flowName, eventType, eventId, correlationKey, payload, headers, occurredAt, receivedAt);
    }
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
