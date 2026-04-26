package dev.verkhovskiy.eventcorrelator;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Событие, сохраненное во внутреннем durable inbox. */
public record BufferedEvent(
    String flowName,
    String eventType,
    String eventId,
    String correlationKey,
    Object payload,
    Map<String, Object> headers,
    Instant occurredAt,
    Instant receivedAt,
    EventStatus status) {

  public BufferedEvent {
    flowName = requireText(flowName, "flowName");
    eventType = requireText(eventType, "eventType");
    eventId = requireText(eventId, "eventId");
    correlationKey = requireText(correlationKey, "correlationKey");
    payload = Objects.requireNonNull(payload, "payload must not be null");
    headers = Map.copyOf(Objects.requireNonNullElse(headers, Map.of()));
    receivedAt = Objects.requireNonNull(receivedAt, "receivedAt must not be null");
    status = Objects.requireNonNull(status, "status must not be null");
  }

  public static BufferedEvent received(IncomingEvent<?> event) {
    return new BufferedEvent(
        event.flowName(),
        event.eventType(),
        event.eventId(),
        event.correlationKey(),
        event.payload(),
        event.headers(),
        event.occurredAt(),
        event.receivedAt(),
        EventStatus.RECEIVED);
  }

  public EventPointer pointer() {
    return new EventPointer(flowName, eventId);
  }

  public BufferedEvent withStatus(EventStatus nextStatus) {
    return new BufferedEvent(
        flowName,
        eventType,
        eventId,
        correlationKey,
        payload,
        headers,
        occurredAt,
        receivedAt,
        nextStatus);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
