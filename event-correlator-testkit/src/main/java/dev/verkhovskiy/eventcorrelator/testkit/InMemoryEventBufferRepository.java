package dev.verkhovskiy.eventcorrelator.testkit;

import dev.verkhovskiy.eventcorrelator.BufferedEvent;
import dev.verkhovskiy.eventcorrelator.EventBufferRepository;
import dev.verkhovskiy.eventcorrelator.EventPointer;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** In-memory repository для unit-тестов event-correlator flow. */
public class InMemoryEventBufferRepository implements EventBufferRepository {

  private final Map<EventPointer, BufferedEvent> events = new LinkedHashMap<>();
  private final Map<EventPointer, Instant> expiresAtByPointer = new LinkedHashMap<>();

  @Override
  public synchronized boolean insertIfAbsent(BufferedEvent event) {
    if (events.containsKey(event.pointer())) {
      return false;
    }
    events.put(event.pointer(), event);
    return true;
  }

  @Override
  public synchronized void markPending(EventPointer pointer, String reason, Instant expiresAt) {
    expiresAtByPointer.put(pointer, expiresAt);
    updateStatus(pointer, EventStatus.PENDING);
  }

  @Override
  public synchronized void markProcessed(EventPointer pointer) {
    expiresAtByPointer.remove(pointer);
    updateStatus(pointer, EventStatus.PROCESSED);
  }

  @Override
  public synchronized void markFailed(EventPointer pointer, String failureMessage) {
    expiresAtByPointer.remove(pointer);
    updateStatus(pointer, EventStatus.FAILED);
  }

  @Override
  public synchronized void markExpired(EventPointer pointer) {
    expiresAtByPointer.remove(pointer);
    updateStatus(pointer, EventStatus.EXPIRED);
  }

  @Override
  public synchronized boolean existsProcessed(
      String flowName, String eventType, String correlationKey) {
    return events.values().stream()
        .anyMatch(
            event ->
                event.flowName().equals(flowName)
                    && event.eventType().equals(eventType)
                    && event.correlationKey().equals(correlationKey)
                    && event.status() == EventStatus.PROCESSED);
  }

  @Override
  public synchronized List<BufferedEvent> findPending(String flowName, String correlationKey) {
    return events.values().stream()
        .filter(
            event ->
                event.flowName().equals(flowName)
                    && event.correlationKey().equals(correlationKey)
                    && event.status() == EventStatus.PENDING)
        .sorted(Comparator.comparing(BufferedEvent::receivedAt))
        .toList();
  }

  public synchronized List<BufferedEvent> events() {
    return new ArrayList<>(events.values());
  }

  @Override
  public synchronized int expirePendingBefore(Instant now, int limit) {
    List<EventPointer> expiredPointers =
        events.values().stream()
            .filter(event -> event.status() == EventStatus.PENDING)
            .filter(event -> isExpired(event.pointer(), now))
            .sorted(Comparator.comparing(BufferedEvent::receivedAt))
            .limit(limit)
            .map(BufferedEvent::pointer)
            .toList();
    expiredPointers.forEach(this::markExpired);
    return expiredPointers.size();
  }

  private void updateStatus(EventPointer pointer, EventStatus status) {
    BufferedEvent event = events.get(pointer);
    if (event == null) {
      throw new IllegalArgumentException("Unknown event pointer: " + pointer);
    }
    events.put(pointer, event.withStatus(status));
  }

  private boolean isExpired(EventPointer pointer, Instant now) {
    Instant expiresAt = expiresAtByPointer.get(pointer);
    return expiresAt != null && !expiresAt.isAfter(now);
  }
}
