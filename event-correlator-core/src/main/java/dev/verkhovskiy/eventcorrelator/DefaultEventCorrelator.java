package dev.verkhovskiy.eventcorrelator;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Базовая реализация коррелятора событий поверх абстрактного durable repository. */
public class DefaultEventCorrelator implements EventCorrelator {

  private final EventDefinitionRegistry definitionRegistry;
  private final EventBufferRepository repository;
  private final Clock clock;
  private final EventCorrelationBoundary boundary;

  public DefaultEventCorrelator(
      EventDefinitionRegistry definitionRegistry, EventBufferRepository repository, Clock clock) {
    this(definitionRegistry, repository, clock, new DirectEventCorrelationBoundary());
  }

  public DefaultEventCorrelator(
      EventDefinitionRegistry definitionRegistry,
      EventBufferRepository repository,
      Clock clock,
      EventCorrelationBoundary boundary) {
    this.definitionRegistry =
        Objects.requireNonNull(definitionRegistry, "definitionRegistry must not be null");
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.boundary = Objects.requireNonNull(boundary, "boundary must not be null");
  }

  @Override
  public EventCorrelationResult accept(IncomingEvent<?> event) {
    Objects.requireNonNull(event, "event must not be null");
    EventFlowDefinition flow = definitionRegistry.requireFlow(event.flowName());
    EventTypeDefinition<?> definition = flow.requireEvent(event.eventType());
    return boundary.execute(
        event.flowName(),
        event.correlationKey(),
        () -> acceptInsideBoundary(event, flow, definition));
  }

  private EventCorrelationResult acceptInsideBoundary(
      IncomingEvent<?> event, EventFlowDefinition flow, EventTypeDefinition<?> definition) {
    BufferedEvent bufferedEvent = BufferedEvent.received(event);

    if (!repository.insertIfAbsent(bufferedEvent)) {
      return EventCorrelationResult.duplicate(bufferedEvent.pointer());
    }

    EventCorrelationResult result = correlate(flow, definition, bufferedEvent);
    if (result.status() == EventCorrelationStatus.PROCESSED) {
      drainPending(flow, event.correlationKey());
    }
    return result;
  }

  private EventCorrelationResult correlate(
      EventFlowDefinition flow, EventTypeDefinition<?> definition, BufferedEvent event) {
    List<String> missingDependencies = missingDependencies(flow, definition, event);
    if (!missingDependencies.isEmpty()) {
      Instant expiresAt = Instant.now(clock).plus(flow.orphanRetention());
      repository.markPending(
          event.pointer(), "missing dependencies: " + missingDependencies, expiresAt);
      return EventCorrelationResult.pending(event.pointer(), missingDependencies);
    }

    try {
      definition.handleUntyped(event.payload());
      repository.markProcessed(event.pointer());
      return EventCorrelationResult.processed(event.pointer());
    } catch (RuntimeException e) {
      repository.markFailed(event.pointer(), e.getMessage());
      return EventCorrelationResult.failed(event.pointer(), e.getMessage());
    }
  }

  private void drainPending(EventFlowDefinition flow, String correlationKey) {
    boolean progressed;
    do {
      progressed = false;
      List<BufferedEvent> pendingEvents = repository.findPending(flow.flowName(), correlationKey);
      for (BufferedEvent pendingEvent : pendingEvents) {
        EventTypeDefinition<?> pendingDefinition = flow.requireEvent(pendingEvent.eventType());
        EventCorrelationResult result = correlate(flow, pendingDefinition, pendingEvent);
        if (result.status() == EventCorrelationStatus.PROCESSED) {
          progressed = true;
        }
      }
    } while (progressed);
  }

  private List<String> missingDependencies(
      EventFlowDefinition flow, EventTypeDefinition<?> definition, BufferedEvent event) {
    List<String> missing = new ArrayList<>();
    for (String requiredEventType : definition.requiredEventTypes()) {
      if (!repository.existsProcessed(flow.flowName(), requiredEventType, event.correlationKey())) {
        missing.add(requiredEventType);
      }
    }
    return missing;
  }
}
