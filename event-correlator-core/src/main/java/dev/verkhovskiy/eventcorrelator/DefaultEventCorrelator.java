package dev.verkhovskiy.eventcorrelator;

import java.time.Clock;
import java.time.Duration;
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
  private final EventFailureRetryPolicy failureRetryPolicy;
  private final EventCorrelatorObserver observer;

  public DefaultEventCorrelator(
      EventDefinitionRegistry definitionRegistry, EventBufferRepository repository, Clock clock) {
    this(
        definitionRegistry,
        repository,
        clock,
        new DirectEventCorrelationBoundary(),
        EventFailureRetryPolicy.noRetries());
  }

  public DefaultEventCorrelator(
      EventDefinitionRegistry definitionRegistry,
      EventBufferRepository repository,
      Clock clock,
      EventCorrelationBoundary boundary) {
    this(definitionRegistry, repository, clock, boundary, EventFailureRetryPolicy.noRetries());
  }

  public DefaultEventCorrelator(
      EventDefinitionRegistry definitionRegistry,
      EventBufferRepository repository,
      Clock clock,
      EventCorrelationBoundary boundary,
      EventFailureRetryPolicy failureRetryPolicy) {
    this(
        definitionRegistry,
        repository,
        clock,
        boundary,
        failureRetryPolicy,
        EventCorrelatorObserver.NOOP);
  }

  public DefaultEventCorrelator(
      EventDefinitionRegistry definitionRegistry,
      EventBufferRepository repository,
      Clock clock,
      EventCorrelationBoundary boundary,
      EventFailureRetryPolicy failureRetryPolicy,
      EventCorrelatorObserver observer) {
    this.definitionRegistry =
        Objects.requireNonNull(definitionRegistry, "definitionRegistry must not be null");
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.boundary = Objects.requireNonNull(boundary, "boundary must not be null");
    this.failureRetryPolicy =
        Objects.requireNonNull(failureRetryPolicy, "failureRetryPolicy must not be null");
    this.observer =
        CompositeEventCorrelatorObserver.of(
            List.of(Objects.requireNonNull(observer, "observer must not be null")));
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

  @Override
  public EventCorrelationResult accept(RawIncomingEvent<?> event) {
    Objects.requireNonNull(event, "event must not be null");
    EventFlowDefinition flow = definitionRegistry.requireFlow(event.flowName());
    EventTypeDefinition<?> definition = flow.requireEvent(event.eventType());
    IncomingEvent<?> correlatedEvent = correlateEvent(event, definition);
    return accept(correlatedEvent);
  }

  @Override
  public EventCorrelationResult replay(BufferedEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    EventFlowDefinition flow = definitionRegistry.requireFlow(event.flowName());
    EventTypeDefinition<?> definition = flow.requireEvent(event.eventType());
    return boundary.execute(
        event.flowName(),
        event.correlationKey(),
        () -> {
          EventCorrelationResult result = correlate(flow, definition, event);
          if (result.status() == EventCorrelationStatus.PROCESSED) {
            drainPending(flow, event.correlationKey());
          }
          return result;
        });
  }

  private IncomingEvent<?> correlateEvent(
      RawIncomingEvent<?> event, EventTypeDefinition<?> definition) {
    return new IncomingEvent<>(
        event.flowName(),
        event.eventType(),
        event.eventId(),
        definition.correlationKeyUntyped(event.payload()),
        event.payload(),
        event.headers(),
        event.occurredAt(),
        event.receivedAt());
  }

  private EventCorrelationResult acceptInsideBoundary(
      IncomingEvent<?> event, EventFlowDefinition flow, EventTypeDefinition<?> definition) {
    BufferedEvent bufferedEvent = BufferedEvent.received(event);

    if (!repository.insertIfAbsent(bufferedEvent)) {
      observer.eventDuplicate(bufferedEvent);
      return EventCorrelationResult.duplicate(bufferedEvent.pointer());
    }
    observer.eventAccepted(bufferedEvent);

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
      observer.eventPending(event.withStatus(EventStatus.PENDING), missingDependencies);
      return EventCorrelationResult.pending(event.pointer(), missingDependencies);
    }

    long startedAt = System.nanoTime();
    try {
      definition.handleUntyped(event.payload());
      repository.markProcessed(event.pointer());
      observer.eventProcessed(event.withStatus(EventStatus.PROCESSED), elapsedSince(startedAt));
      return EventCorrelationResult.processed(event.pointer());
    } catch (RuntimeException e) {
      Instant nextRetryAt = Instant.now(clock).plus(failureRetryPolicy.retryDelay());
      repository.markFailed(
          event.pointer(), e.getMessage(), nextRetryAt, failureRetryPolicy.maxAttempts());
      observer.eventFailed(
          event.withStatus(EventStatus.FAILED), e.getMessage(), elapsedSince(startedAt));
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

  private Duration elapsedSince(long startedAt) {
    return Duration.ofNanos(System.nanoTime() - startedAt);
  }
}
