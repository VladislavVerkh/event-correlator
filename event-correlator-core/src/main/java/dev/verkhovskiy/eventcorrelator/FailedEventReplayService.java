package dev.verkhovskiy.eventcorrelator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Ручно повторно обрабатывает конкретное failed-событие из durable inbox. */
public final class FailedEventReplayService {

  private final EventBufferRepository repository;
  private final EventCorrelator eventCorrelator;
  private final EventCorrelatorObserver observer;

  public FailedEventReplayService(
      EventBufferRepository repository, EventCorrelator eventCorrelator) {
    this(repository, eventCorrelator, EventCorrelatorObserver.NOOP);
  }

  public FailedEventReplayService(
      EventBufferRepository repository,
      EventCorrelator eventCorrelator,
      EventCorrelatorObserver observer) {
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.eventCorrelator =
        Objects.requireNonNull(eventCorrelator, "eventCorrelator must not be null");
    this.observer =
        CompositeEventCorrelatorObserver.of(
            List.of(Objects.requireNonNull(observer, "observer must not be null")));
  }

  /**
   * Захватывает failed-событие по идентификатору и запускает повторную обработку.
   *
   * @return результат replay или `Optional.empty()`, если событие не найдено либо сейчас не
   *     находится в статусе `FAILED`
   */
  public Optional<EventCorrelationResult> replayFailed(String flowName, String eventId) {
    EventPointer pointer = new EventPointer(flowName, eventId);
    Optional<BufferedEvent> event = repository.claimFailedForRetry(pointer);
    observer.failedManualReplayClaimed(pointer, event.isPresent());
    return event.map(eventCorrelator::replay);
  }
}
