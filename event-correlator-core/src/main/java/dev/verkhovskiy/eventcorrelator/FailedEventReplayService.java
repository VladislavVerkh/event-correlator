package dev.verkhovskiy.eventcorrelator;

import java.util.Objects;
import java.util.Optional;

/** Ручно повторно обрабатывает конкретное failed-событие из durable inbox. */
public final class FailedEventReplayService {

  private final EventBufferRepository repository;
  private final EventCorrelator eventCorrelator;

  public FailedEventReplayService(
      EventBufferRepository repository, EventCorrelator eventCorrelator) {
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.eventCorrelator =
        Objects.requireNonNull(eventCorrelator, "eventCorrelator must not be null");
  }

  /**
   * Захватывает failed-событие по идентификатору и запускает повторную обработку.
   *
   * @return результат replay или `Optional.empty()`, если событие не найдено либо сейчас не
   *     находится в статусе `FAILED`
   */
  public Optional<EventCorrelationResult> replayFailed(String flowName, String eventId) {
    EventPointer pointer = new EventPointer(flowName, eventId);
    return repository.claimFailedForRetry(pointer).map(eventCorrelator::replay);
  }
}
