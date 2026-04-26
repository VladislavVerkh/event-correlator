package dev.verkhovskiy.eventcorrelator;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Повторно обрабатывает failed-события, у которых наступило время retry. */
public final class FailedEventRetryService {

  private final EventBufferRepository repository;
  private final EventCorrelator eventCorrelator;
  private final Clock clock;
  private final int batchSize;
  private final EventCorrelatorObserver observer;

  public FailedEventRetryService(
      EventBufferRepository repository,
      EventCorrelator eventCorrelator,
      Clock clock,
      int batchSize) {
    this(repository, eventCorrelator, clock, batchSize, EventCorrelatorObserver.NOOP);
  }

  public FailedEventRetryService(
      EventBufferRepository repository,
      EventCorrelator eventCorrelator,
      Clock clock,
      int batchSize,
      EventCorrelatorObserver observer) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.eventCorrelator =
        Objects.requireNonNull(eventCorrelator, "eventCorrelator must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.batchSize = batchSize;
    this.observer =
        CompositeEventCorrelatorObserver.of(
            List.of(Objects.requireNonNull(observer, "observer must not be null")));
  }

  public int runOnce() {
    Instant now = Instant.now(clock);
    List<BufferedEvent> events = repository.claimFailedReadyForRetry(now, batchSize);
    observer.failedRetryClaimed(events.size());
    events.forEach(eventCorrelator::replay);
    return events.size();
  }
}
