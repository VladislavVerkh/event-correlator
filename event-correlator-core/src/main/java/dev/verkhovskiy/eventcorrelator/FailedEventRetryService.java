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

  public FailedEventRetryService(
      EventBufferRepository repository,
      EventCorrelator eventCorrelator,
      Clock clock,
      int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.eventCorrelator =
        Objects.requireNonNull(eventCorrelator, "eventCorrelator must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.batchSize = batchSize;
  }

  public int runOnce() {
    Instant now = Instant.now(clock);
    List<BufferedEvent> events = repository.claimFailedReadyForRetry(now, batchSize);
    events.forEach(eventCorrelator::replay);
    return events.size();
  }
}
