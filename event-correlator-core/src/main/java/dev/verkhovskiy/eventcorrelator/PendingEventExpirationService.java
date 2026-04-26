package dev.verkhovskiy.eventcorrelator;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Переводит pending-события с истекшим `expiresAt` в `EXPIRED`. */
public final class PendingEventExpirationService {

  private final EventBufferRepository repository;
  private final Clock clock;
  private final int batchSize;
  private final EventCorrelatorObserver observer;

  public PendingEventExpirationService(
      EventBufferRepository repository, Clock clock, int batchSize) {
    this(repository, clock, batchSize, EventCorrelatorObserver.NOOP);
  }

  public PendingEventExpirationService(
      EventBufferRepository repository,
      Clock clock,
      int batchSize,
      EventCorrelatorObserver observer) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.batchSize = batchSize;
    this.observer =
        CompositeEventCorrelatorObserver.of(
            List.of(Objects.requireNonNull(observer, "observer must not be null")));
  }

  public int runOnce() {
    Instant now = Instant.now(clock);
    int expiredCount = repository.expirePendingBefore(now, batchSize);
    observer.pendingExpired(expiredCount);
    return expiredCount;
  }
}
