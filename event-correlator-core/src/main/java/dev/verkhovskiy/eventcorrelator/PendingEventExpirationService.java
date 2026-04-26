package dev.verkhovskiy.eventcorrelator;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/** Переводит pending-события с истекшим `expiresAt` в `EXPIRED`. */
public final class PendingEventExpirationService {

  private final EventBufferRepository repository;
  private final Clock clock;
  private final int batchSize;

  public PendingEventExpirationService(
      EventBufferRepository repository, Clock clock, int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.batchSize = batchSize;
  }

  public int runOnce() {
    Instant now = Instant.now(clock);
    return repository.expirePendingBefore(now, batchSize);
  }
}
