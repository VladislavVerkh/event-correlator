package dev.verkhovskiy.eventcorrelator;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Агрегированное состояние durable inbox для диагностики backlog и health checks. */
public record EventInboxStatistics(
    Map<EventStatus, Long> countByStatus,
    Instant oldestPendingReceivedAt,
    Instant oldestFailedAt,
    long retryReadyCount,
    long expiredCount) {

  public EventInboxStatistics {
    countByStatus = Map.copyOf(Objects.requireNonNullElse(countByStatus, Map.of()));
  }

  public long count(EventStatus status) {
    return countByStatus.getOrDefault(status, 0L);
  }
}
