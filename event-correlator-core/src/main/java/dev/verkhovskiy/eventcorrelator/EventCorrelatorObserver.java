package dev.verkhovskiy.eventcorrelator;

import java.time.Duration;
import java.util.List;

/** Получает события наблюдаемости во время работы event-correlator. */
public interface EventCorrelatorObserver {

  EventCorrelatorObserver NOOP = new EventCorrelatorObserver() {};

  /** Событие впервые сохранено в durable inbox. */
  default void eventAccepted(BufferedEvent event) {}

  /** Событие уже было принято ранее. */
  default void eventDuplicate(BufferedEvent event) {}

  /** Событие перешло в ожидание недостающих зависимостей. */
  default void eventPending(BufferedEvent event, List<String> missingDependencies) {}

  /** Бизнес-обработчик события успешно завершился. */
  default void eventProcessed(BufferedEvent event, Duration handlingDuration) {}

  /** Бизнес-обработчик события завершился ошибкой. */
  default void eventFailed(BufferedEvent event, String failureMessage, Duration handlingDuration) {}

  /** Завершился batch expiration pending-событий. */
  default void pendingExpired(int expiredCount) {}

  /** Для автоматического retry был захвачен batch failed-событий. */
  default void failedRetryClaimed(int claimedCount) {}

  /** Завершилась попытка ручного replay failed-события. */
  default void failedManualReplayClaimed(EventPointer pointer, boolean claimed) {}
}
