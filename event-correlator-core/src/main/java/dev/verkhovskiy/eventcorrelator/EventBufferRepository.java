package dev.verkhovskiy.eventcorrelator;

import java.time.Instant;
import java.util.List;

/** Durable repository для входящих и ожидающих событий. */
public interface EventBufferRepository {

  /**
   * Сохраняет событие, если событие с таким `flowName + eventId` еще не было принято.
   *
   * @param event входящее событие
   * @return `true`, если событие сохранено впервые
   */
  boolean insertIfAbsent(BufferedEvent event);

  /**
   * Переводит событие в ожидание недостающих зависимостей.
   *
   * @param pointer указатель на событие
   * @param reason причина ожидания
   * @param expiresAt момент, после которого событие можно считать просроченным
   */
  void markPending(EventPointer pointer, String reason, Instant expiresAt);

  /** Помечает событие как успешно обработанное. */
  void markProcessed(EventPointer pointer);

  /** Помечает событие как неуспешно обработанное. */
  void markFailed(EventPointer pointer, String failureMessage);

  /** Помечает событие как просроченное. */
  void markExpired(EventPointer pointer);

  /** Проверяет, был ли уже успешно обработан требуемый тип события по тому же business key. */
  boolean existsProcessed(String flowName, String eventType, String correlationKey);

  /** Возвращает pending-события по одному business key. */
  List<BufferedEvent> findPending(String flowName, String correlationKey);

  /**
   * Переводит просроченные pending-события в `EXPIRED`.
   *
   * @param now текущий момент
   * @param limit максимальный размер batch
   * @return количество просроченных событий
   */
  int expirePendingBefore(Instant now, int limit);
}
