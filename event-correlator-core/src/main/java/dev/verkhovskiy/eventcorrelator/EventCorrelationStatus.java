package dev.verkhovskiy.eventcorrelator;

/** Итог приема события коррелятором. */
public enum EventCorrelationStatus {
  /** Событие обработано бизнес-обработчиком. */
  PROCESSED,

  /** Событие сохранено и ждет недостающие зависимости. */
  PENDING,

  /** Событие уже принималось ранее. */
  DUPLICATE,

  /** Бизнес-обработчик завершился ошибкой. */
  FAILED
}
