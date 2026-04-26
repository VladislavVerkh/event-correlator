package dev.verkhovskiy.eventcorrelator;

import java.util.function.Supplier;

/** Выполняет прием события внутри нужной транзакционной границы и границы блокировки. */
public interface EventCorrelationBoundary {

  /**
   * Оборачивает прием одного события и drain pending-событий с тем же бизнес-ключом.
   *
   * @param flowName имя event flow
   * @param correlationKey бизнес-ключ корреляции
   * @param action действие correlator-а
   * @return результат действия
   * @param <T> тип результата
   */
  <T> T execute(String flowName, String correlationKey, Supplier<T> action);
}
