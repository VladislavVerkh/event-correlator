package dev.verkhovskiy.eventcorrelator;

import java.util.function.Supplier;

/**
 * Граница без транзакции и распределенной блокировки, удобная для unit-тестов и in-memory запуска.
 */
public final class DirectEventCorrelationBoundary implements EventCorrelationBoundary {

  @Override
  public <T> T execute(String flowName, String correlationKey, Supplier<T> action) {
    return action.get();
  }
}
