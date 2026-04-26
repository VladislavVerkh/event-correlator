package dev.verkhovskiy.eventcorrelator;

/** Бизнес-обработчик события, который вызывается только после выполнения зависимостей. */
@FunctionalInterface
public interface EventHandler<T> {

  void handle(T payload);
}
