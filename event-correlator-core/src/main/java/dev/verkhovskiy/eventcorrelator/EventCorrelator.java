package dev.verkhovskiy.eventcorrelator;

/** Принимает бизнес-события и обрабатывает их, когда выполнены объявленные зависимости. */
public interface EventCorrelator {

  EventCorrelationResult accept(IncomingEvent<?> event);

  EventCorrelationResult accept(RawIncomingEvent<?> event);

  /** Повторно обрабатывает событие, уже сохраненное во внутреннем durable inbox. */
  EventCorrelationResult replay(BufferedEvent event);
}
