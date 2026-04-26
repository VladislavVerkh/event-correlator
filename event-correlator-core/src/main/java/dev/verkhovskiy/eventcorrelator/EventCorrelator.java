package dev.verkhovskiy.eventcorrelator;

/** Принимает бизнес-события и обрабатывает их, когда выполнены declared dependencies. */
public interface EventCorrelator {

  EventCorrelationResult accept(IncomingEvent<?> event);
}
