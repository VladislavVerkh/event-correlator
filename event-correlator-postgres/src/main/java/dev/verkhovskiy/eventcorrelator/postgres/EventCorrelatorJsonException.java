package dev.verkhovskiy.eventcorrelator.postgres;

/** Ошибка сериализации или десериализации payload события. */
public class EventCorrelatorJsonException extends RuntimeException {

  public EventCorrelatorJsonException(String message, Throwable cause) {
    super(message, cause);
  }
}
