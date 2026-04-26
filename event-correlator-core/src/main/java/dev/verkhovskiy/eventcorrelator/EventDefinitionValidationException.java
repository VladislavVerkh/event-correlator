package dev.verkhovskiy.eventcorrelator;

/** Ошибка структурной валидации event flow definition. */
public class EventDefinitionValidationException extends RuntimeException {

  public EventDefinitionValidationException(String message) {
    super(message);
  }
}
