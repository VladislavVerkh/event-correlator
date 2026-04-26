package dev.verkhovskiy.eventcorrelator;

import java.util.List;
import java.util.Objects;

/** Результат приема события коррелятором. */
public record EventCorrelationResult(
    EventCorrelationStatus status,
    EventPointer pointer,
    List<String> missingDependencies,
    String failureMessage) {

  public EventCorrelationResult {
    status = Objects.requireNonNull(status, "status must not be null");
    pointer = Objects.requireNonNull(pointer, "pointer must not be null");
    missingDependencies = List.copyOf(Objects.requireNonNullElse(missingDependencies, List.of()));
  }

  public static EventCorrelationResult processed(EventPointer pointer) {
    return new EventCorrelationResult(EventCorrelationStatus.PROCESSED, pointer, List.of(), null);
  }

  public static EventCorrelationResult pending(
      EventPointer pointer, List<String> missingDependencies) {
    return new EventCorrelationResult(
        EventCorrelationStatus.PENDING, pointer, missingDependencies, null);
  }

  public static EventCorrelationResult duplicate(EventPointer pointer) {
    return new EventCorrelationResult(EventCorrelationStatus.DUPLICATE, pointer, List.of(), null);
  }

  public static EventCorrelationResult failed(EventPointer pointer, String failureMessage) {
    return new EventCorrelationResult(
        EventCorrelationStatus.FAILED, pointer, List.of(), failureMessage);
  }
}
