package dev.verkhovskiy.eventcorrelator;

/** Указатель на событие во внутреннем inbox. */
public record EventPointer(String flowName, String eventId) {

  public EventPointer {
    if (flowName == null || flowName.isBlank()) {
      throw new IllegalArgumentException("flowName must not be blank");
    }
    if (eventId == null || eventId.isBlank()) {
      throw new IllegalArgumentException("eventId must not be blank");
    }
  }
}
