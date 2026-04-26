package dev.verkhovskiy.eventcorrelator.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import dev.verkhovskiy.eventcorrelator.BufferedEvent;
import dev.verkhovskiy.eventcorrelator.EventStatus;

/** AssertJ helpers для проверки состояния in-memory event buffer. */
public final class EventCorrelatorAssertions {

  private EventCorrelatorAssertions() {}

  public static void assertStatus(
      InMemoryEventBufferRepository repository, String eventId, EventStatus expectedStatus) {
    assertThat(repository.events())
        .filteredOn(event -> event.eventId().equals(eventId))
        .singleElement()
        .extracting(BufferedEvent::status)
        .isEqualTo(expectedStatus);
  }
}
