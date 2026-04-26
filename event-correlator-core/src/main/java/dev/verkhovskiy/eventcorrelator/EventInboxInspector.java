package dev.verkhovskiy.eventcorrelator;

import java.util.List;
import java.util.Optional;

/** API только для чтения, который используется для диагностики durable inbox событий. */
public interface EventInboxInspector {

  /** Находит одно событие по имени flow и идентификатору события. */
  Optional<EventInboxRecord> findEvent(String flowName, String eventId);

  /** Ищет события по диагностическому фильтру. */
  List<EventInboxRecord> findEvents(EventInboxQuery query);
}
