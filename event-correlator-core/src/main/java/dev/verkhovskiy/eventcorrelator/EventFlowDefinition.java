package dev.verkhovskiy.eventcorrelator;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Описание набора связанных событий одного бизнес-агрегата. */
public final class EventFlowDefinition {

  private final String flowName;
  private final Map<String, EventTypeDefinition<?>> eventsByType;
  private final Duration orphanRetention;

  private EventFlowDefinition(
      String flowName, Map<String, EventTypeDefinition<?>> eventsByType, Duration orphanRetention) {
    this.flowName = requireText(flowName, "flowName");
    this.eventsByType = Map.copyOf(eventsByType);
    this.orphanRetention =
        Objects.requireNonNull(orphanRetention, "orphanRetention must not be null");
  }

  public static Builder builder(String flowName) {
    return new Builder(flowName);
  }

  public String flowName() {
    return flowName;
  }

  public Duration orphanRetention() {
    return orphanRetention;
  }

  public EventTypeDefinition<?> requireEvent(String eventType) {
    EventTypeDefinition<?> definition = eventsByType.get(eventType);
    if (definition == null) {
      throw new IllegalArgumentException(
          "Unknown event type `" + eventType + "` for flow `" + flowName + "`");
    }
    return definition;
  }

  public Map<String, EventTypeDefinition<?>> eventsByType() {
    return eventsByType;
  }

  public static final class Builder {

    private final String flowName;
    private final Map<String, EventTypeDefinition<?>> eventsByType = new LinkedHashMap<>();
    private Duration orphanRetention = Duration.ofDays(7);

    private Builder(String flowName) {
      this.flowName = requireText(flowName, "flowName");
    }

    public <T> Builder rootEvent(
        String eventType,
        Class<T> payloadClass,
        Function<T, String> correlationKeyExtractor,
        EventHandler<T> handler) {
      return event(eventType, payloadClass)
          .correlationKey(correlationKeyExtractor)
          .handler(handler)
          .root()
          .add();
    }

    public <T> EventTypeDefinition.EventBuilder<T> event(String eventType, Class<T> payloadClass) {
      return new EventTypeDefinition.EventBuilder<>(this, eventType, payloadClass);
    }

    public Builder orphanRetention(Duration orphanRetention) {
      if (orphanRetention.isNegative() || orphanRetention.isZero()) {
        throw new IllegalArgumentException("orphanRetention must be positive");
      }
      this.orphanRetention = orphanRetention;
      return this;
    }

    public EventFlowDefinition build() {
      if (eventsByType.isEmpty()) {
        throw new EventDefinitionValidationException(
            "Event flow `" + flowName + "` must contain at least one event type");
      }
      EventFlowDefinition definition =
          new EventFlowDefinition(flowName, eventsByType, orphanRetention);
      EventFlowDefinitionValidator.validate(definition);
      return definition;
    }

    Builder add(EventTypeDefinition<?> definition) {
      if (eventsByType.putIfAbsent(definition.eventType(), definition) != null) {
        throw new IllegalArgumentException("Duplicate event type: " + definition.eventType());
      }
      return this;
    }
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
