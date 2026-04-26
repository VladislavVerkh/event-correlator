package dev.verkhovskiy.eventcorrelator;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Описание одного типа события внутри event flow. */
public final class EventTypeDefinition<T> {

  private final String eventType;
  private final Class<T> payloadClass;
  private final Function<T, String> correlationKeyExtractor;
  private final EventHandler<T> handler;
  private final Set<String> requiredEventTypes;

  EventTypeDefinition(
      String eventType,
      Class<T> payloadClass,
      Function<T, String> correlationKeyExtractor,
      EventHandler<T> handler,
      Set<String> requiredEventTypes) {
    this.eventType = requireText(eventType, "eventType");
    this.payloadClass = Objects.requireNonNull(payloadClass, "payloadClass must not be null");
    this.correlationKeyExtractor =
        Objects.requireNonNull(correlationKeyExtractor, "correlationKeyExtractor must not be null");
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
    this.requiredEventTypes = Set.copyOf(requiredEventTypes);
  }

  public String eventType() {
    return eventType;
  }

  public Class<T> payloadClass() {
    return payloadClass;
  }

  public Set<String> requiredEventTypes() {
    return requiredEventTypes;
  }

  public String correlationKey(T payload) {
    return correlationKeyExtractor.apply(payload);
  }

  public void handle(T payload) {
    handler.handle(payload);
  }

  void handleUntyped(Object payload) {
    handle(payloadClass.cast(payload));
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  public static final class EventBuilder<T> {
    private final EventFlowDefinition.Builder parent;
    private final String eventType;
    private final Class<T> payloadClass;
    private final Set<String> requiredEventTypes = new LinkedHashSet<>();
    private Function<T, String> correlationKeyExtractor;
    private EventHandler<T> handler;

    EventBuilder(EventFlowDefinition.Builder parent, String eventType, Class<T> payloadClass) {
      this.parent = Objects.requireNonNull(parent, "parent must not be null");
      this.eventType = requireText(eventType, "eventType");
      this.payloadClass = Objects.requireNonNull(payloadClass, "payloadClass must not be null");
    }

    public EventBuilder<T> root() {
      requiredEventTypes.clear();
      return this;
    }

    public EventBuilder<T> requires(String requiredEventType) {
      requiredEventTypes.add(requireText(requiredEventType, "requiredEventType"));
      return this;
    }

    public EventBuilder<T> requiresRoot(String rootEventType) {
      return requires(rootEventType);
    }

    public EventBuilder<T> correlationKey(Function<T, String> correlationKeyExtractor) {
      this.correlationKeyExtractor =
          Objects.requireNonNull(
              correlationKeyExtractor, "correlationKeyExtractor must not be null");
      return this;
    }

    public EventBuilder<T> handler(EventHandler<T> handler) {
      this.handler = Objects.requireNonNull(handler, "handler must not be null");
      return this;
    }

    public EventFlowDefinition.Builder add() {
      if (correlationKeyExtractor == null) {
        throw new IllegalStateException("correlationKeyExtractor must be configured");
      }
      if (handler == null) {
        throw new IllegalStateException("handler must be configured");
      }
      return parent.add(
          new EventTypeDefinition<>(
              eventType, payloadClass, correlationKeyExtractor, handler, requiredEventTypes));
    }
  }
}
