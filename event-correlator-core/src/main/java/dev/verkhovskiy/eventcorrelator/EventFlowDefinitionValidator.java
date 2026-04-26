package dev.verkhovskiy.eventcorrelator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Проверяет, что граф зависимостей event flow можно выполнить. */
public final class EventFlowDefinitionValidator {

  private EventFlowDefinitionValidator() {}

  public static void validate(EventFlowDefinition definition) {
    Map<String, EventTypeDefinition<?>> eventsByType = definition.eventsByType();
    if (eventsByType.isEmpty()) {
      throw new EventDefinitionValidationException(
          "Event flow `" + definition.flowName() + "` must contain at least one event type");
    }
    validateHasRoot(definition);
    validateRequiredEventTypesExist(definition);
    validateNoCycles(definition);
  }

  private static void validateHasRoot(EventFlowDefinition definition) {
    boolean hasRoot =
        definition.eventsByType().values().stream()
            .anyMatch(event -> event.requiredEventTypes().isEmpty());
    if (!hasRoot) {
      throw new EventDefinitionValidationException(
          "Event flow `" + definition.flowName() + "` must contain at least one root event");
    }
  }

  private static void validateRequiredEventTypesExist(EventFlowDefinition definition) {
    Set<String> knownEventTypes = definition.eventsByType().keySet();
    for (EventTypeDefinition<?> event : definition.eventsByType().values()) {
      for (String requiredEventType : event.requiredEventTypes()) {
        if (!knownEventTypes.contains(requiredEventType)) {
          throw new EventDefinitionValidationException(
              "Event `"
                  + event.eventType()
                  + "` in flow `"
                  + definition.flowName()
                  + "` requires unknown event `"
                  + requiredEventType
                  + "`");
        }
      }
    }
  }

  private static void validateNoCycles(EventFlowDefinition definition) {
    Set<String> visiting = new HashSet<>();
    Set<String> visited = new HashSet<>();
    for (String eventType : definition.eventsByType().keySet()) {
      visit(definition, eventType, visiting, visited);
    }
  }

  private static void visit(
      EventFlowDefinition definition, String eventType, Set<String> visiting, Set<String> visited) {
    if (visited.contains(eventType)) {
      return;
    }
    if (!visiting.add(eventType)) {
      throw new EventDefinitionValidationException(
          "Event flow `"
              + definition.flowName()
              + "` contains cyclic dependency involving event `"
              + eventType
              + "`");
    }

    EventTypeDefinition<?> event = definition.requireEvent(eventType);
    for (String requiredEventType : event.requiredEventTypes()) {
      visit(definition, requiredEventType, visiting, visited);
    }

    visiting.remove(eventType);
    visited.add(eventType);
  }
}
