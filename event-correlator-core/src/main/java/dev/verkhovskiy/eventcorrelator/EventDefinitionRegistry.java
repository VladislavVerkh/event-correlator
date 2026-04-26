package dev.verkhovskiy.eventcorrelator;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Индексирует event flow definitions по имени flow. */
public class EventDefinitionRegistry {

  private final Map<String, EventFlowDefinition> definitionsByFlowName;

  public EventDefinitionRegistry(List<EventFlowDefinition> definitions) {
    definitionsByFlowName =
        List.copyOf(definitions).stream()
            .collect(
                Collectors.toUnmodifiableMap(EventFlowDefinition::flowName, Function.identity()));
  }

  public EventFlowDefinition requireFlow(String flowName) {
    EventFlowDefinition definition = definitionsByFlowName.get(flowName);
    if (definition == null) {
      throw new IllegalArgumentException("Unknown event flow: " + flowName);
    }
    return definition;
  }

  public List<EventFlowDefinition> definitions() {
    return List.copyOf(definitionsByFlowName.values());
  }
}
