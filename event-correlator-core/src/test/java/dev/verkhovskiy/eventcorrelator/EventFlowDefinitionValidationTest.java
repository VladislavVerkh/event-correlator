package dev.verkhovskiy.eventcorrelator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class EventFlowDefinitionValidationTest {

  @Test
  void rejectsUnknownRequiredEventType() {
    assertThatThrownBy(
            () ->
                EventFlowDefinition.builder("contract-events")
                    .rootEvent(
                        "contract.created",
                        ContractCreated.class,
                        ContractCreated::contractId,
                        payload -> {})
                    .event("payment.schedule.changed", PaymentScheduleChanged.class)
                    .requires("unknown.event")
                    .correlationKey(PaymentScheduleChanged::contractId)
                    .handler(payload -> {})
                    .add()
                    .build())
        .isInstanceOf(EventDefinitionValidationException.class)
        .hasMessageContaining("requires unknown event `unknown.event`");
  }

  @Test
  void rejectsCyclicDependencies() {
    assertThatThrownBy(
            () ->
                EventFlowDefinition.builder("contract-events")
                    .rootEvent(
                        "contract.created",
                        ContractCreated.class,
                        ContractCreated::contractId,
                        payload -> {})
                    .event("event.a", ContractCreated.class)
                    .requires("event.b")
                    .correlationKey(ContractCreated::contractId)
                    .handler(payload -> {})
                    .add()
                    .event("event.b", PaymentScheduleChanged.class)
                    .requires("event.a")
                    .correlationKey(PaymentScheduleChanged::contractId)
                    .handler(payload -> {})
                    .add()
                    .build())
        .isInstanceOf(EventDefinitionValidationException.class)
        .hasMessageContaining("contains cyclic dependency");
  }

  @Test
  void rejectsFlowWithoutRootEvent() {
    assertThatThrownBy(
            () ->
                EventFlowDefinition.builder("contract-events")
                    .event("event.a", ContractCreated.class)
                    .requires("event.a")
                    .correlationKey(ContractCreated::contractId)
                    .handler(payload -> {})
                    .add()
                    .build())
        .isInstanceOf(EventDefinitionValidationException.class)
        .hasMessageContaining("must contain at least one root event");
  }

  @Test
  void rejectsDuplicateFlowNamesInRegistry() {
    EventFlowDefinition first = flow();
    EventFlowDefinition second = flow();

    assertThatThrownBy(() -> new EventDefinitionRegistry(List.of(first, second)))
        .isInstanceOf(EventDefinitionValidationException.class)
        .hasMessageContaining("Duplicate event flow: contract-events");
  }

  private EventFlowDefinition flow() {
    return EventFlowDefinition.builder("contract-events")
        .rootEvent(
            "contract.created", ContractCreated.class, ContractCreated::contractId, payload -> {})
        .build();
  }

  private record ContractCreated(String contractId) {}

  private record PaymentScheduleChanged(String contractId) {}
}
