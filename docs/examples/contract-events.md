# Пример contract events

Допустим сервис получает события:

- `contract.created`
- `payment.schedule.changed`
- `contract.attributes.changed`

Все они связаны через `contractId`, но порядок доставки не гарантирован.

## Definition

```java
@Bean
EventFlowDefinition contractEvents(ContractHandlers handlers) {
  return EventFlowDefinition.builder("contract-events")
      .rootEvent(
          "contract.created",
          ContractCreated.class,
          ContractCreated::contractId,
          handlers::applyContract)
      .event("payment.schedule.changed", PaymentScheduleChanged.class)
      .requiresRoot("contract.created")
      .correlationKey(PaymentScheduleChanged::contractId)
      .handler(handlers::applySchedule)
      .add()
      .event("contract.attributes.changed", ContractAttributesChanged.class)
      .requiresRoot("contract.created")
      .correlationKey(ContractAttributesChanged::contractId)
      .handler(handlers::applyAttributes)
      .add()
      .build();
}
```

## Поведение

Если `payment.schedule.changed` пришел первым:

1. событие сохраняется в `ec_event_inbox`;
2. статус становится `PENDING`;
3. handler графика платежей не вызывается;
4. когда приходит `contract.created`, root handler обрабатывается первым;
5. pending-события по тому же `contractId` проверяются повторно и вызывают свои handlers.

Если событие с тем же `eventId` пришло повторно, коррелятор вернет `DUPLICATE`.

## Где вызывать accept

`eventCorrelator.accept(...)` вызывается в listener-е каждого события flow. Это относится и к root
event, и к дочерним событиям.

Listener дочернего события:

```java
@KafkaListener(topics = "payment-schedule-events")
void onPaymentScheduleChanged(PaymentScheduleChanged payload) {
  eventCorrelator.accept(
      IncomingEvent.<PaymentScheduleChanged>builder()
          .flowName("contract-events")
          .eventType("payment.schedule.changed")
          .eventId(payload.eventId())
          .correlationKey(payload.contractId())
          .payload(payload)
          .receivedAt(Instant.now())
          .build());
}
```

Listener root-события:

```java
@KafkaListener(topics = "contract-events")
void onContractCreated(ContractCreated payload) {
  eventCorrelator.accept(
      IncomingEvent.<ContractCreated>builder()
          .flowName("contract-events")
          .eventType("contract.created")
          .eventId(payload.eventId())
          .correlationKey(payload.contractId())
          .payload(payload)
          .receivedAt(Instant.now())
          .build());
}
```

`handlers.applyContract(...)` не вызывается напрямую из listener-а. Он указан в `rootEvent(...)` и
будет вызван самим correlator-ом после сохранения события. Это важно: только так correlator узнает,
что dependency `contract.created` появилась, и сможет выпустить pending-события вроде
`payment.schedule.changed` и `contract.attributes.changed`.
