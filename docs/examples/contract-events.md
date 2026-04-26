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
