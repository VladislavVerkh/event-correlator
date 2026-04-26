# event-correlator

`event-correlator` - независимая библиотека для микросервисов, которые получают связанные
бизнес-события из разных источников и должны обрабатывать их только после появления необходимых
зависимостей.

Типичный кейс: в сервис пришел график платежей или дополнительные атрибуты договора, но событие о
самом договоре еще не пришло. Библиотека durable-сохраняет дочерние события, помечает их как
`PENDING`, а после прихода root-события выпускает накопленные события в бизнес-обработчики.

## Что уже есть

- core contracts и DSL для описания связанных событий;
- структурная валидация event flow definitions;
- `DefaultEventCorrelator`, который дедуплицирует события, проверяет зависимости и обрабатывает
  pending-события после появления root/dependency;
- transactional/locking boundary для последовательной обработки одного `flowName + correlationKey`;
- `EventBufferRepository` как storage contract;
- PostgreSQL repository и Flyway migration для `ec_event_inbox`;
- Spring Boot autoconfiguration;
- `PendingEventExpirationService` для перевода просроченных orphan events в `EXPIRED`;
- testkit с in-memory repository.

## Модули

- `event-correlator-core` - contracts, DSL и базовый runtime.
- `event-correlator-postgres` - PostgreSQL durable inbox.
- `event-correlator-spring-boot-starter` - Spring Boot autoconfiguration.
- `event-correlator-testkit` - test helpers.

## Пример

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
      .event("contract.ready.for.scoring", ContractReadyForScoring.class)
      .requires("contract.created")
      .requires("payment.schedule.changed")
      .correlationKey(ContractReadyForScoring::contractId)
      .handler(handlers::sendToScoring)
      .add()
      .orphanRetention(Duration.ofDays(7))
      .build();
}
```

`.requires(...)` можно вызвать несколько раз. В этом случае событие останется `PENDING`, пока все
указанные зависимости по тому же `correlationKey` не будут успешно обработаны и не получат статус
`PROCESSED`.

Transport adapter, например Kafka listener, нормализует каждое сообщение и вызывает
`eventCorrelator.accept(...)`.

Дочернее событие:

```java
eventCorrelator.accept(
    IncomingEvent.<PaymentScheduleChanged>builder()
        .flowName("contract-events")
        .eventType("payment.schedule.changed")
        .eventId(kafkaEventId)
        .correlationKey(payload.contractId())
        .payload(payload)
        .receivedAt(Instant.now())
        .build());
```

Корневое событие тоже должно входить через correlator:

```java
eventCorrelator.accept(
    IncomingEvent.<ContractCreated>builder()
        .flowName("contract-events")
        .eventType("contract.created")
        .eventId(kafkaEventId)
        .correlationKey(payload.contractId())
        .payload(payload)
        .receivedAt(Instant.now())
        .build());
```

Listener не должен напрямую вызывать `handlers.applyContract(...)`. Root handler вызывается внутри
correlator-а, после сохранения root-события в durable inbox. После успешной обработки root-события
correlator повторно проверяет pending-события по тому же `correlationKey` и выпускает те, у которых
теперь выполнены зависимости.

Для expiration просроченных pending-событий приложение задает расписание:

```java
@Scheduled(fixedDelayString = "PT1M")
void expirePendingEvents() {
  pendingEventExpirationService.runOnce();
}
```

## Сборка

```bash
./gradlew check
```

## Документация

- [Архитектура](docs/architecture.md)
- [Пример contract events](docs/examples/contract-events.md)
