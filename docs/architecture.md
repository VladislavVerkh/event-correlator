# Архитектура

`event-correlator` решает задачу durable-корреляции входящих событий. Он не привязан к Kafka,
RabbitMQ или другому транспорту: transport adapter обычно создает `RawIncomingEvent` и вызывает
`EventCorrelator.accept(...)`. Correlator сам вычисляет `correlationKey` через extractor из
`EventFlowDefinition`. Если adapter уже вычислил business key, он может передать готовый
`IncomingEvent`.

Это правило применяется ко всем событиям одного flow, включая root-событие. Root-событие нельзя
обрабатывать напрямую в listener-е, потому что correlator должен сначала сохранить его в durable
inbox, пометить как `PROCESSED`, а затем повторно проверить накопленные `PENDING` события по тому же
`correlationKey`.

## Компоненты

```text
Kafka/Rabbit/HTTP listener
  |
  | builds RawIncomingEvent
  v
EventCorrelator
  |
  | stores event, checks dependencies
  v
EventBufferRepository
  |
  | durable state
  v
PostgreSQL ec_event_inbox

When dependencies are satisfied:

EventCorrelator -> EventHandler<T> -> application business logic
```

## Порядок вызова handlers

Когда приходит событие, correlator сначала пытается обработать именно его. Если зависимости
выполнены, событие получает статус `PROCESSED`, а затем correlator ищет накопленные `PENDING`
события с тем же `flowName + correlationKey`.

Pending-события проверяются в порядке приема:

```text
received_at, created_at
```

Для каждого pending-события зависимости проверяются заново. Если все required event types уже имеют
статус `PROCESSED`, вызывается handler этого pending-события. Если за проход был обработан хотя бы
один pending event, correlator запускает следующий проход. Повторы продолжаются, пока очередной
проход не перестанет давать прогресс.

Если между двумя событиями нет явной зависимости, порядок их handlers определяется временем приема.
Если бизнесу нужен строгий порядок, его надо выразить через `.requires(...)`.

## Транзакция и lock

В Spring/PostgreSQL режиме весь прием события выполняется внутри одной boundary:

```text
open transaction
  -> pg_advisory_xact_lock(flowName, correlationKey)
  -> insert incoming event
  -> check dependencies
  -> call handler when ready
  -> update event status
  -> drain pending events with the same correlationKey
commit transaction
```

Lock берется по паре `flowName + correlationKey`, поэтому два события одного бизнес-объекта не
должны одновременно проверять зависимости и выпускать pending-события. События разных
`correlationKey` могут обрабатываться параллельно.

PostgreSQL implementation использует transaction-scoped advisory lock:

```sql
pg_advisory_xact_lock(hashtext(flow_name), hashtext(correlation_key))
```

Если handler завершился исключением, текущая реализация переводит событие в `FAILED` и возвращает
`EventCorrelationStatus.FAILED`. Поэтому handler должен быть идемпотентным и не оставлять частично
примененные бизнес-изменения перед выбросом исключения. Более строгая retry/rollback модель будет
отдельным слоем развития.

## Expiration pending-событий

Когда событие не может быть обработано из-за отсутствующих dependencies, correlator переводит его в
`PENDING` и сохраняет `expires_at`. Значение считается от `orphanRetention` текущего
`EventFlowDefinition`.

`PendingEventExpirationService` переводит просроченные pending-события в `EXPIRED` batch-ами:

```java
@Scheduled(fixedDelayString = "PT1M")
void expirePendingEvents() {
  pendingEventExpirationService.runOnce();
}
```

Spring Boot starter создает `PendingEventExpirationService`, но не задает расписание сам.
Приложение выбирает частоту запуска исходя из нагрузки и допустимой задержки диагностики orphan
events.

Размер batch настраивается свойством:

```properties
event.correlator.expiration-batch-size=100
```

PostgreSQL implementation использует `for update skip locked`, поэтому несколько экземпляров
приложения могут запускать expiration параллельно без обработки одного и того же batch.

## Retry failed-событий

Если handler выбрасывает `RuntimeException`, correlator переводит событие в `FAILED`, увеличивает
`attempts` и, если лимит попыток еще не исчерпан, сохраняет `next_retry_at`.

Retry не запускается сам по себе. Приложение задает расписание:

```java
@Scheduled(fixedDelayString = "PT30S")
void retryFailedEvents() {
  failedEventRetryService.runOnce();
}
```

`FailedEventRetryService` batch-ом захватывает события, у которых `next_retry_at <= now`, и вызывает
`EventCorrelator.replay(...)`. Replay не создает новое inbox-событие и не проходит через duplicate
check: он повторно обрабатывает уже сохраненный payload.

Основные свойства:

```properties
event.correlator.failure-max-attempts=1
event.correlator.failure-retry-delay=PT1M
event.correlator.failed-retry-batch-size=100
```

`failure-max-attempts=1` означает: первая ошибка сразу оставляет событие в `FAILED` без retry. Чтобы
включить повторы, задайте значение больше `1`. Например, `3` означает первичную попытку и две
повторные попытки.

## Валидация definitions

`EventFlowDefinition` проверяется при `build()` и при регистрации в `EventDefinitionRegistry`.
Валидация ловит ошибки конфигурации до обработки событий:

- flow должен содержать хотя бы один event type;
- flow должен содержать хотя бы одно root-событие без dependencies;
- каждый `.requires(...)` должен ссылаться на существующий event type того же flow;
- граф dependencies не должен содержать циклы;
- `EventDefinitionRegistry` не принимает два flow с одинаковым `flowName`.

Если правило нарушено, выбрасывается `EventDefinitionValidationException`.

## Границы ответственности

Библиотека отвечает за:

- durable прием входящих событий;
- дедупликацию по `flowName + eventId`;
- хранение событий, которые пришли раньше зависимостей;
- проверку required event types по `correlationKey`;
- повторную попытку pending-событий после успешной обработки зависимости;
- serializing обработки одного `flowName + correlationKey` через transactional boundary;
- retention marker для orphan events.

Приложение отвечает за:

- чтение сообщений из транспорта;
- выбор `eventId` и `eventType`;
- вызов `EventCorrelator.accept(...)` для root и дочерних событий;
- бизнес-обработчики;
- миграции и транзакционные границы;
- политику повторной обработки `FAILED` и `EXPIRED` событий.

## Durable inbox

Основная таблица:

```text
ec_event_inbox
```

Ключ дедупликации:

```text
flow_name + event_id
```

Ключ бизнес-корреляции:

```text
flow_name + correlation_key
```

`payload_json` и `headers_json` хранятся как `jsonb`, но business payload процесса или агрегата не
становится источником истины для приложения. Библиотека хранит только входящее событие и его
статус обработки.
