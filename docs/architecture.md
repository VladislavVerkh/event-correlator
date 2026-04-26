# Архитектура

`event-correlator` решает задачу durable-корреляции входящих событий. Он не привязан к Kafka,
RabbitMQ или другому транспорту: транспортный adapter должен создать `IncomingEvent` и вызвать
`EventCorrelator.accept(...)`.

Это правило применяется ко всем событиям одного flow, включая root-событие. Root-событие нельзя
обрабатывать напрямую в listener-е, потому что correlator должен сначала сохранить его в durable
inbox, пометить как `PROCESSED`, а затем повторно проверить накопленные `PENDING` события по тому же
`correlationKey`.

## Компоненты

```text
Kafka/Rabbit/HTTP listener
  |
  | builds IncomingEvent
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

## Границы ответственности

Библиотека отвечает за:

- durable прием входящих событий;
- дедупликацию по `flowName + eventId`;
- хранение событий, которые пришли раньше зависимостей;
- проверку required event types по `correlationKey`;
- повторную попытку pending-событий после успешной обработки зависимости;
- retention marker для orphan events.

Приложение отвечает за:

- чтение сообщений из транспорта;
- выбор `eventId`, `eventType` и `correlationKey`;
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
