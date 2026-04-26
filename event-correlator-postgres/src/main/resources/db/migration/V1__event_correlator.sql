create table ec_event_inbox
(
    flow_name       text        not null,
    event_id        text        not null,
    event_type      text        not null,
    correlation_key text        not null,
    payload_json    jsonb       not null,
    headers_json    jsonb       not null default '{}'::jsonb,
    occurred_at     timestamptz,
    received_at     timestamptz not null,
    status          text        not null,
    pending_reason  text,
    processed_at    timestamptz,
    failed_at       timestamptz,
    failure_message text,
    expires_at      timestamptz,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    primary key (flow_name, event_id)
);

create index ec_event_inbox_pending_idx
    on ec_event_inbox (flow_name, correlation_key, received_at)
    where status = 'PENDING';

create index ec_event_inbox_processed_dependency_idx
    on ec_event_inbox (flow_name, event_type, correlation_key)
    where status = 'PROCESSED';

create index ec_event_inbox_expired_pending_idx
    on ec_event_inbox (expires_at)
    where status = 'PENDING';
