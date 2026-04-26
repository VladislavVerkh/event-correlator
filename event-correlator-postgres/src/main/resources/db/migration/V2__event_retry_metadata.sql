alter table ec_event_inbox
    add column attempts integer not null default 0,
    add column next_retry_at timestamptz;

create index ec_event_inbox_failed_retry_idx
    on ec_event_inbox (next_retry_at, failed_at, received_at)
    where status = 'FAILED' and next_retry_at is not null;

