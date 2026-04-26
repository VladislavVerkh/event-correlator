package dev.verkhovskiy.eventcorrelator;

import java.time.Instant;

/** Представление события из durable inbox для чтения без изменения его состояния. */
public record EventInboxRecord(
    String flowName,
    String eventId,
    String eventType,
    String correlationKey,
    EventStatus status,
    String pendingReason,
    String failureMessage,
    int attempts,
    Instant occurredAt,
    Instant receivedAt,
    Instant processedAt,
    Instant failedAt,
    Instant expiresAt,
    Instant nextRetryAt,
    Instant createdAt,
    Instant updatedAt) {}
