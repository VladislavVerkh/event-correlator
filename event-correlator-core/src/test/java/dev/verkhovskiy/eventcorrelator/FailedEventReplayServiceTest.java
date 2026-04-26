package dev.verkhovskiy.eventcorrelator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class FailedEventReplayServiceTest {

  @Test
  void replaysFailedEventByPointer() {
    AtomicInteger calls = new AtomicInteger();
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> {
                  if (calls.incrementAndGet() == 1) {
                    throw new IllegalStateException("temporary failure");
                  }
                })
            .build();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelator correlator =
        new DefaultEventCorrelator(
            new EventDefinitionRegistry(List.of(flow)),
            repository,
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
            new DirectEventCorrelationBoundary(),
            new EventFailureRetryPolicy(1, Duration.ofSeconds(30)));
    FailedEventReplayService replayService = new FailedEventReplayService(repository, correlator);

    EventCorrelationResult firstAttempt =
        correlator.accept(
            IncomingEvent.<ContractCreated>builder()
                .flowName("contract-events")
                .eventType("contract.created")
                .eventId("event-1")
                .correlationKey("contract-1")
                .payload(new ContractCreated("contract-1"))
                .receivedAt(Instant.parse("2026-01-01T00:00:00Z"))
                .build());

    assertThat(firstAttempt.status()).isEqualTo(EventCorrelationStatus.FAILED);

    Optional<EventCorrelationResult> replayResult =
        replayService.replayFailed("contract-events", "event-1");

    assertThat(replayResult).isPresent();
    assertThat(replayResult.orElseThrow().status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(calls).hasValue(2);
    assertThat(repository.status("event-1")).isEqualTo(EventStatus.PROCESSED);
  }

  @Test
  void returnsEmptyWhenEventIsNotFailed() {
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelator correlator =
        new EventCorrelator() {
          @Override
          public EventCorrelationResult accept(IncomingEvent<?> event) {
            throw new UnsupportedOperationException();
          }

          @Override
          public EventCorrelationResult accept(RawIncomingEvent<?> event) {
            throw new UnsupportedOperationException();
          }

          @Override
          public EventCorrelationResult replay(BufferedEvent event) {
            throw new UnsupportedOperationException();
          }
        };
    FailedEventReplayService replayService = new FailedEventReplayService(repository, correlator);

    Optional<EventCorrelationResult> replayResult =
        replayService.replayFailed("contract-events", "event-1");

    assertThat(replayResult).isEmpty();
  }

  @Test
  void notifiesObserverAboutManualReplayClaim() {
    AtomicInteger claimed = new AtomicInteger();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelatorObserver observer =
        new EventCorrelatorObserver() {
          @Override
          public void failedManualReplayClaimed(EventPointer pointer, boolean claimResult) {
            if (claimResult) {
              claimed.incrementAndGet();
            }
          }
        };
    repository.insertIfAbsent(
        new BufferedEvent(
            "contract-events",
            "contract.created",
            "event-1",
            "contract-1",
            new ContractCreated("contract-1"),
            Map.of(),
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            EventStatus.RECEIVED));
    repository.markFailed(
        new EventPointer("contract-events", "event-1"),
        "temporary failure",
        Instant.parse("2026-01-01T00:00:30Z"),
        1);
    EventCorrelator correlator =
        new EventCorrelator() {
          @Override
          public EventCorrelationResult accept(IncomingEvent<?> event) {
            throw new UnsupportedOperationException();
          }

          @Override
          public EventCorrelationResult accept(RawIncomingEvent<?> event) {
            throw new UnsupportedOperationException();
          }

          @Override
          public EventCorrelationResult replay(BufferedEvent event) {
            return EventCorrelationResult.processed(event.pointer());
          }
        };
    FailedEventReplayService replayService =
        new FailedEventReplayService(repository, correlator, observer);

    replayService.replayFailed("contract-events", "event-1");

    assertThat(claimed).hasValue(1);
  }

  private record ContractCreated(String contractId) {}

  private static final class TestEventBufferRepository implements EventBufferRepository {
    private final Map<EventPointer, BufferedEvent> events = new LinkedHashMap<>();
    private final Map<EventPointer, Instant> nextRetryAtByPointer = new LinkedHashMap<>();
    private final Map<EventPointer, Integer> attemptsByPointer = new LinkedHashMap<>();

    @Override
    public boolean insertIfAbsent(BufferedEvent event) {
      if (events.containsKey(event.pointer())) {
        return false;
      }
      events.put(event.pointer(), event);
      return true;
    }

    @Override
    public void markPending(EventPointer pointer, String reason, Instant expiresAt) {
      update(pointer, EventStatus.PENDING);
    }

    @Override
    public void markProcessed(EventPointer pointer) {
      nextRetryAtByPointer.remove(pointer);
      update(pointer, EventStatus.PROCESSED);
    }

    @Override
    public void markFailed(
        EventPointer pointer, String failureMessage, Instant nextRetryAt, int maxAttempts) {
      int nextAttempts = attemptsByPointer.getOrDefault(pointer, 0) + 1;
      attemptsByPointer.put(pointer, nextAttempts);
      if (nextAttempts < maxAttempts) {
        nextRetryAtByPointer.put(pointer, nextRetryAt);
      }
      update(pointer, EventStatus.FAILED);
    }

    @Override
    public void markExpired(EventPointer pointer) {
      update(pointer, EventStatus.EXPIRED);
    }

    @Override
    public boolean existsProcessed(String flowName, String eventType, String correlationKey) {
      return events.values().stream()
          .anyMatch(
              event ->
                  event.flowName().equals(flowName)
                      && event.eventType().equals(eventType)
                      && event.correlationKey().equals(correlationKey)
                      && event.status() == EventStatus.PROCESSED);
    }

    @Override
    public List<BufferedEvent> findPending(String flowName, String correlationKey) {
      return events.values().stream()
          .filter(
              event ->
                  event.flowName().equals(flowName)
                      && event.correlationKey().equals(correlationKey)
                      && event.status() == EventStatus.PENDING)
          .sorted(Comparator.comparing(BufferedEvent::receivedAt))
          .toList();
    }

    @Override
    public int expirePendingBefore(Instant now, int limit) {
      return 0;
    }

    @Override
    public List<BufferedEvent> claimFailedReadyForRetry(Instant now, int limit) {
      return List.of();
    }

    @Override
    public Optional<BufferedEvent> claimFailedForRetry(EventPointer pointer) {
      BufferedEvent event = events.get(pointer);
      if (event == null || event.status() != EventStatus.FAILED) {
        return Optional.empty();
      }
      nextRetryAtByPointer.remove(pointer);
      update(pointer, EventStatus.PENDING);
      return Optional.of(events.get(pointer));
    }

    EventStatus status(String eventId) {
      return events.values().stream()
          .filter(event -> event.eventId().equals(eventId))
          .findFirst()
          .orElseThrow()
          .status();
    }

    private void update(EventPointer pointer, EventStatus status) {
      events.computeIfPresent(pointer, (ignored, event) -> event.withStatus(status));
    }
  }
}
