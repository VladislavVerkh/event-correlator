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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class FailedEventRetryServiceTest {

  @Test
  void retriesFailedEventsWhenRetryTimeComes() {
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
    Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    EventCorrelator correlator =
        new DefaultEventCorrelator(
            new EventDefinitionRegistry(List.of(flow)),
            repository,
            clock,
            new DirectEventCorrelationBoundary(),
            new EventFailureRetryPolicy(2, Duration.ofSeconds(30)));
    FailedEventRetryService retryService =
        new FailedEventRetryService(repository, correlator, clock, 10);

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
    assertThat(repository.events())
        .singleElement()
        .extracting(BufferedEvent::status)
        .isEqualTo(EventStatus.FAILED);

    int retried = retryService.runOnce();

    assertThat(retried).isZero();
    assertThat(calls).hasValue(1);

    retryService =
        new FailedEventRetryService(
            repository,
            correlator,
            Clock.fixed(Instant.parse("2026-01-01T00:00:31Z"), ZoneOffset.UTC),
            10);

    retried = retryService.runOnce();

    assertThat(retried).isEqualTo(1);
    assertThat(calls).hasValue(2);
    assertThat(repository.events())
        .singleElement()
        .extracting(BufferedEvent::status)
        .isEqualTo(EventStatus.PROCESSED);
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
      List<EventPointer> pointers =
          events.values().stream()
              .filter(event -> event.status() == EventStatus.FAILED)
              .filter(event -> isRetryReady(event.pointer(), now))
              .limit(limit)
              .map(BufferedEvent::pointer)
              .toList();
      pointers.forEach(
          pointer -> {
            nextRetryAtByPointer.remove(pointer);
            update(pointer, EventStatus.PENDING);
          });
      return pointers.stream().map(events::get).toList();
    }

    List<BufferedEvent> events() {
      return List.copyOf(events.values());
    }

    private boolean isRetryReady(EventPointer pointer, Instant now) {
      Instant nextRetryAt = nextRetryAtByPointer.get(pointer);
      return nextRetryAt != null && !nextRetryAt.isAfter(now);
    }

    private void update(EventPointer pointer, EventStatus status) {
      events.computeIfPresent(pointer, (ignored, event) -> event.withStatus(status));
    }
  }
}
