package dev.verkhovskiy.eventcorrelator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class DefaultEventCorrelatorTest {

  @Test
  void keepsChildEventPendingUntilRootEventIsProcessed() {
    List<String> handled = new ArrayList<>();
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> handled.add("root:" + payload.contractId()))
            .event("payment.schedule.changed", PaymentScheduleChanged.class)
            .requiresRoot("contract.created")
            .correlationKey(PaymentScheduleChanged::contractId)
            .handler(payload -> handled.add("schedule:" + payload.contractId()))
            .add()
            .build();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelator correlator = correlator(flow, repository);

    EventCorrelationResult childResult =
        correlator.accept(
            event(
                "contract-events",
                "payment.schedule.changed",
                "event-2",
                "contract-1",
                new PaymentScheduleChanged("contract-1")));

    assertThat(childResult.status()).isEqualTo(EventCorrelationStatus.PENDING);
    assertThat(handled).isEmpty();
    assertThat(repository.status("event-2")).isEqualTo(EventStatus.PENDING);

    EventCorrelationResult rootResult =
        correlator.accept(
            event(
                "contract-events",
                "contract.created",
                "event-1",
                "contract-1",
                new ContractCreated("contract-1")));

    assertThat(rootResult.status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(handled).containsExactly("root:contract-1", "schedule:contract-1");
    assertThat(repository.status("event-1")).isEqualTo(EventStatus.PROCESSED);
    assertThat(repository.status("event-2")).isEqualTo(EventStatus.PROCESSED);
  }

  @Test
  void returnsDuplicateWhenEventWasAlreadyAccepted() {
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> {})
            .build();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelator correlator = correlator(flow, repository);
    IncomingEvent<ContractCreated> event =
        event(
            "contract-events",
            "contract.created",
            "event-1",
            "contract-1",
            new ContractCreated("contract-1"));

    assertThat(correlator.accept(event).status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(correlator.accept(event).status()).isEqualTo(EventCorrelationStatus.DUPLICATE);
  }

  @Test
  void executesAcceptInsideCorrelationBoundary() {
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> {})
            .build();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    RecordingBoundary boundary = new RecordingBoundary();
    EventCorrelator correlator =
        new DefaultEventCorrelator(
            new EventDefinitionRegistry(List.of(flow)),
            repository,
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
            boundary);

    correlator.accept(
        event(
            "contract-events",
            "contract.created",
            "event-1",
            "contract-1",
            new ContractCreated("contract-1")));

    assertThat(boundary.calls).isEqualTo(1);
    assertThat(boundary.flowName).isEqualTo("contract-events");
    assertThat(boundary.correlationKey).isEqualTo("contract-1");
  }

  @Test
  void derivesCorrelationKeyForRawIncomingEvent() {
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                payload -> "derived:" + payload.contractId(),
                payload -> {})
            .build();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelator correlator = correlator(flow, repository);

    EventCorrelationResult result =
        correlator.accept(
            RawIncomingEvent.<ContractCreated>builder()
                .flowName("contract-events")
                .eventType("contract.created")
                .eventId("event-1")
                .payload(new ContractCreated("contract-1"))
                .receivedAt(Instant.parse("2026-01-01T00:00:00Z"))
                .build());

    assertThat(result.status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(repository.correlationKey("event-1")).isEqualTo("derived:contract-1");
  }

  @Test
  void notifiesObserverAboutAcceptedAndProcessedEvent() {
    AtomicInteger accepted = new AtomicInteger();
    AtomicInteger processed = new AtomicInteger();
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> {})
            .build();
    TestEventBufferRepository repository = new TestEventBufferRepository();
    EventCorrelatorObserver observer =
        new EventCorrelatorObserver() {
          @Override
          public void eventAccepted(BufferedEvent event) {
            accepted.incrementAndGet();
          }

          @Override
          public void eventProcessed(BufferedEvent event, java.time.Duration handlingDuration) {
            processed.incrementAndGet();
          }
        };
    EventCorrelator correlator =
        new DefaultEventCorrelator(
            new EventDefinitionRegistry(List.of(flow)),
            repository,
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
            new DirectEventCorrelationBoundary(),
            EventFailureRetryPolicy.noRetries(),
            observer);

    correlator.accept(
        event(
            "contract-events",
            "contract.created",
            "event-1",
            "contract-1",
            new ContractCreated("contract-1")));

    assertThat(accepted).hasValue(1);
    assertThat(processed).hasValue(1);
  }

  private EventCorrelator correlator(
      EventFlowDefinition flow, TestEventBufferRepository repository) {
    return new DefaultEventCorrelator(
        new EventDefinitionRegistry(List.of(flow)),
        repository,
        Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));
  }

  private <T> IncomingEvent<T> event(
      String flowName, String eventType, String eventId, String correlationKey, T payload) {
    return IncomingEvent.<T>builder()
        .flowName(flowName)
        .eventType(eventType)
        .eventId(eventId)
        .correlationKey(correlationKey)
        .payload(payload)
        .receivedAt(Instant.parse("2026-01-01T00:00:00Z"))
        .build();
  }

  private record ContractCreated(String contractId) {}

  private record PaymentScheduleChanged(String contractId) {}

  private static final class TestEventBufferRepository implements EventBufferRepository {
    private final Map<EventPointer, BufferedEvent> events = new LinkedHashMap<>();

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
      update(pointer, EventStatus.PROCESSED);
    }

    @Override
    public void markFailed(
        EventPointer pointer, String failureMessage, Instant nextRetryAt, int maxAttempts) {
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
      return Optional.empty();
    }

    EventStatus status(String eventId) {
      return events.values().stream()
          .filter(event -> event.eventId().equals(eventId))
          .findFirst()
          .orElseThrow()
          .status();
    }

    String correlationKey(String eventId) {
      return events.values().stream()
          .filter(event -> event.eventId().equals(eventId))
          .findFirst()
          .orElseThrow()
          .correlationKey();
    }

    private void update(EventPointer pointer, EventStatus status) {
      events.computeIfPresent(pointer, (ignored, event) -> event.withStatus(status));
    }
  }

  private static final class RecordingBoundary implements EventCorrelationBoundary {
    private int calls;
    private String flowName;
    private String correlationKey;

    @Override
    public <T> T execute(String flowName, String correlationKey, Supplier<T> action) {
      this.calls++;
      this.flowName = flowName;
      this.correlationKey = correlationKey;
      return action.get();
    }
  }
}
