package dev.verkhovskiy.eventcorrelator.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import dev.verkhovskiy.eventcorrelator.BufferedEvent;
import dev.verkhovskiy.eventcorrelator.EventPointer;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MicrometerEventCorrelatorObserverTest {

  @Test
  void publishesCountersAndTimers() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MicrometerEventCorrelatorObserver observer =
        new MicrometerEventCorrelatorObserver(meterRegistry);
    BufferedEvent event = event();

    observer.eventAccepted(event);
    observer.eventPending(event, List.of("contract.created"));
    observer.eventProcessed(event, Duration.ofMillis(25));
    observer.pendingExpired(3);
    observer.failedRetryClaimed(2);
    observer.failedManualReplayClaimed(new EventPointer("contract-events", "event-1"), true);

    assertThat(
            meterRegistry
                .get("event.correlator.events.accepted")
                .tag("flow", "contract-events")
                .tag("event_type", "contract.created")
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(
            meterRegistry
                .get("event.correlator.events.outcome")
                .tag("status", "pending")
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(
            meterRegistry
                .get("event.correlator.handler.duration")
                .tag("status", "processed")
                .timer()
                .count())
        .isEqualTo(1);
    assertThat(meterRegistry.get("event.correlator.pending.expired").counter().count())
        .isEqualTo(3);
    assertThat(meterRegistry.get("event.correlator.failed.retry.claimed").counter().count())
        .isEqualTo(2);
    assertThat(
            meterRegistry
                .get("event.correlator.failed.manual.replay")
                .tag("result", "claimed")
                .counter()
                .count())
        .isEqualTo(1);
  }

  private BufferedEvent event() {
    return new BufferedEvent(
        "contract-events",
        "contract.created",
        "event-1",
        "contract-1",
        new ContractCreated("contract-1"),
        Map.of(),
        null,
        Instant.parse("2026-01-01T00:00:00Z"),
        EventStatus.RECEIVED);
  }

  private record ContractCreated(String contractId) {}
}
