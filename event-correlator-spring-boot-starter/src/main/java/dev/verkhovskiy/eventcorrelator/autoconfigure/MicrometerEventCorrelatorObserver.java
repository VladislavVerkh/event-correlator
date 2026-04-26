package dev.verkhovskiy.eventcorrelator.autoconfigure;

import dev.verkhovskiy.eventcorrelator.BufferedEvent;
import dev.verkhovskiy.eventcorrelator.EventCorrelatorObserver;
import dev.verkhovskiy.eventcorrelator.EventPointer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;

/** Публикует события наблюдаемости event-correlator как метрики Micrometer. */
final class MicrometerEventCorrelatorObserver implements EventCorrelatorObserver {

  private final MeterRegistry meterRegistry;

  MicrometerEventCorrelatorObserver(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public void eventAccepted(BufferedEvent event) {
    increment("event.correlator.events.accepted", eventTags(event), 1);
  }

  @Override
  public void eventDuplicate(BufferedEvent event) {
    increment("event.correlator.events.outcome", outcomeTags(event, "duplicate"), 1);
  }

  @Override
  public void eventPending(BufferedEvent event, List<String> missingDependencies) {
    increment("event.correlator.events.outcome", outcomeTags(event, "pending"), 1);
  }

  @Override
  public void eventProcessed(BufferedEvent event, Duration handlingDuration) {
    increment("event.correlator.events.outcome", outcomeTags(event, "processed"), 1);
    recordHandlingDuration(event, "processed", handlingDuration);
  }

  @Override
  public void eventFailed(BufferedEvent event, String failureMessage, Duration handlingDuration) {
    increment("event.correlator.events.outcome", outcomeTags(event, "failed"), 1);
    recordHandlingDuration(event, "failed", handlingDuration);
  }

  @Override
  public void pendingExpired(int expiredCount) {
    increment("event.correlator.pending.expired", Tags.empty(), expiredCount);
  }

  @Override
  public void failedRetryClaimed(int claimedCount) {
    increment("event.correlator.failed.retry.claimed", Tags.empty(), claimedCount);
  }

  @Override
  public void failedManualReplayClaimed(EventPointer pointer, boolean claimed) {
    Tags tags = Tags.of("flow", pointer.flowName(), "result", claimed ? "claimed" : "not_found");
    increment("event.correlator.failed.manual.replay", tags, 1);
  }

  private void recordHandlingDuration(
      BufferedEvent event, String status, Duration handlingDuration) {
    Timer.builder("event.correlator.handler.duration")
        .tags(outcomeTags(event, status))
        .register(meterRegistry)
        .record(handlingDuration);
  }

  private void increment(String name, Tags tags, int amount) {
    if (amount <= 0) {
      return;
    }
    Counter.builder(name).tags(tags).register(meterRegistry).increment(amount);
  }

  private Tags eventTags(BufferedEvent event) {
    return Tags.of("flow", event.flowName(), "event_type", event.eventType());
  }

  private Tags outcomeTags(BufferedEvent event, String status) {
    return Tags.of("flow", event.flowName(), "event_type", event.eventType(), "status", status);
  }
}
