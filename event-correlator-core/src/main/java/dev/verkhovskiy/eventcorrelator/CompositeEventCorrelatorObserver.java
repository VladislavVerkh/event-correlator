package dev.verkhovskiy.eventcorrelator;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Безопасно рассылает события наблюдаемости нескольким наблюдателям. */
public final class CompositeEventCorrelatorObserver implements EventCorrelatorObserver {

  private final List<EventCorrelatorObserver> observers;

  private CompositeEventCorrelatorObserver(List<EventCorrelatorObserver> observers) {
    this.observers = observers;
  }

  public static EventCorrelatorObserver of(List<EventCorrelatorObserver> observers) {
    List<EventCorrelatorObserver> copy =
        Objects.requireNonNullElse(observers, List.<EventCorrelatorObserver>of()).stream()
            .filter(Objects::nonNull)
            .toList();
    if (copy.isEmpty()) {
      return EventCorrelatorObserver.NOOP;
    }
    return new CompositeEventCorrelatorObserver(copy);
  }

  @Override
  public void eventAccepted(BufferedEvent event) {
    observe(observer -> observer.eventAccepted(event));
  }

  @Override
  public void eventDuplicate(BufferedEvent event) {
    observe(observer -> observer.eventDuplicate(event));
  }

  @Override
  public void eventPending(BufferedEvent event, List<String> missingDependencies) {
    List<String> missing = List.copyOf(missingDependencies);
    observe(observer -> observer.eventPending(event, missing));
  }

  @Override
  public void eventProcessed(BufferedEvent event, Duration handlingDuration) {
    observe(observer -> observer.eventProcessed(event, handlingDuration));
  }

  @Override
  public void eventFailed(BufferedEvent event, String failureMessage, Duration handlingDuration) {
    observe(observer -> observer.eventFailed(event, failureMessage, handlingDuration));
  }

  @Override
  public void pendingExpired(int expiredCount) {
    observe(observer -> observer.pendingExpired(expiredCount));
  }

  @Override
  public void failedRetryClaimed(int claimedCount) {
    observe(observer -> observer.failedRetryClaimed(claimedCount));
  }

  @Override
  public void failedManualReplayClaimed(EventPointer pointer, boolean claimed) {
    observe(observer -> observer.failedManualReplayClaimed(pointer, claimed));
  }

  private void observe(Consumer<EventCorrelatorObserver> action) {
    for (EventCorrelatorObserver observer : observers) {
      try {
        action.accept(observer);
      } catch (RuntimeException ignored) {
        // Ошибка наблюдателя не должна менять результат обработки бизнес-события.
      }
    }
  }
}
