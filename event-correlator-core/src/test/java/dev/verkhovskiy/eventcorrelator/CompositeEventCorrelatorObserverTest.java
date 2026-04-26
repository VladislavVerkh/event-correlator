package dev.verkhovskiy.eventcorrelator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CompositeEventCorrelatorObserverTest {

  @Test
  void ignoresObserverFailureAndNotifiesNextObserver() {
    AtomicInteger notified = new AtomicInteger();
    EventCorrelatorObserver throwingObserver =
        new EventCorrelatorObserver() {
          @Override
          public void pendingExpired(int expiredCount) {
            throw new IllegalStateException("observer failure");
          }
        };
    EventCorrelatorObserver recordingObserver =
        new EventCorrelatorObserver() {
          @Override
          public void pendingExpired(int expiredCount) {
            notified.addAndGet(expiredCount);
          }
        };
    EventCorrelatorObserver observer =
        CompositeEventCorrelatorObserver.of(List.of(throwingObserver, recordingObserver));

    observer.pendingExpired(3);

    assertThat(notified).hasValue(3);
  }
}
