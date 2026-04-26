package dev.verkhovskiy.eventcorrelator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class PendingEventExpirationServiceTest {

  @Test
  void expiresPendingEventsThroughRepository() {
    RecordingEventBufferRepository repository = new RecordingEventBufferRepository();
    PendingEventExpirationService service =
        new PendingEventExpirationService(
            repository, Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC), 50);

    int expired = service.runOnce();

    assertThat(expired).isEqualTo(3);
    assertThat(repository.now).isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
    assertThat(repository.limit).isEqualTo(50);
  }

  private static final class RecordingEventBufferRepository implements EventBufferRepository {
    private Instant now;
    private int limit;

    @Override
    public boolean insertIfAbsent(BufferedEvent event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markPending(EventPointer pointer, String reason, Instant expiresAt) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markProcessed(EventPointer pointer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markFailed(EventPointer pointer, String failureMessage) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markExpired(EventPointer pointer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean existsProcessed(String flowName, String eventType, String correlationKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<BufferedEvent> findPending(String flowName, String correlationKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int expirePendingBefore(Instant now, int limit) {
      this.now = now;
      this.limit = limit;
      return 3;
    }
  }
}
