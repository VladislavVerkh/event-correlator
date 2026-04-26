package dev.verkhovskiy.eventcorrelator.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.eventcorrelator.DefaultEventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventCorrelationResult;
import dev.verkhovskiy.eventcorrelator.EventCorrelationStatus;
import dev.verkhovskiy.eventcorrelator.EventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventDefinitionRegistry;
import dev.verkhovskiy.eventcorrelator.EventFailureRetryPolicy;
import dev.verkhovskiy.eventcorrelator.EventFlowDefinition;
import dev.verkhovskiy.eventcorrelator.EventInboxStatistics;
import dev.verkhovskiy.eventcorrelator.EventInboxStatisticsInspector;
import dev.verkhovskiy.eventcorrelator.EventInboxStatisticsQuery;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import dev.verkhovskiy.eventcorrelator.FailedEventReplayService;
import dev.verkhovskiy.eventcorrelator.FailedEventRetryService;
import dev.verkhovskiy.eventcorrelator.IncomingEvent;
import dev.verkhovskiy.eventcorrelator.PendingEventExpirationService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class PostgresEventCorrelatorIntegrationTest {

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17-alpine");

  private static NamedParameterJdbcTemplate jdbcTemplate;

  private final List<String> handledEvents = new CopyOnWriteArrayList<>();

  @BeforeAll
  static void migrateDatabase() {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(POSTGRES.getDriverClassName());
    dataSource.setUrl(POSTGRES.getJdbcUrl());
    dataSource.setUsername(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());

    new ResourceDatabasePopulator(
            new ClassPathResource("db/migration/V1__event_correlator.sql"),
            new ClassPathResource("db/migration/V2__event_retry_metadata.sql"))
        .execute(dataSource);
    jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
  }

  @BeforeEach
  void cleanInbox() {
    jdbcTemplate.getJdbcTemplate().update("truncate table ec_event_inbox");
    handledEvents.clear();
  }

  @AfterEach
  void clearHandlers() {
    handledEvents.clear();
  }

  @Test
  void processesChildWhenRootArrivesAfterChild() {
    EventCorrelator correlator =
        correlator(
            EventFlowDefinition.builder("contract-events")
                .rootEvent(
                    "contract.created",
                    ContractCreated.class,
                    ContractCreated::contractId,
                    payload -> handledEvents.add("root:" + payload.contractId()))
                .event("payment.schedule.changed", PaymentScheduleChanged.class)
                .requiresRoot("contract.created")
                .correlationKey(PaymentScheduleChanged::contractId)
                .handler(payload -> handledEvents.add("schedule:" + payload.contractId()))
                .add()
                .build());

    EventCorrelationResult childResult =
        correlator.accept(
            event(
                "payment.schedule.changed",
                "event-2",
                "contract-1",
                new PaymentScheduleChanged("contract-1")));

    assertThat(childResult.status()).isEqualTo(EventCorrelationStatus.PENDING);
    assertThat(status("event-2")).isEqualTo(EventStatus.PENDING);
    assertThat(handledEvents).isEmpty();

    EventCorrelationResult rootResult =
        correlator.accept(
            event("contract.created", "event-1", "contract-1", new ContractCreated("contract-1")));

    assertThat(rootResult.status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(handledEvents).containsExactly("root:contract-1", "schedule:contract-1");
    assertThat(status("event-1")).isEqualTo(EventStatus.PROCESSED);
    assertThat(status("event-2")).isEqualTo(EventStatus.PROCESSED);
  }

  @Test
  void waitsForSeveralDependenciesBeforeProcessingDependentEvent() {
    EventCorrelator correlator =
        correlator(
            EventFlowDefinition.builder("contract-events")
                .rootEvent(
                    "contract.created",
                    ContractCreated.class,
                    ContractCreated::contractId,
                    payload -> handledEvents.add("root"))
                .event("payment.schedule.changed", PaymentScheduleChanged.class)
                .requiresRoot("contract.created")
                .correlationKey(PaymentScheduleChanged::contractId)
                .handler(payload -> handledEvents.add("schedule"))
                .add()
                .event("contract.attributes.changed", ContractAttributesChanged.class)
                .requiresRoot("contract.created")
                .correlationKey(ContractAttributesChanged::contractId)
                .handler(payload -> handledEvents.add("attributes"))
                .add()
                .event("contract.ready.for.scoring", ContractReadyForScoring.class)
                .requires("contract.created")
                .requires("payment.schedule.changed")
                .requires("contract.attributes.changed")
                .correlationKey(ContractReadyForScoring::contractId)
                .handler(payload -> handledEvents.add("ready"))
                .add()
                .build());

    correlator.accept(
        event(
            "contract.ready.for.scoring",
            "event-4",
            "contract-1",
            new ContractReadyForScoring("contract-1")));
    correlator.accept(
        event("contract.created", "event-1", "contract-1", new ContractCreated("contract-1")));
    correlator.accept(
        event(
            "payment.schedule.changed",
            "event-2",
            "contract-1",
            new PaymentScheduleChanged("contract-1")));

    assertThat(status("event-4")).isEqualTo(EventStatus.PENDING);
    assertThat(handledEvents).containsExactly("root", "schedule");

    correlator.accept(
        event(
            "contract.attributes.changed",
            "event-3",
            "contract-1",
            new ContractAttributesChanged("contract-1")));

    assertThat(status("event-4")).isEqualTo(EventStatus.PROCESSED);
    assertThat(handledEvents).containsExactly("root", "schedule", "attributes", "ready");
  }

  @Test
  void keepsSingleInboxRecordForDuplicateEvent() {
    AtomicInteger handledCount = new AtomicInteger();
    EventCorrelator correlator =
        correlator(
            EventFlowDefinition.builder("contract-events")
                .rootEvent(
                    "contract.created",
                    ContractCreated.class,
                    ContractCreated::contractId,
                    payload -> handledCount.incrementAndGet())
                .build());
    IncomingEvent<ContractCreated> event =
        event("contract.created", "event-1", "contract-1", new ContractCreated("contract-1"));

    EventCorrelationResult firstResult = correlator.accept(event);
    EventCorrelationResult secondResult = correlator.accept(event);

    assertThat(firstResult.status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(secondResult.status()).isEqualTo(EventCorrelationStatus.DUPLICATE);
    assertThat(handledCount).hasValue(1);
    assertThat(status("event-1")).isEqualTo(EventStatus.PROCESSED);
    assertThat(inboxCount()).isEqualTo(1);
  }

  @Test
  void serializesConcurrentEventsForSameCorrelationKey() throws Exception {
    AtomicInteger activeHandlers = new AtomicInteger();
    AtomicInteger maxActiveHandlers = new AtomicInteger();
    CountDownLatch firstHandlerStarted = new CountDownLatch(1);
    Consumer<String> handler =
        eventName -> {
          int active = activeHandlers.incrementAndGet();
          maxActiveHandlers.updateAndGet(current -> Math.max(current, active));
          firstHandlerStarted.countDown();
          try {
            Thread.sleep(300);
            handledEvents.add(eventName);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Handler was interrupted", e);
          } finally {
            activeHandlers.decrementAndGet();
          }
        };
    EventCorrelator correlator =
        correlator(
            EventFlowDefinition.builder("contract-events")
                .rootEvent(
                    "contract.created",
                    ContractCreated.class,
                    ContractCreated::contractId,
                    payload -> handler.accept("created"))
                .rootEvent(
                    "contract.activated",
                    ContractActivated.class,
                    ContractActivated::contractId,
                    payload -> handler.accept("activated"))
                .build());
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      Future<EventCorrelationResult> firstResult =
          executorService.submit(
              accept(
                  correlator,
                  event(
                      "contract.created",
                      "event-1",
                      "contract-1",
                      new ContractCreated("contract-1"))));
      assertThat(firstHandlerStarted.await(5, TimeUnit.SECONDS)).isTrue();
      Future<EventCorrelationResult> secondResult =
          executorService.submit(
              accept(
                  correlator,
                  event(
                      "contract.activated",
                      "event-2",
                      "contract-1",
                      new ContractActivated("contract-1"))));

      assertThat(firstResult.get(5, TimeUnit.SECONDS).status())
          .isEqualTo(EventCorrelationStatus.PROCESSED);
      assertThat(secondResult.get(5, TimeUnit.SECONDS).status())
          .isEqualTo(EventCorrelationStatus.PROCESSED);
    } finally {
      executorService.shutdownNow();
    }

    assertThat(maxActiveHandlers).hasValue(1);
    assertThat(handledEvents).containsExactly("created", "activated");
    assertThat(status("event-1")).isEqualTo(EventStatus.PROCESSED);
    assertThat(status("event-2")).isEqualTo(EventStatus.PROCESSED);
  }

  @Test
  void retriesFailedEventThroughPostgresInbox() {
    AtomicInteger attempts = new AtomicInteger();
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> {
                  if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("temporary failure");
                  }
                  handledEvents.add("processed");
                })
            .build();
    PostgresEventBufferRepository repository = repository(flow);
    EventCorrelator correlator =
        correlator(flow, repository, new EventFailureRetryPolicy(2, Duration.ZERO));
    FailedEventRetryService retryService =
        new FailedEventRetryService(repository, correlator, fixedClock(), 10);

    EventCorrelationResult failedResult =
        correlator.accept(
            event("contract.created", "event-1", "contract-1", new ContractCreated("contract-1")));

    assertThat(failedResult.status()).isEqualTo(EventCorrelationStatus.FAILED);
    assertThat(status("event-1")).isEqualTo(EventStatus.FAILED);
    assertThat(attempts("event-1")).isEqualTo(1);

    int retried = retryService.runOnce();

    assertThat(retried).isEqualTo(1);
    assertThat(status("event-1")).isEqualTo(EventStatus.PROCESSED);
    assertThat(attempts).hasValue(2);
    assertThat(handledEvents).containsExactly("processed");
  }

  @Test
  void expiresPendingEventThroughPostgresInbox() {
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> handledEvents.add("root"))
            .event("payment.schedule.changed", PaymentScheduleChanged.class)
            .requiresRoot("contract.created")
            .correlationKey(PaymentScheduleChanged::contractId)
            .handler(payload -> handledEvents.add("schedule"))
            .add()
            .orphanRetention(Duration.ofSeconds(30))
            .build();
    PostgresEventBufferRepository repository = repository(flow);
    EventCorrelator correlator = correlator(flow, repository, EventFailureRetryPolicy.noRetries());
    PendingEventExpirationService expirationService =
        new PendingEventExpirationService(repository, fixedClockAt("2026-01-01T00:00:31Z"), 10);

    EventCorrelationResult childResult =
        correlator.accept(
            event(
                "payment.schedule.changed",
                "event-2",
                "contract-1",
                new PaymentScheduleChanged("contract-1")));

    assertThat(childResult.status()).isEqualTo(EventCorrelationStatus.PENDING);
    assertThat(status("event-2")).isEqualTo(EventStatus.PENDING);

    int expired = expirationService.runOnce();

    assertThat(expired).isEqualTo(1);
    assertThat(status("event-2")).isEqualTo(EventStatus.EXPIRED);
    assertThat(handledEvents).isEmpty();
  }

  @Test
  void manuallyReplaysFailedEventThroughPostgresInbox() {
    AtomicInteger attempts = new AtomicInteger();
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> {
                  if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("manual retry required");
                  }
                  handledEvents.add("processed");
                })
            .build();
    PostgresEventBufferRepository repository = repository(flow);
    EventCorrelator correlator = correlator(flow, repository, EventFailureRetryPolicy.noRetries());
    FailedEventReplayService replayService = new FailedEventReplayService(repository, correlator);

    EventCorrelationResult failedResult =
        correlator.accept(
            event("contract.created", "event-1", "contract-1", new ContractCreated("contract-1")));

    assertThat(failedResult.status()).isEqualTo(EventCorrelationStatus.FAILED);
    assertThat(status("event-1")).isEqualTo(EventStatus.FAILED);

    var replayResult = replayService.replayFailed("contract-events", "event-1");

    assertThat(replayResult).isPresent();
    assertThat(replayResult.orElseThrow().status()).isEqualTo(EventCorrelationStatus.PROCESSED);
    assertThat(status("event-1")).isEqualTo(EventStatus.PROCESSED);
    assertThat(attempts).hasValue(2);
    assertThat(handledEvents).containsExactly("processed");
  }

  @Test
  void returnsInboxStatisticsThroughPostgresInspector() {
    EventFlowDefinition flow =
        EventFlowDefinition.builder("contract-events")
            .rootEvent(
                "contract.created",
                ContractCreated.class,
                ContractCreated::contractId,
                payload -> handledEvents.add("created"))
            .rootEvent(
                "contract.failed",
                ContractFailed.class,
                ContractFailed::contractId,
                payload -> {
                  throw new IllegalStateException("temporary failure");
                })
            .event("payment.schedule.changed", PaymentScheduleChanged.class)
            .requiresRoot("contract.created")
            .correlationKey(PaymentScheduleChanged::contractId)
            .handler(payload -> handledEvents.add("schedule"))
            .add()
            .orphanRetention(Duration.ofSeconds(30))
            .build();
    PostgresEventBufferRepository repository = repository(flow);
    EventCorrelator correlator =
        correlator(flow, repository, new EventFailureRetryPolicy(2, Duration.ZERO));
    PendingEventExpirationService expirationService =
        new PendingEventExpirationService(repository, fixedClockAt("2026-01-01T00:00:31Z"), 10);
    EventInboxStatisticsInspector statisticsInspector =
        new PostgresEventInboxStatisticsInspector(jdbcTemplate);

    correlator.accept(
        event("contract.created", "event-1", "contract-1", new ContractCreated("contract-1")));
    correlator.accept(
        event(
            "payment.schedule.changed",
            "event-2",
            "contract-2",
            new PaymentScheduleChanged("contract-2")));
    expirationService.runOnce();
    correlator.accept(
        event(
            "payment.schedule.changed",
            "event-3",
            "contract-3",
            new PaymentScheduleChanged("contract-3")));
    correlator.accept(
        event("contract.failed", "event-4", "contract-4", new ContractFailed("contract-4")));

    EventInboxStatistics statistics =
        statisticsInspector.getStatistics(
            EventInboxStatisticsQuery.builder().flowName("contract-events").build());

    assertThat(statistics.count(EventStatus.PROCESSED)).isEqualTo(1);
    assertThat(statistics.count(EventStatus.EXPIRED)).isEqualTo(1);
    assertThat(statistics.count(EventStatus.PENDING)).isEqualTo(1);
    assertThat(statistics.count(EventStatus.FAILED)).isEqualTo(1);
    assertThat(statistics.retryReadyCount()).isEqualTo(1);
    assertThat(statistics.expiredCount()).isEqualTo(1);
    assertThat(statistics.oldestPendingReceivedAt())
        .isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
    assertThat(statistics.oldestFailedAt()).isNotNull();
  }

  private Callable<EventCorrelationResult> accept(
      EventCorrelator correlator, IncomingEvent<?> event) {
    return () -> correlator.accept(event);
  }

  private EventCorrelator correlator(EventFlowDefinition flow) {
    return correlator(flow, repository(flow), EventFailureRetryPolicy.noRetries());
  }

  private EventCorrelator correlator(
      EventFlowDefinition flow,
      PostgresEventBufferRepository repository,
      EventFailureRetryPolicy retryPolicy) {
    EventDefinitionRegistry definitionRegistry = new EventDefinitionRegistry(List.of(flow));
    return new DefaultEventCorrelator(
        definitionRegistry, repository, fixedClock(), boundary(), retryPolicy);
  }

  private PostgresEventBufferRepository repository(EventFlowDefinition flow) {
    PostgresEventBufferRepository repository =
        new PostgresEventBufferRepository(
            jdbcTemplate, new ObjectMapper(), new EventDefinitionRegistry(List.of(flow)));
    return repository;
  }

  private SpringPostgresEventCorrelationBoundary boundary() {
    return new SpringPostgresEventCorrelationBoundary(
        new TransactionTemplate(
            new DataSourceTransactionManager(jdbcTemplate.getJdbcTemplate().getDataSource())),
        new PostgresEventCorrelationLock(jdbcTemplate));
  }

  private Clock fixedClock() {
    return fixedClockAt("2026-01-01T00:00:00Z");
  }

  private Clock fixedClockAt(String instant) {
    return Clock.fixed(Instant.parse(instant), ZoneOffset.UTC);
  }

  private <T> IncomingEvent<T> event(
      String eventType, String eventId, String correlationKey, T payload) {
    return IncomingEvent.<T>builder()
        .flowName("contract-events")
        .eventType(eventType)
        .eventId(eventId)
        .correlationKey(correlationKey)
        .payload(payload)
        .headers(Map.of("source", "integration-test"))
        .receivedAt(Instant.parse("2026-01-01T00:00:00Z"))
        .build();
  }

  private EventStatus status(String eventId) {
    return EventStatus.valueOf(
        jdbcTemplate.queryForObject(
            "select status from ec_event_inbox where event_id = :eventId",
            new MapSqlParameterSource().addValue("eventId", eventId),
            String.class));
  }

  private int inboxCount() {
    Integer count =
        jdbcTemplate.queryForObject(
            "select count(*) from ec_event_inbox", new MapSqlParameterSource(), Integer.class);
    return count == null ? 0 : count;
  }

  private int attempts(String eventId) {
    Integer attempts =
        jdbcTemplate.queryForObject(
            "select attempts from ec_event_inbox where event_id = :eventId",
            new MapSqlParameterSource().addValue("eventId", eventId),
            Integer.class);
    return attempts == null ? 0 : attempts;
  }

  private record ContractCreated(String contractId) {}

  private record ContractActivated(String contractId) {}

  private record ContractFailed(String contractId) {}

  private record PaymentScheduleChanged(String contractId) {}

  private record ContractAttributesChanged(String contractId) {}

  private record ContractReadyForScoring(String contractId) {}
}
