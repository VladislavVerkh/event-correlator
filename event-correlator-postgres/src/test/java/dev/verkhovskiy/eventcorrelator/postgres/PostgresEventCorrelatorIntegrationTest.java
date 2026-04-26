package dev.verkhovskiy.eventcorrelator.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.eventcorrelator.DefaultEventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventCorrelationResult;
import dev.verkhovskiy.eventcorrelator.EventCorrelationStatus;
import dev.verkhovskiy.eventcorrelator.EventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventDefinitionRegistry;
import dev.verkhovskiy.eventcorrelator.EventFlowDefinition;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import dev.verkhovskiy.eventcorrelator.IncomingEvent;
import java.time.Clock;
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

  private Callable<EventCorrelationResult> accept(
      EventCorrelator correlator, IncomingEvent<?> event) {
    return () -> correlator.accept(event);
  }

  private EventCorrelator correlator(EventFlowDefinition flow) {
    PostgresEventBufferRepository repository =
        new PostgresEventBufferRepository(
            jdbcTemplate, new ObjectMapper(), new EventDefinitionRegistry(List.of(flow)));
    SpringPostgresEventCorrelationBoundary boundary =
        new SpringPostgresEventCorrelationBoundary(
            new TransactionTemplate(
                new DataSourceTransactionManager(jdbcTemplate.getJdbcTemplate().getDataSource())),
            new PostgresEventCorrelationLock(jdbcTemplate));
    return new DefaultEventCorrelator(
        new EventDefinitionRegistry(List.of(flow)),
        repository,
        Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
        boundary);
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

  private record ContractCreated(String contractId) {}

  private record ContractActivated(String contractId) {}

  private record PaymentScheduleChanged(String contractId) {}

  private record ContractAttributesChanged(String contractId) {}

  private record ContractReadyForScoring(String contractId) {}
}
