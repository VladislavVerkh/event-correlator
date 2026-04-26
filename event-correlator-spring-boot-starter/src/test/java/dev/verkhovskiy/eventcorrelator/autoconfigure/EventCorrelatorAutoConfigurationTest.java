package dev.verkhovskiy.eventcorrelator.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.eventcorrelator.EventCorrelationBoundary;
import dev.verkhovskiy.eventcorrelator.EventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventDefinitionRegistry;
import dev.verkhovskiy.eventcorrelator.EventFailureRetryPolicy;
import dev.verkhovskiy.eventcorrelator.EventFlowDefinition;
import dev.verkhovskiy.eventcorrelator.FailedEventRetryService;
import dev.verkhovskiy.eventcorrelator.PendingEventExpirationService;
import dev.verkhovskiy.eventcorrelator.postgres.PostgresEventBufferRepository;
import dev.verkhovskiy.eventcorrelator.postgres.PostgresEventCorrelationLock;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class EventCorrelatorAutoConfigurationTest {

  @Test
  void createsRuntimeBeansWhenJdbcInfrastructureExists() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventCorrelatorAutoConfiguration.class))
        .withBean(ObjectMapper.class, ObjectMapper::new)
        .withBean(NamedParameterJdbcTemplate.class, () -> mock(NamedParameterJdbcTemplate.class))
        .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
        .withBean(EventFlowDefinition.class, EventCorrelatorAutoConfigurationTest::flow)
        .run(
            context -> {
              assertThat(context).hasSingleBean(EventDefinitionRegistry.class);
              assertThat(context).hasSingleBean(PostgresEventCorrelationLock.class);
              assertThat(context).hasSingleBean(TransactionTemplate.class);
              assertThat(context).hasSingleBean(EventCorrelationBoundary.class);
              assertThat(context).hasSingleBean(EventFailureRetryPolicy.class);
              assertThat(context).hasSingleBean(PostgresEventBufferRepository.class);
              assertThat(context).hasSingleBean(EventCorrelator.class);
              assertThat(context).hasSingleBean(PendingEventExpirationService.class);
              assertThat(context).hasSingleBean(FailedEventRetryService.class);
            });
  }

  @Test
  void backsOffWhenDisabled() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventCorrelatorAutoConfiguration.class))
        .withPropertyValues("event.correlator.enabled=false")
        .withBean(EventFlowDefinition.class, EventCorrelatorAutoConfigurationTest::flow)
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(EventDefinitionRegistry.class);
              assertThat(context).doesNotHaveBean(EventCorrelator.class);
            });
  }

  private static EventFlowDefinition flow() {
    return EventFlowDefinition.builder("contract-events")
        .rootEvent(
            "contract.created", ContractCreated.class, ContractCreated::contractId, payload -> {})
        .build();
  }

  private record ContractCreated(String contractId) {}
}
