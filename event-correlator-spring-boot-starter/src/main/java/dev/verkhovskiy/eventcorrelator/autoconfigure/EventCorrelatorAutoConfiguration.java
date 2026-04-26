package dev.verkhovskiy.eventcorrelator.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.eventcorrelator.DefaultEventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventBufferRepository;
import dev.verkhovskiy.eventcorrelator.EventCorrelationBoundary;
import dev.verkhovskiy.eventcorrelator.EventCorrelator;
import dev.verkhovskiy.eventcorrelator.EventDefinitionRegistry;
import dev.verkhovskiy.eventcorrelator.EventFlowDefinition;
import dev.verkhovskiy.eventcorrelator.PendingEventExpirationService;
import dev.verkhovskiy.eventcorrelator.postgres.PostgresEventBufferRepository;
import dev.verkhovskiy.eventcorrelator.postgres.PostgresEventCorrelationLock;
import dev.verkhovskiy.eventcorrelator.postgres.SpringPostgresEventCorrelationBoundary;
import java.time.Clock;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Автоконфигурация Spring Boot для event-correlator. */
@AutoConfiguration
@EnableConfigurationProperties(EventCorrelatorProperties.class)
@ConditionalOnProperty(
    prefix = "event.correlator",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class EventCorrelatorAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  EventDefinitionRegistry eventDefinitionRegistry(List<EventFlowDefinition> definitions) {
    return new EventDefinitionRegistry(definitions);
  }

  @Bean
  @ConditionalOnMissingBean
  Clock eventCorrelatorClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnClass(NamedParameterJdbcTemplate.class)
  @ConditionalOnBean(NamedParameterJdbcTemplate.class)
  PostgresEventCorrelationLock postgresEventCorrelationLock(
      NamedParameterJdbcTemplate jdbcTemplate) {
    return new PostgresEventCorrelationLock(jdbcTemplate);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnClass(TransactionTemplate.class)
  @ConditionalOnBean(PlatformTransactionManager.class)
  TransactionTemplate eventCorrelatorTransactionTemplate(
      PlatformTransactionManager transactionManager) {
    return new TransactionTemplate(transactionManager);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({TransactionTemplate.class, PostgresEventCorrelationLock.class})
  EventCorrelationBoundary eventCorrelationBoundary(
      TransactionTemplate transactionTemplate, PostgresEventCorrelationLock lock) {
    return new SpringPostgresEventCorrelationBoundary(transactionTemplate, lock);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnClass(NamedParameterJdbcTemplate.class)
  @ConditionalOnBean({
    NamedParameterJdbcTemplate.class,
    ObjectMapper.class,
    EventDefinitionRegistry.class
  })
  PostgresEventBufferRepository postgresEventBufferRepository(
      NamedParameterJdbcTemplate jdbcTemplate,
      ObjectMapper objectMapper,
      EventDefinitionRegistry definitionRegistry) {
    return new PostgresEventBufferRepository(jdbcTemplate, objectMapper, definitionRegistry);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({EventDefinitionRegistry.class, EventBufferRepository.class, Clock.class})
  EventCorrelator eventCorrelator(
      EventDefinitionRegistry definitionRegistry,
      EventBufferRepository repository,
      Clock clock,
      ObjectProvider<EventCorrelationBoundary> boundary) {
    EventCorrelationBoundary eventCorrelationBoundary = boundary.getIfAvailable();
    if (eventCorrelationBoundary == null) {
      return new DefaultEventCorrelator(definitionRegistry, repository, clock);
    }
    return new DefaultEventCorrelator(
        definitionRegistry, repository, clock, eventCorrelationBoundary);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({EventBufferRepository.class, Clock.class})
  PendingEventExpirationService pendingEventExpirationService(
      EventBufferRepository repository, Clock clock, EventCorrelatorProperties properties) {
    return new PendingEventExpirationService(
        repository, clock, properties.getExpirationBatchSize());
  }
}
