package dev.verkhovskiy.eventcorrelator.autoconfigure;

import dev.verkhovskiy.eventcorrelator.EventCorrelatorObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/** Автоконфигурация Micrometer-метрик для event-correlator. */
@AutoConfiguration(after = EventCorrelatorAutoConfiguration.class)
@ConditionalOnProperty(
    prefix = "event.correlator",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
public class EventCorrelatorMicrometerAutoConfiguration {

  @Bean
  @ConditionalOnBean(MeterRegistry.class)
  EventCorrelatorObserver eventCorrelatorMicrometerObserver(MeterRegistry meterRegistry) {
    return new MicrometerEventCorrelatorObserver(meterRegistry);
  }
}
