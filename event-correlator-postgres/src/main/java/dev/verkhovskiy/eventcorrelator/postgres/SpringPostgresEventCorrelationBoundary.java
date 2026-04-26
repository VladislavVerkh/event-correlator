package dev.verkhovskiy.eventcorrelator.postgres;

import dev.verkhovskiy.eventcorrelator.EventCorrelationBoundary;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.function.Supplier;
import org.springframework.transaction.support.TransactionTemplate;

/** Spring/PostgreSQL граница: открывает транзакцию и блокирует один correlation key. */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification =
        "TransactionTemplate и PostgresEventCorrelationLock являются внедренными"
            + " инфраструктурными Spring-бинами.")
public class SpringPostgresEventCorrelationBoundary implements EventCorrelationBoundary {

  private final TransactionTemplate transactionTemplate;
  private final PostgresEventCorrelationLock lock;

  public SpringPostgresEventCorrelationBoundary(
      TransactionTemplate transactionTemplate, PostgresEventCorrelationLock lock) {
    this.transactionTemplate = transactionTemplate;
    this.lock = lock;
  }

  @Override
  public <T> T execute(String flowName, String correlationKey, Supplier<T> action) {
    return transactionTemplate.execute(
        status -> {
          lock.lock(flowName, correlationKey);
          return action.get();
        });
  }
}
