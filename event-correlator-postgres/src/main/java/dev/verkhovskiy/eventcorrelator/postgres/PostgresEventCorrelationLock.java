package dev.verkhovskiy.eventcorrelator.postgres;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Берет PostgreSQL advisory lock на время текущей транзакции для одного business key. */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "NamedParameterJdbcTemplate является внедренным инфраструктурным Spring-бином.")
public class PostgresEventCorrelationLock {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  public PostgresEventCorrelationLock(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void lock(String flowName, String correlationKey) {
    String sql =
        """
        select pg_advisory_xact_lock(hashtext(:flowName), hashtext(:correlationKey))
        """;
    jdbcTemplate.query(
        sql,
        new MapSqlParameterSource()
            .addValue("flowName", flowName)
            .addValue("correlationKey", correlationKey),
        resultSet -> null);
  }
}
