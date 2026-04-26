package dev.verkhovskiy.eventcorrelator.postgres;

import dev.verkhovskiy.eventcorrelator.EventInboxStatistics;
import dev.verkhovskiy.eventcorrelator.EventInboxStatisticsInspector;
import dev.verkhovskiy.eventcorrelator.EventInboxStatisticsQuery;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** PostgreSQL-инспектор агрегированной статистики durable inbox. */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "NamedParameterJdbcTemplate является внедренным инфраструктурным Spring-бином.")
public class PostgresEventInboxStatisticsInspector implements EventInboxStatisticsInspector {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  public PostgresEventInboxStatisticsInspector(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public EventInboxStatistics getStatistics(EventInboxStatisticsQuery query) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    String filters = filters(query, params);
    Map<EventStatus, Long> countByStatus = countByStatus(filters, params);
    return aggregate(filters, params, countByStatus);
  }

  private Map<EventStatus, Long> countByStatus(String filters, MapSqlParameterSource params) {
    String sql =
        """
        select status, count(*) as event_count
        from ec_event_inbox
        where 1 = 1
        """
            + filters
            + "group by status\n";
    Map<EventStatus, Long> counts = new EnumMap<>(EventStatus.class);
    for (EventStatus status : EventStatus.values()) {
      counts.put(status, 0L);
    }
    jdbcTemplate.query(
        sql,
        params,
        resultSet -> {
          while (resultSet.next()) {
            counts.put(
                EventStatus.valueOf(resultSet.getString("status")),
                resultSet.getLong("event_count"));
          }
          return counts;
        });
    return counts;
  }

  private EventInboxStatistics aggregate(
      String filters, MapSqlParameterSource params, Map<EventStatus, Long> countByStatus) {
    String sql =
        """
        select min(received_at) filter (where status = 'PENDING') as oldest_pending_received_at,
               min(failed_at) filter (where status = 'FAILED') as oldest_failed_at,
               count(*) filter (
                 where status = 'FAILED'
                   and next_retry_at is not null
                   and next_retry_at <= now()
               ) as retry_ready_count,
               count(*) filter (where status = 'EXPIRED') as expired_count
        from ec_event_inbox
        where 1 = 1
        """
            + filters;
    return jdbcTemplate.queryForObject(
        sql, params, (resultSet, rowNumber) -> mapStatistics(resultSet, countByStatus));
  }

  private EventInboxStatistics mapStatistics(
      ResultSet resultSet, Map<EventStatus, Long> countByStatus) throws SQLException {
    return new EventInboxStatistics(
        countByStatus,
        instant(resultSet.getTimestamp("oldest_pending_received_at")),
        instant(resultSet.getTimestamp("oldest_failed_at")),
        resultSet.getLong("retry_ready_count"),
        resultSet.getLong("expired_count"));
  }

  private String filters(EventInboxStatisticsQuery query, MapSqlParameterSource params) {
    StringBuilder filters = new StringBuilder();
    appendFilter(filters, params, "flow_name", "flowName", query.flowName());
    appendFilter(filters, params, "event_type", "eventType", query.eventType());
    appendFilter(filters, params, "correlation_key", "correlationKey", query.correlationKey());
    return filters.toString();
  }

  private void appendFilter(
      StringBuilder sql,
      MapSqlParameterSource params,
      String columnName,
      String parameterName,
      String value) {
    if (value == null || value.isBlank()) {
      return;
    }
    sql.append("  and ").append(columnName).append(" = :").append(parameterName).append("\n");
    params.addValue(parameterName, value);
  }

  private Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
