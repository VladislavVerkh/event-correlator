package dev.verkhovskiy.eventcorrelator.postgres;

import dev.verkhovskiy.eventcorrelator.EventInboxInspector;
import dev.verkhovskiy.eventcorrelator.EventInboxQuery;
import dev.verkhovskiy.eventcorrelator.EventInboxRecord;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** PostgreSQL-инспектор durable inbox событий только для чтения. */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "NamedParameterJdbcTemplate является внедренным инфраструктурным Spring-бином.")
public class PostgresEventInboxInspector implements EventInboxInspector {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  public PostgresEventInboxInspector(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public Optional<EventInboxRecord> findEvent(String flowName, String eventId) {
    String sql =
        """
        select flow_name,
               event_id,
               event_type,
               correlation_key,
               status,
               pending_reason,
               failure_message,
               attempts,
               occurred_at,
               received_at,
               processed_at,
               failed_at,
               expires_at,
               next_retry_at,
               created_at,
               updated_at
        from ec_event_inbox
        where flow_name = :flowName
          and event_id = :eventId
        """;
    List<EventInboxRecord> records =
        jdbcTemplate.query(
            sql,
            new MapSqlParameterSource().addValue("flowName", flowName).addValue("eventId", eventId),
            rowMapper());
    return records.stream().findFirst();
  }

  @Override
  public List<EventInboxRecord> findEvents(EventInboxQuery query) {
    StringBuilder sql =
        new StringBuilder(
            """
            select flow_name,
                   event_id,
                   event_type,
                   correlation_key,
                   status,
                   pending_reason,
                   failure_message,
                   attempts,
                   occurred_at,
                   received_at,
                   processed_at,
                   failed_at,
                   expires_at,
                   next_retry_at,
                   created_at,
                   updated_at
            from ec_event_inbox
            where 1 = 1
            """);
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("limit", query.limit());
    appendFilter(sql, params, "flow_name", "flowName", query.flowName());
    appendFilter(sql, params, "event_type", "eventType", query.eventType());
    appendFilter(sql, params, "correlation_key", "correlationKey", query.correlationKey());
    if (query.status() != null) {
      sql.append("  and status = :status\n");
      params.addValue("status", query.status().name());
    }
    sql.append("order by received_at desc, created_at desc\n");
    sql.append("limit :limit\n");
    return jdbcTemplate.query(sql.toString(), params, rowMapper());
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

  private RowMapper<EventInboxRecord> rowMapper() {
    return (resultSet, rowNumber) -> mapRecord(resultSet);
  }

  private EventInboxRecord mapRecord(ResultSet resultSet) throws SQLException {
    return new EventInboxRecord(
        resultSet.getString("flow_name"),
        resultSet.getString("event_id"),
        resultSet.getString("event_type"),
        resultSet.getString("correlation_key"),
        EventStatus.valueOf(resultSet.getString("status")),
        resultSet.getString("pending_reason"),
        resultSet.getString("failure_message"),
        resultSet.getInt("attempts"),
        instant(resultSet.getTimestamp("occurred_at")),
        instant(resultSet.getTimestamp("received_at")),
        instant(resultSet.getTimestamp("processed_at")),
        instant(resultSet.getTimestamp("failed_at")),
        instant(resultSet.getTimestamp("expires_at")),
        instant(resultSet.getTimestamp("next_retry_at")),
        instant(resultSet.getTimestamp("created_at")),
        instant(resultSet.getTimestamp("updated_at")));
  }

  private Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
