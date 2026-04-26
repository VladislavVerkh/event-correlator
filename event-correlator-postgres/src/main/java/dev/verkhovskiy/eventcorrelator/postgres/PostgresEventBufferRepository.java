package dev.verkhovskiy.eventcorrelator.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.eventcorrelator.BufferedEvent;
import dev.verkhovskiy.eventcorrelator.EventBufferRepository;
import dev.verkhovskiy.eventcorrelator.EventDefinitionRegistry;
import dev.verkhovskiy.eventcorrelator.EventFlowDefinition;
import dev.verkhovskiy.eventcorrelator.EventPointer;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import dev.verkhovskiy.eventcorrelator.EventTypeDefinition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** PostgreSQL repository для durable inbox событий. */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification =
        "NamedParameterJdbcTemplate, ObjectMapper и registry являются внедренными инфраструктурными"
            + " зависимостями.")
public class PostgresEventBufferRepository implements EventBufferRepository {

  private static final TypeReference<Map<String, Object>> HEADERS_TYPE = new TypeReference<>() {};

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private final EventDefinitionRegistry definitionRegistry;

  public PostgresEventBufferRepository(
      NamedParameterJdbcTemplate jdbcTemplate,
      ObjectMapper objectMapper,
      EventDefinitionRegistry definitionRegistry) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
    this.definitionRegistry = definitionRegistry;
  }

  @Override
  public boolean insertIfAbsent(BufferedEvent event) {
    String sql =
        """
        insert into ec_event_inbox (
          flow_name,
          event_id,
          event_type,
          correlation_key,
          payload_json,
          headers_json,
          occurred_at,
          received_at,
          status
        )
        values (
          :flowName,
          :eventId,
          :eventType,
          :correlationKey,
          cast(:payloadJson as jsonb),
          cast(:headersJson as jsonb),
          :occurredAt,
          :receivedAt,
          :status
        )
        on conflict (flow_name, event_id) do nothing
        """;

    int inserted =
        jdbcTemplate.update(
            sql,
            new MapSqlParameterSource()
                .addValue("flowName", event.flowName())
                .addValue("eventId", event.eventId())
                .addValue("eventType", event.eventType())
                .addValue("correlationKey", event.correlationKey())
                .addValue("payloadJson", writeJson(event.payload()))
                .addValue("headersJson", writeJson(event.headers()))
                .addValue("occurredAt", timestamp(event.occurredAt()))
                .addValue("receivedAt", timestamp(event.receivedAt()))
                .addValue("status", event.status().name()));
    return inserted == 1;
  }

  @Override
  public void markPending(EventPointer pointer, String reason, Instant expiresAt) {
    String sql =
        """
        update ec_event_inbox
        set status = :status,
            pending_reason = :reason,
            expires_at = :expiresAt,
            updated_at = now()
        where flow_name = :flowName and event_id = :eventId
        """;
    jdbcTemplate.update(
        sql,
        pointerParameters(pointer)
            .addValue("status", EventStatus.PENDING.name())
            .addValue("reason", reason)
            .addValue("expiresAt", timestamp(expiresAt)));
  }

  @Override
  public void markProcessed(EventPointer pointer) {
    String sql =
        """
        update ec_event_inbox
        set status = :status,
            processed_at = now(),
            updated_at = now()
        where flow_name = :flowName and event_id = :eventId
        """;
    jdbcTemplate.update(
        sql, pointerParameters(pointer).addValue("status", EventStatus.PROCESSED.name()));
  }

  @Override
  public void markFailed(EventPointer pointer, String failureMessage) {
    String sql =
        """
        update ec_event_inbox
        set status = :status,
            failure_message = :failureMessage,
            failed_at = now(),
            updated_at = now()
        where flow_name = :flowName and event_id = :eventId
        """;
    jdbcTemplate.update(
        sql,
        pointerParameters(pointer)
            .addValue("status", EventStatus.FAILED.name())
            .addValue("failureMessage", failureMessage));
  }

  @Override
  public void markExpired(EventPointer pointer) {
    String sql =
        """
        update ec_event_inbox
        set status = :status,
            updated_at = now()
        where flow_name = :flowName and event_id = :eventId
        """;
    jdbcTemplate.update(
        sql, pointerParameters(pointer).addValue("status", EventStatus.EXPIRED.name()));
  }

  @Override
  public boolean existsProcessed(String flowName, String eventType, String correlationKey) {
    String sql =
        """
        select exists (
          select 1
          from ec_event_inbox
          where flow_name = :flowName
            and event_type = :eventType
            and correlation_key = :correlationKey
            and status = :status
        )
        """;
    Boolean result =
        jdbcTemplate.queryForObject(
            sql,
            new MapSqlParameterSource()
                .addValue("flowName", flowName)
                .addValue("eventType", eventType)
                .addValue("correlationKey", correlationKey)
                .addValue("status", EventStatus.PROCESSED.name()),
            Boolean.class);
    return Boolean.TRUE.equals(result);
  }

  @Override
  public List<BufferedEvent> findPending(String flowName, String correlationKey) {
    String sql =
        """
        select flow_name,
               event_id,
               event_type,
               correlation_key,
               payload_json::text as payload_json,
               headers_json::text as headers_json,
               occurred_at,
               received_at,
               status
        from ec_event_inbox
        where flow_name = :flowName
          and correlation_key = :correlationKey
          and status = :status
        order by received_at, created_at
        """;
    return jdbcTemplate.query(
        sql,
        new MapSqlParameterSource()
            .addValue("flowName", flowName)
            .addValue("correlationKey", correlationKey)
            .addValue("status", EventStatus.PENDING.name()),
        rowMapper());
  }

  @Override
  public int expirePendingBefore(Instant now, int limit) {
    String sql =
        """
        with expired as (
          select flow_name, event_id
          from ec_event_inbox
          where status = :pendingStatus
            and expires_at is not null
            and expires_at <= :now
          order by expires_at, received_at, created_at
          limit :limit
          for update skip locked
        )
        update ec_event_inbox event
        set status = :expiredStatus,
            updated_at = now()
        from expired
        where event.flow_name = expired.flow_name
          and event.event_id = expired.event_id
        """;
    return jdbcTemplate.update(
        sql,
        new MapSqlParameterSource()
            .addValue("pendingStatus", EventStatus.PENDING.name())
            .addValue("expiredStatus", EventStatus.EXPIRED.name())
            .addValue("now", timestamp(now))
            .addValue("limit", limit));
  }

  private RowMapper<BufferedEvent> rowMapper() {
    return (resultSet, rowNumber) -> mapEvent(resultSet);
  }

  private BufferedEvent mapEvent(ResultSet resultSet) throws SQLException {
    String flowName = resultSet.getString("flow_name");
    String eventType = resultSet.getString("event_type");
    EventFlowDefinition flow = definitionRegistry.requireFlow(flowName);
    EventTypeDefinition<?> definition = flow.requireEvent(eventType);
    Object payload = readJson(resultSet.getString("payload_json"), definition.payloadClass());
    Map<String, Object> headers = readJson(resultSet.getString("headers_json"), HEADERS_TYPE);
    return new BufferedEvent(
        flowName,
        eventType,
        resultSet.getString("event_id"),
        resultSet.getString("correlation_key"),
        payload,
        headers,
        instant(resultSet.getTimestamp("occurred_at")),
        instant(resultSet.getTimestamp("received_at")),
        EventStatus.valueOf(resultSet.getString("status")));
  }

  private MapSqlParameterSource pointerParameters(EventPointer pointer) {
    return new MapSqlParameterSource()
        .addValue("flowName", pointer.flowName())
        .addValue("eventId", pointer.eventId());
  }

  private String writeJson(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new EventCorrelatorJsonException("Cannot serialize event payload", e);
    }
  }

  private <T> T readJson(String json, Class<T> targetClass) {
    try {
      return objectMapper.readValue(json, targetClass);
    } catch (JsonProcessingException e) {
      throw new EventCorrelatorJsonException("Cannot deserialize event payload", e);
    }
  }

  private <T> T readJson(String json, TypeReference<T> typeReference) {
    try {
      return objectMapper.readValue(json, typeReference);
    } catch (JsonProcessingException e) {
      throw new EventCorrelatorJsonException("Cannot deserialize event headers", e);
    }
  }

  private Timestamp timestamp(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  private Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
