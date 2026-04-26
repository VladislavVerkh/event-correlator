package dev.verkhovskiy.eventcorrelator.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.eventcorrelator.EventInboxQuery;
import dev.verkhovskiy.eventcorrelator.EventInboxRecord;
import dev.verkhovskiy.eventcorrelator.EventStatus;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

@ExtendWith(MockitoExtension.class)
class PostgresEventInboxInspectorTest {

  @Mock private NamedParameterJdbcTemplate jdbcTemplate;

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void mapsInboxRecordWhenFindingEvent() {
    when(jdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
        .thenAnswer(
            invocation -> {
              RowMapper<EventInboxRecord> mapper = invocation.getArgument(2);
              return List.of(mapper.mapRow(resultSet(), 0));
            });
    PostgresEventInboxInspector inspector = new PostgresEventInboxInspector(jdbcTemplate);

    Optional<EventInboxRecord> event = inspector.findEvent("contract-events", "event-1");

    assertThat(event).isPresent();
    assertThat(event.orElseThrow())
        .extracting(
            EventInboxRecord::flowName,
            EventInboxRecord::eventId,
            EventInboxRecord::eventType,
            EventInboxRecord::correlationKey,
            EventInboxRecord::status,
            EventInboxRecord::pendingReason,
            EventInboxRecord::failureMessage,
            EventInboxRecord::attempts,
            EventInboxRecord::receivedAt)
        .containsExactly(
            "contract-events",
            "event-1",
            "contract.created",
            "contract-1",
            EventStatus.FAILED,
            "waiting for contract.created",
            "temporary failure",
            2,
            Instant.parse("2026-01-01T00:00:01Z"));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void appliesQueryFiltersWhenFindingEvents() {
    when(jdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
        .thenReturn(List.of());
    PostgresEventInboxInspector inspector = new PostgresEventInboxInspector(jdbcTemplate);

    inspector.findEvents(
        EventInboxQuery.builder()
            .flowName("contract-events")
            .eventType("contract.created")
            .correlationKey("contract-1")
            .status(EventStatus.FAILED)
            .limit(25)
            .build());

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<SqlParameterSource> paramsCaptor =
        ArgumentCaptor.forClass(SqlParameterSource.class);
    verify(jdbcTemplate).query(sqlCaptor.capture(), paramsCaptor.capture(), any(RowMapper.class));

    assertThat(sqlCaptor.getValue())
        .contains("and flow_name = :flowName")
        .contains("and event_type = :eventType")
        .contains("and correlation_key = :correlationKey")
        .contains("and status = :status")
        .contains("order by received_at desc, created_at desc")
        .contains("limit :limit");

    MapSqlParameterSource params = (MapSqlParameterSource) paramsCaptor.getValue();
    assertThat(params.getValue("flowName")).isEqualTo("contract-events");
    assertThat(params.getValue("eventType")).isEqualTo("contract.created");
    assertThat(params.getValue("correlationKey")).isEqualTo("contract-1");
    assertThat(params.getValue("status")).isEqualTo("FAILED");
    assertThat(params.getValue("limit")).isEqualTo(25);
  }

  private ResultSet resultSet() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString("flow_name")).thenReturn("contract-events");
    when(resultSet.getString("event_id")).thenReturn("event-1");
    when(resultSet.getString("event_type")).thenReturn("contract.created");
    when(resultSet.getString("correlation_key")).thenReturn("contract-1");
    when(resultSet.getString("status")).thenReturn("FAILED");
    when(resultSet.getString("pending_reason")).thenReturn("waiting for contract.created");
    when(resultSet.getString("failure_message")).thenReturn("temporary failure");
    when(resultSet.getInt("attempts")).thenReturn(2);
    when(resultSet.getTimestamp("occurred_at"))
        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T00:00:00Z")));
    when(resultSet.getTimestamp("received_at"))
        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T00:00:01Z")));
    when(resultSet.getTimestamp("processed_at")).thenReturn(null);
    when(resultSet.getTimestamp("failed_at"))
        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T00:00:02Z")));
    when(resultSet.getTimestamp("expires_at")).thenReturn(null);
    when(resultSet.getTimestamp("next_retry_at"))
        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T00:01:02Z")));
    when(resultSet.getTimestamp("created_at"))
        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T00:00:01Z")));
    when(resultSet.getTimestamp("updated_at"))
        .thenReturn(Timestamp.from(Instant.parse("2026-01-01T00:00:02Z")));
    return resultSet;
  }
}
