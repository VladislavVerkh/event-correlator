package dev.verkhovskiy.eventcorrelator.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.eventcorrelator.EventDefinitionRegistry;
import dev.verkhovskiy.eventcorrelator.EventFlowDefinition;
import dev.verkhovskiy.eventcorrelator.EventPointer;
import java.util.List;
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
class PostgresEventBufferRepositoryTest {

  @Mock private NamedParameterJdbcTemplate jdbcTemplate;

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void claimsFailedEventForManualRetry() {
    when(jdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
        .thenReturn(List.of());
    PostgresEventBufferRepository repository =
        new PostgresEventBufferRepository(
            jdbcTemplate, new ObjectMapper(), new EventDefinitionRegistry(List.of(flow())));

    repository.claimFailedForRetry(new EventPointer("contract-events", "event-1"));

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<SqlParameterSource> paramsCaptor =
        ArgumentCaptor.forClass(SqlParameterSource.class);
    verify(jdbcTemplate).query(sqlCaptor.capture(), paramsCaptor.capture(), any(RowMapper.class));

    assertThat(sqlCaptor.getValue())
        .contains("and status = :failedStatus")
        .contains("for update skip locked")
        .contains("pending_reason = 'manual retry requested'")
        .contains("next_retry_at = null");

    MapSqlParameterSource params = (MapSqlParameterSource) paramsCaptor.getValue();
    assertThat(params.getValue("flowName")).isEqualTo("contract-events");
    assertThat(params.getValue("eventId")).isEqualTo("event-1");
    assertThat(params.getValue("failedStatus")).isEqualTo("FAILED");
    assertThat(params.getValue("pendingStatus")).isEqualTo("PENDING");
  }

  private static EventFlowDefinition flow() {
    return EventFlowDefinition.builder("contract-events")
        .rootEvent(
            "contract.created", ContractCreated.class, ContractCreated::contractId, payload -> {})
        .build();
  }

  private record ContractCreated(String contractId) {}
}
