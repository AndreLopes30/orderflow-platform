#!/usr/bin/env bash
set -euo pipefail

readonly ORDER_BASE_URL="${ORDER_BASE_URL:-http://localhost:8080}"
readonly NOTIFICATION_BASE_URL="${NOTIFICATION_BASE_URL:-http://localhost:8081}"
readonly PROMETHEUS_BASE_URL="${PROMETHEUS_BASE_URL:-http://localhost:9090}"
readonly GRAFANA_BASE_URL="${GRAFANA_BASE_URL:-http://localhost:3000}"
readonly OTEL_HEALTH_URL="${OTEL_HEALTH_URL:-http://localhost:13133}"
readonly TEMPO_BASE_URL="${TEMPO_BASE_URL:-http://localhost:3200}"
readonly SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-120}"
readonly ORDER_DB_NAME="${ORDER_DB_NAME:-orders}"
readonly ORDER_DB_USER="${ORDER_DB_USER:-orderflow}"
readonly NOTIFICATION_DB_NAME="${NOTIFICATION_DB_NAME:-notifications}"
readonly NOTIFICATION_DB_USER="${NOTIFICATION_DB_USER:-orderflow}"

log() {
  printf '[smoke] %s\n' "$*" >&2
}

fail() {
  log "ERROR: $*"
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

wait_until() {
  local description="$1"
  local timeout_seconds="$2"
  shift 2
  local started_at=$SECONDS

  until "$@"; do
    if (( SECONDS - started_at >= timeout_seconds )); then
      fail "Timed out after ${timeout_seconds}s waiting for ${description}"
    fi
    sleep 1
  done
  log "Validated: ${description}"
}

http_ok() {
  curl --fail --silent --show-error --max-time 5 "$1" >/dev/null
}

http_json_matches() {
  local url="$1"
  local filter="$2"
  local response
  response="$(curl --fail --silent --show-error --max-time 5 "$url")" || return 1
  jq --exit-status "$filter" <<<"$response" >/dev/null
}

http_contains() {
  local url="$1"
  local expected="$2"
  local response
  response="$(curl --fail --silent --show-error --max-time 5 "$url")" || return 1
  grep --fixed-strings --quiet "$expected" <<<"$response"
}

order_sql() {
  docker compose exec -T order-postgres \
    psql -X --set ON_ERROR_STOP=1 -U "$ORDER_DB_USER" -d "$ORDER_DB_NAME" -Atc "$1"
}

notification_sql() {
  docker compose exec -T notification-postgres \
    psql -X --set ON_ERROR_STOP=1 -U "$NOTIFICATION_DB_USER" -d "$NOTIFICATION_DB_NAME" -Atc "$1"
}

order_sql_equals() {
  [[ "$(order_sql "$1")" == "$2" ]]
}

notification_sql_equals() {
  [[ "$(notification_sql "$1")" == "$2" ]]
}

redis_equals() {
  [[ "$(docker compose exec -T redis redis-cli --raw "$1" "$2")" == "$3" ]]
}

redis_is_ready() {
  [[ "$(docker compose exec -T redis redis-cli --raw PING)" == 'PONG' ]]
}

redis_ttl_is_positive() {
  local ttl
  ttl="$(docker compose exec -T redis redis-cli --raw TTL "$1")"
  [[ "$ttl" =~ ^[0-9]+$ ]] && (( ttl > 0 ))
}

random_uuid() {
  tr '[:upper:]' '[:lower:]' </proc/sys/kernel/random/uuid
}

post_order() {
  local customer_id="$1"
  local total="$2"
  local body_file
  local status
  body_file="$(mktemp)"
  status="$(curl --silent --show-error --max-time 10 \
    --output "$body_file" --write-out '%{http_code}' \
    --request POST "$ORDER_BASE_URL/orders" \
    --header 'Content-Type: application/json' \
    --data "{\"customerId\":\"${customer_id}\",\"total\":${total}}")"
  if [[ "$status" != "201" ]]; then
    cat "$body_file" >&2
    rm -f "$body_file"
    fail "POST /orders returned HTTP ${status}, expected 201"
  fi
  cat "$body_file"
  rm -f "$body_file"
}

notification_matches() {
  local order_id="$1"
  local customer_id="$2"
  local response
  response="$(curl --fail --silent --show-error --max-time 5 \
    "$NOTIFICATION_BASE_URL/notifications/orders/$order_id")" || return 1
  jq --exit-status \
    --arg order_id "$order_id" \
    --arg customer_id "$customer_id" \
    '.orderId == $order_id and .customerId == $customer_id and .status == "SENT" and .channel == "SIMULATED"' \
    <<<"$response" >/dev/null
}

kafka_topic_contains() {
  local topic="$1"
  local expected="$2"
  local records
  records="$(docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:19092 \
    --topic "$topic" \
    --from-beginning \
    --timeout-ms 3000 2>/dev/null || true)"
  grep --fixed-strings --quiet "$expected" <<<"$records"
}

publish_kafka_twice() {
  local key="$1"
  local payload="$2"
  printf '%s|%s\n%s|%s\n' "$key" "$payload" "$key" "$payload" |
    docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:19092 \
      --topic order.created \
      --producer-property acks=all \
      --property parse.key=true \
      --property key.separator='|' >/dev/null
}

metric_value() {
  local metric="$1"
  curl --fail --silent --show-error --max-time 5 \
    "$NOTIFICATION_BASE_URL/actuator/prometheus" |
    awk -v metric="$metric" '$1 ~ ("^" metric "(\\{|$)") { print $NF; found=1 } END { if (!found) exit 1 }'
}

metric_at_least() {
  local metric="$1"
  local minimum="$2"
  local value
  value="$(metric_value "$metric")" || return 1
  awk -v value="$value" -v minimum="$minimum" 'BEGIN { exit !(value >= minimum) }'
}

prometheus_targets_are_up() {
  local response
  response="$(curl --fail --silent --show-error --max-time 5 --get \
    --data-urlencode 'query=up{job=~"order-service|notification-service|otel-collector"}' \
    "$PROMETHEUS_BASE_URL/api/v1/query")" || return 1
  jq --exit-status \
    '.status == "success" and ([.data.result[] | select(.value[1] == "1")] | length) == 3' \
    <<<"$response" >/dev/null
}

for dependency in curl jq docker awk grep tr; do
  require_command "$dependency"
done
docker compose version >/dev/null

log 'Waiting for application and observability readiness'
wait_until 'order-service readiness' "$SMOKE_TIMEOUT_SECONDS" \
  http_json_matches "$ORDER_BASE_URL/actuator/health/readiness" '.status == "UP"'
wait_until 'notification-service readiness' "$SMOKE_TIMEOUT_SECONDS" \
  http_json_matches "$NOTIFICATION_BASE_URL/actuator/health/readiness" '.status == "UP"'
wait_until 'order-service aggregate health' 30 \
  http_json_matches "$ORDER_BASE_URL/actuator/health" '.status == "UP"'
wait_until 'notification-service aggregate health' 30 \
  http_json_matches "$NOTIFICATION_BASE_URL/actuator/health" '.status == "UP"'
wait_until 'order-service Prometheus endpoint' 30 \
  http_contains "$ORDER_BASE_URL/actuator/prometheus" 'http_server_requests_seconds_count'
wait_until 'notification-service Prometheus endpoint' 30 \
  http_contains "$NOTIFICATION_BASE_URL/actuator/prometheus" 'orderflow_notifications_attempts_total'
wait_until 'Prometheus readiness' 60 http_ok "$PROMETHEUS_BASE_URL/-/ready"
wait_until 'Prometheus scrape targets' 60 prometheus_targets_are_up
wait_until 'Grafana health' 60 \
  http_json_matches "$GRAFANA_BASE_URL/api/health" '.database == "ok"'
wait_until 'OpenTelemetry Collector health' 60 http_ok "$OTEL_HEALTH_URL"
wait_until 'Tempo readiness' 60 http_ok "$TEMPO_BASE_URL/ready"

log 'Validating Flyway migrations and infrastructure'
wait_until 'order-service Flyway migration' 30 \
  order_sql_equals \
  'SELECT CASE WHEN count(*) > 0 AND bool_and(success) THEN 1 ELSE 0 END FROM flyway_schema_history' '1'
wait_until 'notification-service Flyway migration' 30 \
  notification_sql_equals \
  'SELECT CASE WHEN count(*) > 0 AND bool_and(success) THEN 1 ELSE 0 END FROM flyway_schema_history' '1'
redis_is_ready || fail 'Redis did not answer PONG'

topics="$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list)"
for topic in order.created order.created.retry-0 order.created.retry-1 order.created.dlt; do
  grep --fixed-strings --line-regexp --quiet "$topic" <<<"$topics" || fail "Kafka topic missing: $topic"
done
main_topic_description="$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic order.created)"
grep --fixed-strings --quiet 'PartitionCount: 3' <<<"$main_topic_description" ||
  fail 'order.created does not have three partitions'

log 'Running healthy order flow, Outbox, Kafka, cache and notification checks'
order_json="$(post_order 'customer-ci' '150.50')"
order_id="$(jq --exit-status --raw-output '.id' <<<"$order_json")"
jq --exit-status \
  --arg order_id "$order_id" \
  '.id == $order_id and .customerId == "customer-ci" and .total == 150.50 and .status == "CREATED"' \
  <<<"$order_json" >/dev/null || fail 'POST /orders response has unexpected fields'

wait_until 'Order and Outbox committed together' 30 order_sql_equals \
  "SELECT count(*) FROM orders o JOIN outbox_events e ON e.aggregate_id=o.id WHERE o.id='${order_id}' AND e.event_type='OrderCreatedEvent'" '1'
order_row="$(order_sql \
  "SELECT customer_id || '|' || total::text || '|' || status FROM orders WHERE id='${order_id}'")"
[[ "$order_row" == 'customer-ci|150.50|CREATED' ]] || fail "Unexpected persisted order: $order_row"
wait_until 'Outbox publication acknowledgement' 45 order_sql_equals \
  "SELECT count(*) FROM outbox_events WHERE aggregate_id='${order_id}' AND published_at IS NOT NULL AND attempts=0" '1'
wait_until 'OrderCreatedEvent present in Kafka' 45 kafka_topic_contains order.created "$order_id"
wait_until 'notification-service processed the order' 45 notification_matches "$order_id" 'customer-ci'

first_get="$(curl --fail --silent --show-error --max-time 10 "$ORDER_BASE_URL/orders/$order_id")"
jq --exit-status --arg order_id "$order_id" \
  '.id == $order_id and .customerId == "customer-ci" and .total == 150.50 and .status == "CREATED"' \
  <<<"$first_get" >/dev/null || fail 'First GET returned unexpected order data'
wait_until 'Redis cache population' 20 redis_equals EXISTS "orders::${order_id}" '1'
redis_ttl_is_positive "orders::${order_id}" || fail 'Cached order has no positive TTL'
second_get="$(curl --fail --silent --show-error --max-time 10 "$ORDER_BASE_URL/orders/$order_id")"
jq --exit-status --arg order_id "$order_id" '.id == $order_id and .status == "CREATED"' \
  <<<"$second_get" >/dev/null || fail 'Cached GET returned unexpected order data'

patch_file="$(mktemp)"
patch_status="$(curl --silent --show-error --max-time 10 \
  --output "$patch_file" --write-out '%{http_code}' \
  --request PATCH "$ORDER_BASE_URL/orders/$order_id/status" \
  --header 'Content-Type: application/json' \
  --data '{"status":"PROCESSING"}')"
[[ "$patch_status" == '200' ]] || {
  cat "$patch_file" >&2
  rm -f "$patch_file"
  fail "PATCH /orders/{id}/status returned HTTP ${patch_status}"
}
jq --exit-status '.status == "PROCESSING"' "$patch_file" >/dev/null || fail 'PATCH response is not PROCESSING'
rm -f "$patch_file"
wait_until 'Redis cache eviction after status update' 20 redis_equals EXISTS "orders::${order_id}" '0'
updated_get="$(curl --fail --silent --show-error --max-time 10 "$ORDER_BASE_URL/orders/$order_id")"
jq --exit-status --arg order_id "$order_id" '.id == $order_id and .status == "PROCESSING"' \
  <<<"$updated_get" >/dev/null || fail 'GET after cache eviction returned stale data'
wait_until 'Redis cache repopulation with updated order' 20 redis_equals EXISTS "orders::${order_id}" '1'

log 'Proving idempotency with a duplicate Kafka event'
duplicate_event_id="$(random_uuid)"
duplicate_order_id="$(random_uuid)"
duplicate_payload="$(jq --compact-output --null-input \
  --arg event_id "$duplicate_event_id" \
  --arg occurred_at "$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')" \
  --arg order_id "$duplicate_order_id" \
  '{eventId:$event_id,eventVersion:1,occurredAt:$occurred_at,orderId:$order_id,customerId:"customer-idempotency-ci",total:150.50}')"
publish_kafka_twice "$duplicate_order_id" "$duplicate_payload"
wait_until 'duplicate event claimed exactly once' 45 notification_sql_equals \
  "SELECT count(*) FROM processed_events WHERE event_id='${duplicate_event_id}'" '1'
wait_until 'duplicate event produced one notification' 45 notification_sql_equals \
  "SELECT count(*) FROM notification_records WHERE event_id='${duplicate_event_id}'" '1'
wait_until 'duplicate metric increment' 30 metric_at_least orderflow_notifications_duplicates_total 1

log 'Proving retry, DLT persistence and consumer recovery'
failures_before="$(metric_value orderflow_notifications_simulated_failures_total || printf '0')"
failed_order_json="$(post_order "fail-dlt-ci-$(random_uuid)" '99.90')"
failed_order_id="$(jq --exit-status --raw-output '.id' <<<"$failed_order_json")"
wait_until 'failed order Outbox publication' 45 order_sql_equals \
  "SELECT count(*) FROM outbox_events WHERE aggregate_id='${failed_order_id}' AND published_at IS NOT NULL" '1'
failed_event_id="$(order_sql "SELECT id FROM outbox_events WHERE aggregate_id='${failed_order_id}'")"
[[ -n "$failed_event_id" ]] || fail 'Could not locate failed eventId in Outbox'
wait_until 'failed event persisted from DLT' 60 notification_sql_equals \
  "SELECT count(*) FROM dead_letter_events WHERE event_id='${failed_event_id}' AND order_id='${failed_order_id}'" '1'
wait_until 'failed event present in order.created.dlt' 45 kafka_topic_contains order.created.dlt "$failed_order_id"
notification_sql_equals \
  "SELECT count(*) FROM processed_events WHERE event_id='${failed_event_id}'" '0' ||
  fail 'Failed event unexpectedly remained claimed'
notification_sql_equals \
  "SELECT count(*) FROM notification_records WHERE event_id='${failed_event_id}'" '0' ||
  fail 'Failed event unexpectedly produced a notification'
failures_after="$(metric_value orderflow_notifications_simulated_failures_total)"
awk -v before="$failures_before" -v after="$failures_after" \
  'BEGIN { exit !((after - before) >= 3) }' ||
  fail "Expected at least three controlled failures, observed before=${failures_before} after=${failures_after}"
wait_until 'notification-service remains ready after DLT' 30 \
  http_json_matches "$NOTIFICATION_BASE_URL/actuator/health/readiness" '.status == "UP"'

recovery_customer="customer-after-dlt-ci"
recovery_order_json="$(post_order "$recovery_customer" '25.00')"
recovery_order_id="$(jq --exit-status --raw-output '.id' <<<"$recovery_order_json")"
wait_until 'consumer processes a healthy event after DLT' 45 \
  notification_matches "$recovery_order_id" "$recovery_customer"

log "SUCCESS: distributed validation completed for order ${order_id}"
