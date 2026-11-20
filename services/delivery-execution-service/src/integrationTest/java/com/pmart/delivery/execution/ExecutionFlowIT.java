package com.pmart.delivery.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.pmart.common.test.EventEnvelopeFixtures;
import com.pmart.common.test.PostgresKafkaContainerSupport;
import com.pmart.common.test.TestJwt;
import com.pmart.common.test.TestJwtConfiguration;
import com.pmart.delivery.execution.application.EarningService;
import com.pmart.delivery.execution.application.EvidencePhotoStorage;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestClient;
import org.wiremock.spring.ConfigureWireMock;
import org.wiremock.spring.EnableWireMock;
import org.wiremock.spring.InjectWireMock;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 배송 수행 0.2.0(PC 1 검토 H2): 배정 사건 → 가게 도착 → 인계 사실(PackageHandedOver)로만 픽업 완료(포장 목록을 모르면 보류, DE-1) →
 * 고객 도착 → 증빙 사진 → 전달 완료(DeliveryCompleted, 인계 줄 합계). 배정 해제·재배정·픽업 전 취소. 실제 PostgreSQL·Kafka, 사진
 * 저장소만 메모리(S3 어댑터는 customer-service 와 같은 방식).
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.main.banner-mode=off",
      "spring.security.oauth2.resourceserver.jwt.jwk-set-uri=http://localhost:1/jwks",
      "pmart.messaging.outbox.delete-after-insert=false",
      "pmart.execution.confirm-interval=PT1H", // 확정 판단은 시험이 직접 부른다
      "pmart.execution.photo-sweep-interval=PT1H", // 사진 정리도 시험이 직접 부른다
      "pmart.execution.seq-backfill-interval=PT1H" // 순번 채우기도 시험이 직접 부른다
    })
@Import({TestJwtConfiguration.class, ExecutionFlowIT.MemoryPhotos.class})
@EnableWireMock({
  @ConfigureWireMock(name = "order", baseUrlProperties = "pmart.execution.urls.order"),
  @ConfigureWireMock(name = "dispatch", baseUrlProperties = "pmart.execution.urls.dispatch"),
  @ConfigureWireMock(name = "audit", baseUrlProperties = "pmart.execution.urls.audit"),
  @ConfigureWireMock(name = "rider", baseUrlProperties = "pmart.execution.urls.rider")
})
class ExecutionFlowIT extends PostgresKafkaContainerSupport {

  static final Map<String, byte[]> STORED = new ConcurrentHashMap<>();

  @TestConfiguration
  static class MemoryPhotos {
    @Bean
    EvidencePhotoStorage evidencePhotoStorage() {
      return new EvidencePhotoStorage() {
        @Override
        public void put(String key, byte[] bytes, String type) {
          STORED.put(key, bytes);
        }

        @Override
        public java.util.Optional<byte[]> get(String key) {
          return java.util.Optional.ofNullable(STORED.get(key));
        }

        @Override
        public boolean delete(String key) {
          STORED.remove(key);
          return true;
        }
      };
    }
  }

  static final byte[] JPEG = {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0, 0, 16, 'J', 'F'};

  @LocalServerPort int port;
  @Autowired ObjectMapper mapper;
  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired EarningService earnings;
  @Autowired com.pmart.delivery.execution.application.PhotoSweeper sweeper;
  @Autowired com.pmart.delivery.execution.application.AutoCompletion autoCompletion;

  @InjectWireMock("order")
  WireMockServer orderApi;

  @InjectWireMock("dispatch")
  WireMockServer dispatchApi;

  @InjectWireMock("audit")
  WireMockServer auditApi;

  @InjectWireMock("rider")
  WireMockServer riderApi;

  RestClient http;

  @BeforeEach
  void setUp() {
    http =
        RestClient.builder()
            .baseUrl("http://localhost:" + port + "/api/v1")
            .defaultStatusHandler(HttpStatusCode::isError, (q, r) -> {})
            .build();
  }

  /** BR-24(사용자 2026-11-14): 배정 라이더에게 픽업 뒤 ~ 전달 완료 전만, 고객 전화번호 그대로, 저장 안 함(no-store). */
  @Test
  void dropInfoOnlyForAssignedRiderBetweenPickupAndCompletion() {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    created(id, "SHP-" + UUID.randomUUID(), order);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    Map<String, Object> drop = new java.util.LinkedHashMap<>();
    drop.put("orderId", order);
    drop.put("roadAddress", "서울 강남구 테헤란로 1");
    drop.put("detailAddress", "101동 1001호");
    drop.put("receiverName", "김고객");
    drop.put("receiverPhone", "01012345678");
    drop.put("entranceMethod", "PASSWORD");
    drop.put("entranceNote", "#1234");
    drop.put("deliveryNote", "문 앞에 두세요");
    drop.put("geo", Map.of("lat", 37.5, "lng", 127.03));
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(mapper.writeValueAsString(drop))));

    // 픽업 전 409
    ResponseEntity<String> early = dropInfo(id, rider);
    assertThat(early.getStatusCode().value()).isEqualTo(409);
    assertThat(body(early).path("code").asString()).isEqualTo("DROP_INFO_CLOSED");

    jdbc.update("UPDATE execution_delivery SET state = 'PICKED_UP' WHERE delivery_id = ?", id);
    ResponseEntity<String> ok = dropInfo(id, rider);
    assertThat(ok.getStatusCode().value()).isEqualTo(200);
    assertThat(ok.getHeaders().getCacheControl()).contains("no-store");
    JsonNode d = body(ok);
    assertThat(d.path("receiverPhone").asString()).isEqualTo("01012345678"); // 안심번호 아님
    assertThat(d.path("detailAddress").asString()).isEqualTo("101동 1001호");
    assertThat(d.path("entranceMethod").asString()).isEqualTo("PASSWORD");
    assertThat(d.path("deliveryNote").asString()).isEqualTo("문 앞에 두세요");
    assertThat(d.path("lat").asDouble()).isEqualTo(37.5);
    // 다른 라이더는 있는지도 모른다(404), 운영자 토큰은 403
    assertThat(dropInfo(id, "RDR-OTHER").getStatusCode().value()).isEqualTo(404);
    assertThat(
            http.get()
                .uri("/deliveries/{id}/drop-info", id)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-1", List.of("OPERATOR_ADMIN"), List.of()))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);

    // 실패 등록(확정 전)은 아직 진행 중 — 열림, 전달 완료 뒤 닫힘
    jdbc.update(
        "UPDATE execution_delivery SET state = 'FAILURE_REPORTED' WHERE delivery_id = ?", id);
    assertThat(dropInfo(id, rider).getStatusCode().value()).isEqualTo(200);
    jdbc.update("UPDATE execution_delivery SET state = 'COMPLETED' WHERE delivery_id = ?", id);
    assertThat(dropInfo(id, rider).getStatusCode().value()).isEqualTo(409);

    // 주문 당시 사본 없는 주문 → 404 DROP_INFO_NOT_FOUND, order 장애 → 503
    jdbc.update("UPDATE execution_delivery SET state = 'PICKED_UP' WHERE delivery_id = ?", id);
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse().withStatus(404)));
    ResponseEntity<String> none = dropInfo(id, rider);
    assertThat(none.getStatusCode().value()).isEqualTo(404);
    assertThat(body(none).path("code").asString()).isEqualTo("DROP_INFO_NOT_FOUND");
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse().withStatus(503)));
    assertThat(dropInfo(id, rider).getStatusCode().value()).isEqualTo(503);
  }

  /**
   * 직원용 전달지(0.24.0, 2026-11-18 — 라이더 백오피스 관제, 사용자 "정확한 주소" + "사유 없이 + 열람 기록"): 라이더 운영·관리자만, 배정 ~ 전달
   * 전, 주소·좌표만(전화·출입 없음), audit 열람 기록 뒤에만, no-store.
   */
  @Test
  void staffSeesDropLocationOnlyWhileInProgressAfterAuditRecord() throws Exception {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    created(id, "SHP-" + UUID.randomUUID(), order);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    Map<String, Object> drop = new java.util.LinkedHashMap<>();
    drop.put("orderId", order);
    drop.put("roadAddress", "서울 강남구 테헤란로 1");
    drop.put("detailAddress", "101동 1001호");
    drop.put("receiverName", "김고객");
    drop.put("receiverPhone", "01012345678");
    drop.put("entranceMethod", "PASSWORD");
    drop.put("entranceNote", "#1234");
    drop.put("geo", Map.of("lat", 37.5, "lng", 127.03));
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    mapper.writeValueAsString(drop))));
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"orderId\":\"" + order + "\",\"accountId\":\"ACC-1\"}")));
    auditApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.post(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/pii-access-logs"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"piiAccessLogId\":\"PAL-1\",\"recordedAt\":\"" + Instant.now() + "\"}")));
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    ResponseEntity<String> ok = staffGet("/deliveries/" + id + "/drop-location", ops);
    assertThat(ok.getStatusCode().value()).as(ok.getBody()).isEqualTo(200);
    assertThat(ok.getHeaders().getCacheControl()).contains("no-store");
    JsonNode b = body(ok);
    assertThat(b.path("kind").asString()).isEqualTo("DELIVERY_DROP");
    assertThat(b.path("roadAddress").asString()).isEqualTo("서울 강남구 테헤란로 1");
    assertThat(b.path("detailAddress").asString()).isEqualTo("101동 1001호");
    assertThat(b.path("lat").asDouble()).isEqualTo(37.5);
    assertThat(b.path("piiAccessLogId").asString()).isEqualTo("PAL-1");
    assertThat(b.path("coordinateSource").asString()).isEqualTo("ORDER_PIN");
    // 고객이 핀 없이 저장한 주소(2026-11-18) → 배차가 지오코딩한 좌표(route-points drop)
    drop.remove("geo");
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    mapper.writeValueAsString(drop))));
    dispatchApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/deliveries/" + id + "/route-points"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"deliveryId\":\""
                        + id
                        + "\",\"pickup\":{\"lat\":35.15,\"lng\":129.06},\"drop\":{\"lat\":35.1612,\"lng\":129.0601}}")));
    JsonNode geo = body(staffGet("/deliveries/" + id + "/drop-location", ops));
    assertThat(geo.path("lat").asDouble()).isEqualTo(35.1612);
    assertThat(geo.path("coordinateSource").asString()).isEqualTo("ADDRESS_GEOCODE");
    assertThat(ok.getBody()).doesNotContain("01012345678", "김고객", "#1234");
    auditApi.verify(
        com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/pii-access-logs"))
            .withRequestBody(
                com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath(
                    "$.subjectRef",
                    com.github.tomakehurst.wiremock.client.WireMock.equalTo("ACC-1")))
            .withRequestBody(
                com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath(
                    "$.caseRef",
                    com.github.tomakehurst.wiremock.client.WireMock.equalTo("DELIVERY:" + id)))
            .withRequestBody(
                com.github.tomakehurst.wiremock.client.WireMock.notContaining("테헤란로")));
    // 상담원·센터 관리자는 403
    assertThat(
            staffGet(
                    "/deliveries/" + id + "/drop-location",
                    TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    // 기록이 안 되면 주소를 주지 않는다
    auditApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.post(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/pii-access-logs"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse().withStatus(503)));
    ResponseEntity<String> down = staffGet("/deliveries/" + id + "/drop-location", ops);
    assertThat(down.getStatusCode().value()).isEqualTo(503);
    assertThat(down.getBody()).doesNotContain("테헤란로");
    // 끝난 배송은 닫힘
    jdbc.update("UPDATE execution_delivery SET state = 'COMPLETED' WHERE delivery_id = ?", id);
    ResponseEntity<String> closed = staffGet("/deliveries/" + id + "/drop-location", ops);
    assertThat(closed.getStatusCode().value()).isEqualTo(409);
    assertThat(body(closed).path("code").asString()).isEqualTo("DROP_LOCATION_CLOSED");
  }

  /**
   * 직원용 접속·대기(2026-11-18 — 사용자 "접속 시간 따로 더함", 대기 = 운행 중 + 신규배차 켬 + 배송 0건): rider 운행 구간 [2시간 전 ~ 1시간
   * 전 신규배차 켬], [1시간 전 ~ 지금 끔] → 접속 약 2시간, 대기 1시간(배송 없음). rider 를 못 부르면 null. 라이더 본인 조회에는 없음.
   */
  @Test
  void staffOperatingTimeAddsConnectedAndWaiting() {
    String rider = "RDR-" + UUID.randomUUID();
    Instant now = Instant.now();
    riderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/riders/" + rider + "/duty-segments"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"riderId\":\""
                        + rider
                        + "\",\"items\":[{\"startedAt\":\""
                        + now.minusSeconds(7200)
                        + "\",\"endedAt\":\""
                        + now.minusSeconds(3600)
                        + "\",\"available\":true},{\"startedAt\":\""
                        + now.minusSeconds(3600)
                        + "\",\"endedAt\":null,\"available\":false}]}")));
    java.time.LocalDate today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Seoul"));
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    JsonNode t =
        body(
            staffGet(
                "/rider-operating-time?riderId="
                    + rider
                    + "&from="
                    + today.minusDays(1)
                    + "&to="
                    + today,
                ops));
    assertThat(t.path("totalSeconds").asLong()).isZero();
    assertThat(t.path("connectedSeconds").asLong()).isBetween(7195L, 7260L);
    assertThat(t.path("waitingSeconds").asLong()).isEqualTo(3600L);
    // 라이더 본인 조회에는 접속·대기가 없다
    JsonNode mine = body(meGet("/riders/me/operating-time", rider));
    assertThat(
            mine.path("connectedSeconds").isNull() || mine.path("connectedSeconds").isMissingNode())
        .isTrue();
    // rider 장애 → null(운행시간은 그대로)
    riderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/riders/" + rider + "/duty-segments"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse().withStatus(503)));
    JsonNode down = body(staffGet("/rider-operating-time?riderId=" + rider, ops));
    assertThat(
            down.path("connectedSeconds").isNull() || down.path("connectedSeconds").isMissingNode())
        .isTrue();
  }

  /** 순번 채우기(2026-11-18, ADR-0070): DeliveryCreated 에 순번이 없던 배송을 dispatch sequence 로 채운다. */
  @org.springframework.beans.factory.annotation.Autowired
  com.pmart.delivery.execution.application.SeqBackfill seqBackfill;

  @Test
  void backfillsMissingDeliverySeqFromDispatch() {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    created(id, "SHP-" + UUID.randomUUID(), order); // 순번 없는 옛 사건
    awaitState(id, "UNASSIGNED");
    dispatchApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/deliveries/" + id + "/sequence"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"deliveryId\":\"" + id + "\",\"deliverySeq\":2}")));
    seqBackfill.run();
    assertThat(
            jdbc.queryForObject(
                "SELECT delivery_seq FROM execution_delivery WHERE delivery_id = ?",
                Integer.class,
                id))
        .isEqualTo(2);
  }

  ResponseEntity<String> dropInfo(String id, String rider) {
    return http.get()
        .uri("/deliveries/{id}/drop-info", id)
        .header("Authorization", "Bearer " + TestJwt.rider(rider))
        .retrieve()
        .toEntity(String.class);
  }

  @Test
  void pickupOnlyAfterAllPackagesHandedOverThenCompleteWithPhoto() {
    String id = "DLV-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    created(id, shipment, order);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");

    assertThat(post(id, "store-arrivals", "RDR-OTHER", Map.of()).getStatusCode().value())
        .isEqualTo(403);
    ResponseEntity<String> arrived = post(id, "store-arrivals", rider, Map.of());
    assertThat(arrived.getStatusCode().value()).isEqualTo(200);
    assertThat(body(arrived).path("status").asString()).isEqualTo("APPLIED");
    assertThat(outbox(id, "RiderArrivedAtStore").path("payload").path("riderId").asString())
        .isEqualTo(rider);

    // 포장 목록을 모를 때 온 첫 인계 — 픽업 완료 아님(M4 와 같은 규칙)
    handedOver(id, shipment, order, rider, "PKG-1", "HO-1", Map.of("L-1", 2));
    awaitSql("SELECT count(*) FROM package_handover WHERE handover_id = 'HO-1'");
    ResponseEntity<String> early =
        post(id, "pickups", rider, Map.of("handoverIds", List.of("HO-1")));
    assertThat(early.getStatusCode().value()).isEqualTo(422);
    assertThat(body(early).path("code").asString()).isEqualTo("HANDOVER_NOT_CONFIRMED");

    // 목록이 오면(포장 2개) 아직 하나 남음 → 두 번째 인계에서 픽업 완료
    send(
        "mfc.fulfillment.v1",
        "ShipmentReady",
        "Shipment",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            "MFC-1",
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(Map.of("packageId", "PKG-1"), Map.of("packageId", "PKG-2"))));
    awaitSql("SELECT count(*) FROM shipment_package WHERE shipment_id = '" + shipment + "'");
    assertThat(state(id)).isEqualTo("ARRIVED_AT_STORE");
    // DE-4(2026-11-18): 배송 조회 packageIds = 출고 포장 ∪ 인계 기록 — 아직 안 넘긴 PKG-2 도 보여야 라이더 앱이 QR 을 받는다
    assertThat(view(id, rider).path("packageIds").valueStream().map(JsonNode::asString).toList())
        .containsExactlyInAnyOrder("PKG-1", "PKG-2");
    handedOver(id, shipment, order, rider, "PKG-2", "HO-2", Map.of("L-1", 1, "L-2", 3));
    awaitState(id, "PICKED_UP");
    JsonNode picked = outbox(id, "PickupCompleted").path("payload");
    assertThat(picked.path("packageIds").size()).isEqualTo(2);
    assertThat(picked.path("handoverIds").toString()).contains("HO-1", "HO-2");
    // 같은 포장을 다시 인계(재인계) — 완료 수량은 포장마다 마지막 인계만(두 번 세지 않는다, PC 1 재검토 ③)
    handedOver(id, shipment, order, rider, "PKG-2", "HO-2B", Map.of("L-1", 1, "L-2", 3));
    awaitSql("SELECT count(*) FROM package_handover WHERE handover_id = 'HO-2B'");
    assertThat(
            post(id, "pickups", rider, Map.of("handoverIds", List.of("HO-1", "HO-2")))
                .getStatusCode()
                .value())
        .isEqualTo(200);

    // 고객 도착 전 완료 → 409, 잘못된 사진 → 422, 사진 올리기 → 201
    String takenAt = Instant.now().toString();
    assertThat(
            post(id, "completions", rider, completion(List.of("EVP-NONE"), takenAt))
                .getStatusCode()
                .value())
        .isEqualTo(409);
    ResponseEntity<String> badPhoto = upload(id, rider, "x".getBytes());
    assertThat(badPhoto.getStatusCode().value()).isEqualTo(422);
    assertThat(body(badPhoto).path("code").asString()).isEqualTo("PHOTO_INVALID");
    ResponseEntity<String> photo = upload(id, rider, JPEG);
    assertThat(photo.getStatusCode().value()).isEqualTo(201);
    String photoRef = body(photo).path("photoRef").asString();
    assertThat(body(photo).path("contentType").asString()).isEqualTo("image/jpeg");
    assertThat(STORED).containsKey("deliveries/" + id + "/" + photoRef);
    assertThat(upload(id, "RDR-OTHER", JPEG).getStatusCode().value()).isEqualTo(403);
    // 저장소 올리기는 트랜잭션 밖(PC 1 검토 후속): 같은 키 재시도 → 같은 사진 참조·객체 하나
    String key = "photo-" + UUID.randomUUID();
    String again = body(upload(id, rider, JPEG, key)).path("photoRef").asString();
    assertThat(body(upload(id, rider, JPEG, key)).path("photoRef").asString()).isEqualTo(again);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM photo_upload WHERE delivery_id = ?", Integer.class, id))
        .isZero(); // 참조를 저장한 트랜잭션이 올리는 중 기록을 지웠다
    // 중복 요청 기록이 없어진 뒤 같은 요청을 다시 보내도 409 를 반복하지 않고 그 사진을 돌려준다(#12 후속)
    jdbc.update("DELETE FROM request_record WHERE request_id = ?", key);
    ResponseEntity<String> afterPurge = upload(id, rider, JPEG, key);
    assertThat(afterPurge.getStatusCode().is2xxSuccessful()).isTrue();
    assertThat(body(afterPurge).path("photoRef").asString()).isEqualTo(again);
    // 같은 키·다른 사진 → 409. 올린 객체는 바로 지우지 않고(동시 중복 요청이 같은 객체로 성공했을 수 있다) 5시간 뒤 참조가 없으면 지운다
    // (사용자 2026-11-14 "5시간이 지나면 삭제")
    byte[] other = java.util.Arrays.copyOf(JPEG, JPEG.length + 1);
    long before = STORED.keySet().stream().filter(k -> k.startsWith("deliveries/" + id)).count();
    assertThat(upload(id, rider, other, key).getStatusCode().value()).isEqualTo(409);
    assertThat(STORED.keySet().stream().filter(k -> k.startsWith("deliveries/" + id)).count())
        .isEqualTo(before + 1);
    sweeper.sweep(); // 아직 5시간 안 — 그대로
    assertThat(STORED.keySet().stream().filter(k -> k.startsWith("deliveries/" + id)).count())
        .isEqualTo(before + 1);
    jdbc.update(
        "UPDATE photo_upload SET started_at = started_at - interval '6 hours' WHERE delivery_id = ?",
        id);
    sweeper.sweep();
    assertThat(STORED.keySet().stream().filter(k -> k.startsWith("deliveries/" + id)).count())
        .isEqualTo(before);
    assertThat(STORED).containsKey("deliveries/" + id + "/" + again); // 참조 있는 사진은 남는다
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM photo_upload WHERE delivery_id = ?", Integer.class, id))
        .isZero();

    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    ResponseEntity<String> noPhoto =
        post(id, "completions", rider, completion(List.of("EVP-NONE"), takenAt));
    assertThat(noPhoto.getStatusCode().value()).isEqualTo(422);
    assertThat(body(noPhoto).path("code").asString()).isEqualTo("EVIDENCE_REQUIRED");
    ResponseEntity<String> done =
        post(id, "completions", rider, completion(List.of(photoRef), takenAt));
    assertThat(done.getStatusCode().value()).isEqualTo(200);
    assertThat(state(id)).isEqualTo("COMPLETED");

    // 고객 전달 증빙(2026-11-17 사용자 "본인 배송 완료 사진 공개"): 본인 주문만, 완료 사진만
    String me = "ACC-" + UUID.randomUUID();
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"orderId\":\"" + order + "\",\"accountId\":\"" + me + "\"}")));
    ResponseEntity<String> proof = customerGet("/deliveries/" + id + "/delivery-proof", me);
    assertThat(proof.getStatusCode().value()).isEqualTo(200);
    assertThat(body(proof).path("photoRefs").get(0).asString()).isEqualTo(photoRef);
    assertThat(body(proof).path("placement").isString() || body(proof).path("placement").isNull())
        .isTrue();
    assertThat(proof.getBody()).doesNotContain(rider);
    assertThat(
            customerGet("/deliveries/" + id + "/delivery-proof", "ACC-OTHER")
                .getStatusCode()
                .value())
        .isEqualTo(404);
    assertThat(
            http.get()
                .uri("/deliveries/{id}/delivery-proof", id)
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(photo(id, photoRef, TestJwt.customer(me)).getStatusCode().value()).isEqualTo(200);
    assertThat(photo(id, photoRef, TestJwt.customer("ACC-OTHER")).getStatusCode().value())
        .isEqualTo(404);

    JsonNode completed = outbox(id, "DeliveryCompleted").path("payload");
    assertThat(completed.path("orderId").asString()).isEqualTo(order);
    assertThat(completed.path("method").asString()).isEqualTo("CONTACTLESS");
    assertThat(completed.path("evidenceRef").path("kind").asString()).isEqualTo("PHOTO");
    Map<String, Integer> lines = new java.util.HashMap<>();
    for (JsonNode l : completed.path("lines")) {
      lines.put(l.path("orderLineId").asString(), l.path("qty").asInt());
    }
    assertThat(lines).containsExactlyInAnyOrderEntriesOf(Map.of("L-1", 3, "L-2", 3));

    JsonNode view =
        body(
            http.get()
                .uri("/deliveries/{id}", id)
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    assertThat(view.path("state").asString()).isEqualTo("COMPLETED");
    assertThat(view.path("evidence").path("photoRefs").get(0).asString()).isEqualTo(photoRef);
    assertThat(view.path("history").size()).isEqualTo(5); // 배정·가게·픽업·고객·완료
    assertThat(
            http.get()
                .uri("/deliveries/{id}", id)
                .header("Authorization", "Bearer " + TestJwt.rider("RDR-OTHER"))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(404);
    JsonNode mine =
        body(
            http.get()
                .uri("/riders/me/deliveries?state=COMPLETED")
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    assertThat(mine.path("items").size()).isEqualTo(1);

    // 수행료 — 수락 당시 금액(DeliveryAssigned.fee)으로 확정 대기, 5시간 뒤 확정(시험은 확정 시각을 당겨 직접 판단)
    Map<String, Object> earning =
        jdbc.queryForMap(
            "SELECT calculable, total, settlement_state, payout_date FROM rider_earning WHERE delivery_id = ?",
            id);
    assertThat(earning.get("calculable")).isEqualTo(true);
    assertThat(earning.get("total")).isEqualTo(3900);
    assertThat(earning.get("settlement_state")).isEqualTo("PENDING_CONFIRMATION");
    assertThat(earning.get("payout_date")).isNotNull();
    assertThat(outbox(id, "RiderEarningRecorded").path("payload").path("status").asString())
        .isEqualTo("PENDING_CONFIRMATION");
    jdbc.update(
        "UPDATE rider_earning SET confirm_at = ? WHERE delivery_id = ?",
        java.sql.Timestamp.from(Instant.now().minusSeconds(1)),
        id);
    earnings.confirmDue();
    JsonNode myEarnings =
        body(
            http.get()
                .uri("/riders/me/earnings")
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    assertThat(myEarnings.path("items").get(0).path("settlementState").asString())
        .isEqualTo("CONFIRMED");
    assertThat(myEarnings.path("items").get(0).path("total").asInt()).isEqualTo(3900);
    assertThat(myEarnings.path("totals").path("confirmed").asInt()).isEqualTo(3900);
    assertThat(view(id, rider).path("earning").path("settlementState").asString())
        .isEqualTo("CONFIRMED");
    // 배송 사건마다 aggregateVersion 이 다르다(수행료 사건 포함)
    assertThat(
            jdbc.queryForList(
                "SELECT (payload->>'aggregateVersion')::bigint FROM outbox_event WHERE aggregateid = ?",
                Long.class,
                id))
        .doesNotHaveDuplicates();
  }

  @Test
  void failureReportedStaysInProgressUntilOperatorDecides() {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();

    // 고객 사유인데 연락 1회 → 422, 남의(없는) 사진 → 422
    ResponseEntity<String> oneCall =
        post(id, "failures", rider, failure("CUSTOMER_ABSENT", List.of(photo), 1));
    assertThat(oneCall.getStatusCode().value()).isEqualTo(422);
    assertThat(body(oneCall).path("code").asString()).isEqualTo("CONTACT_ATTEMPTS_REQUIRED");
    ResponseEntity<String> noPhoto =
        post(id, "failures", rider, failure("CUSTOMER_ABSENT", List.of("EVP-NONE"), 2));
    assertThat(body(noPhoto).path("code").asString()).isEqualTo("EVIDENCE_REQUIRED");

    // 등록 → FAILURE_REPORTED(접수 — 아직 진행 중), 지원센터로 갈 사건
    ResponseEntity<String> reported =
        post(id, "failures", rider, failure("CUSTOMER_ABSENT", List.of(photo), 2));
    assertThat(reported.getStatusCode().value()).isEqualTo(200);
    assertThat(body(reported).path("status").asString()).isEqualTo("ACCEPTED");
    assertThat(state(id)).isEqualTo("FAILURE_REPORTED");
    JsonNode incident = outbox(id, "DeliveryIncidentRaised").path("payload");
    assertThat(incident.path("category").asString()).isEqualTo("DELIVERY_FAILURE");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryFailed'",
                Integer.class,
                id))
        .isZero();
    // 2026-11-19 사용자 "라이더 백오피스만": 같은 센터 관리자·상담원·작업자 403, 라이더 운영만 반려 → 원래 상태(고객 도착 전이면 PICKED_UP)로
    assertThat(
            decide(id, "OP-X", List.of("MFC_SUPERVISOR"), List.of("MFC-1"), "REJECT")
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(
            decide(id, "OP-CS", List.of("CS_AGENT"), List.of(), "REJECT").getStatusCode().value())
        .isEqualTo(403);
    assertThat(
            decide(id, "OP-W", List.of("MFC_WORKER"), List.of("MFC-1"), "REJECT")
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(decide(id, "OP-1", List.of("MFC-1"), "REJECT").getStatusCode().value())
        .isEqualTo(200);
    assertThat(state(id)).isEqualTo("PICKED_UP");

    // 다시 등록(고객 도착 뒤) → 확정 → FAILED + DeliveryFailed, 수행료 산정 불가
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    assertThat(
            post(id, "failures", rider, failure("UNREACHABLE", List.of(photo), 2))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(
            post(id, "completions", rider, completion(List.of(photo), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(409); // 확정 전에는 완료도 안 된다
    assertThat(decide(id, "OP-1", List.of("MFC-1"), "CONFIRM").getStatusCode().value())
        .isEqualTo(200);
    assertThat(state(id)).isEqualTo("FAILED");
    JsonNode failed = outbox(id, "DeliveryFailed").path("payload");
    assertThat(failed.path("reason").asString()).isEqualTo("UNREACHABLE");
    assertThat(failed.path("custody").asString()).isEqualTo("RIDER");
    // 고객 사유(연락 두절) 확정 → 수락 당시 금액(2026-11-17 사용자 "고객 귀책이면 원래 측정돼 있던 수행료")
    assertThat(
            jdbc.queryForObject(
                "SELECT calculable FROM rider_earning WHERE delivery_id = ?", Boolean.class, id))
        .isTrue();
    JsonNode v = view(id, rider);
    assertThat(v.path("failure").path("decision").asString()).isEqualTo("CONFIRM");
    assertThat(v.path("failure").path("contactAttempts").asInt()).isEqualTo(2);
    assertThat(decide(id, "OP-1", List.of("MFC-1"), "CONFIRM").getStatusCode().value())
        .isEqualTo(409); // 이미 끝남
  }

  @Test
  void helpRequestIsRecordedAndRaisedForSupportCenter() {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    assertThat(
            post(id, "help-requests", "RDR-OTHER", Map.of("category", "ACCIDENT"))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    ResponseEntity<String> created =
        post(
            id,
            "help-requests",
            rider,
            Map.of("category", "STORE_WAITING", "detail", "포장이 아직 안 나옴"));
    assertThat(created.getStatusCode().value()).isEqualTo(202);
    String requestId = body(created).path("resultId").asString();
    JsonNode raised = outbox(id, "DeliveryIncidentRaised").path("payload");
    assertThat(raised.path("type").asString()).isEqualTo("HELP_REQUEST");
    assertThat(raised.path("category").asString()).isEqualTo("STORE_WAITING");
    assertThat(raised.path("stage").asString()).isEqualTo("PICKED_UP");
    assertThat(raised.path("incidentId").asString())
        .isEqualTo(id + ":" + requestId); // 요청 ID(앱 Idempotency-Key)는 배송마다만 유일 — 배송 ID 를 붙인다

    JsonNode mine =
        body(
            http.get()
                .uri("/deliveries/{id}/help-requests/{r}", id, requestId)
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    assertThat(mine.path("state").asString()).isEqualTo("OPEN");
    assertThat(mine.path("caseId").isNull() || mine.path("caseId").isMissingNode()).isTrue();
    assertThat(
            http.get()
                .uri("/deliveries/{id}/help-requests/{r}", id, requestId)
                .header("Authorization", "Bearer " + TestJwt.rider("RDR-OTHER"))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(404);
    assertThat(
            http.get()
                .uri("/deliveries/{id}/help-requests/{r}", id, requestId)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(view(id, rider).path("incidents").get(0).path("category").asString())
        .isEqualTo("STORE_WAITING");
  }

  /** 0.4.0 백오피스 직원 목록(BOD-4·5): 배송 찾기·도움요청 대기 목록·처리함 닫기, RIDER_OPS 권한(BOD-1). */
  @Test
  void staffFindsDeliveriesAndHelpQueueAndResolvesHelpRequest() {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), order, rider);
    String requestId =
        body(post(
                id,
                "help-requests",
                rider,
                Map.of("category", "ADDRESS_ISSUE", "detail", "동 호수가 안 보임")))
            .path("resultId")
            .asString();
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    String worker = TestJwt.operator("OP-W", List.of("MFC_WORKER"), List.of("MFC-1"));

    // 배송 찾기 — 주문 ID 로(상담원), 라이더는 403, 센터 직원은 mfcId 를 줘야
    JsonNode found =
        body(
            staffGet(
                "/deliveries?orderId=" + order,
                TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of())));
    assertThat(found.path("items")).hasSize(1);
    assertThat(found.path("items").get(0).path("deliveryId").asString()).isEqualTo(id);
    assertThat(found.path("items").get(0).path("state").asString()).isEqualTo("PICKED_UP");
    assertThat(found.path("items").get(0).path("riderId").asString()).isEqualTo(rider);
    assertThat(
            staffGet("/deliveries?orderId=" + order, TestJwt.rider(rider)).getStatusCode().value())
        .isEqualTo(403);
    assertThat(staffGet("/deliveries?orderId=" + order, worker).getStatusCode().value())
        .isEqualTo(403);
    assertThat(body(staffGet("/deliveries?mfcId=MFC-1&orderId=" + order, worker)).path("items"))
        .hasSize(1);
    assertThat(staffGet("/deliveries?mfcId=MFC-2&orderId=" + order, worker).getStatusCode().value())
        .isEqualTo(403);
    assertThat(staffGet("/deliveries?sort=SIDEWAYS", ops).getStatusCode().value()).isEqualTo(400);

    // 도움요청 대기 목록 — 라이더 글(detail)은 목록에 없다
    JsonNode queue = body(staffGet("/delivery-help-requests?riderId=" + rider, ops));
    assertThat(queue.path("items")).hasSize(1);
    JsonNode row = queue.path("items").get(0);
    assertThat(row.path("incidentId").asString()).isEqualTo(id + ":" + requestId);
    assertThat(row.path("orderId").asString()).isEqualTo(order);
    assertThat(row.path("mfcId").asString()).isEqualTo("MFC-1");
    assertThat(row.path("state").asString()).isEqualTo("OPEN");
    assertThat(row.has("detail")).isFalse();
    assertThat(staffGet("/delivery-help-requests?riderId=" + rider, worker).getStatusCode().value())
        .isEqualTo(403);
    assertThat(staffGet("/delivery-help-requests?state=DONE", ops).getStatusCode().value())
        .isEqualTo(400);

    // 단건 — 라이더도 자기 글은 본다
    JsonNode mine =
        body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, TestJwt.rider(rider)));
    assertThat(mine.path("detail").asString()).isEqualTo("동 호수가 안 보임");

    // 닫기 — 같은 센터 작업자는 403, 라이더 운영은 닫는다, 같은 키 재전송은 같은 결과, 새 키는 409
    assertThat(resolve(id, requestId, worker, "K-" + UUID.randomUUID()).getStatusCode().value())
        .isEqualTo(403);
    String key = "K-" + UUID.randomUUID();
    ResponseEntity<String> closed = resolve(id, requestId, ops, key);
    assertThat(closed.getStatusCode().value()).isEqualTo(200);
    assertThat(body(closed).path("state").asString()).isEqualTo("RESOLVED");
    assertThat(body(closed).path("resolvedBy").asString()).isEqualTo("OP-RO");
    assertThat(body(closed).path("resolutionNote").asString()).isEqualTo("고객과 통화해 위치 안내");
    assertThat(resolve(id, requestId, ops, key).getStatusCode().value()).isEqualTo(200);
    assertThat(resolve(id, requestId, ops, "K-" + UUID.randomUUID()).getStatusCode().value())
        .isEqualTo(409);
    assertThat(body(staffGet("/delivery-help-requests?riderId=" + rider, ops)).path("items"))
        .isEmpty();
    assertThat(
            body(staffGet("/delivery-help-requests?state=RESOLVED&riderId=" + rider, ops))
                .path("items"))
        .hasSize(1);

    // 라이더에게는 닫힌 것만 — 누가·메모는 숨긴다
    JsonNode after =
        body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, TestJwt.rider(rider)));
    assertThat(after.path("state").asString()).isEqualTo("RESOLVED");
    assertThat(after.path("resolvedAt").isNull()).isFalse();
    assertThat(after.path("resolvedBy").isNull() || after.path("resolvedBy").isMissingNode())
        .isTrue();
    assertThat(
            after.path("resolutionNote").isNull() || after.path("resolutionNote").isMissingNode())
        .isTrue();

    // 라이더 운영은 배송 상세도 본다(BOD-1)
    assertThat(staffGet("/deliveries/" + id, ops).getStatusCode().value()).isEqualTo(200);
  }

  String handoverCode(String deliveryId, String token) {
    String c = handoverCodeOrNull(deliveryId, token);
    assertThat(c).as("반납 인증번호 " + deliveryId).isNotNull();
    return c;
  }

  @org.jspecify.annotations.Nullable
  String handoverCodeOrNull(String deliveryId, String token) {
    for (JsonNode h : body(staffGet("/return-handovers?mfcId=MFC-1", token)).path("items")) {
      if (deliveryId.equals(h.path("deliveryId").asString())) {
        return h.path("code").asString();
      }
    }
    return null;
  }

  /** 이의 인정 보상금(2026-11-19 사용자 "보상금 한 줄 따로"): 인정된 수행료 이의에만, 한 번, 지급 묶음에 함께. */
  @Test
  void compensationForAcceptedEarningAppeal() throws Exception {
    String rider = "RDR-cp-" + UUID.randomUUID();
    String delivery = "DLV-cp-" + UUID.randomUUID();
    String ok = "RAP-ok-" + UUID.randomUUID();
    String open = "RAP-open-" + UUID.randomUUID();
    for (String[] a : new String[][] {{ok, "ACCEPTED"}, {open, "OPEN"}}) {
      riderApi.stubFor(
          com.github.tomakehurst.wiremock.client.WireMock.get(
                  com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                      "/api/v1/internal/rider-appeals/" + a[0]))
              .willReturn(
                  com.github.tomakehurst.wiremock.client.WireMock.okJson(
                      mapper.writeValueAsString(
                          Map.of(
                              "appealId",
                              a[0],
                              "riderId",
                              rider,
                              "targetType",
                              "EARNING",
                              "targetRef",
                              delivery,
                              "state",
                              a[1])))));
    }
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    String cs = TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of());
    Map<String, Object> body = Map.of("appealId", ok, "amount", 1500, "reason", "엘리베이터 고장 확인");
    assertThat(staffPost("/rider-compensations", cs, body).getStatusCode().value()).isEqualTo(403);
    assertThat(
            staffPost(
                    "/rider-compensations",
                    ops,
                    Map.of("appealId", open, "amount", 1500, "reason", "x"))
                .getBody())
        .contains("APPEAL_NOT_ACCEPTED");
    ResponseEntity<String> made = staffPost("/rider-compensations", ops, body);
    assertThat(made.getStatusCode().value()).as(made.getBody()).isEqualTo(200);
    JsonNode c = body(made);
    assertThat(c.path("deliveryId").asString()).isEqualTo(delivery);
    assertThat(c.path("riderId").asString()).isEqualTo(rider);
    assertThat(c.path("settlementState").asString()).isEqualTo("CONFIRMED");
    assertThat(staffPost("/rider-compensations", ops, body).getBody())
        .contains("COMPENSATION_EXISTS");
    // 라이더는 내 보상금에서 보고(직원 ID 없음), 지급 묶음에 함께 들어간다
    JsonNode mine = body(meGet("/riders/me/compensations", rider));
    assertThat(mine.path("items").get(0).path("amount").asInt()).isEqualTo(1500);
    assertThat(mine.path("items").get(0).path("recordedBy").isNull()).isTrue();
    String date = c.path("payoutDate").asString();
    JsonNode batch =
        body(staffGet("/rider-payouts?riderId=" + rider + "&payoutDate=" + date, ops))
            .path("items")
            .get(0);
    assertThat(batch.path("total").asInt()).isEqualTo(1500);
    assertThat(batch.path("compensationCount").asInt()).isEqualTo(1);
    String adm = TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of());
    assertThat(
            body(staffGet("/delivery-staff-actions?riderId=" + rider, adm))
                .path("items")
                .get(0)
                .path("action")
                .asString())
        .isEqualTo("COMPENSATION_RECORDED");
  }

  /** 수행료 지급 결과(2026-11-18 사용자 "결과 받는 창구 + 수동 표시"): 묶음 단위, 관리자만, 실패 사유 필수, 이미 지급은 409. */
  @Test
  void payoutResultsMoveConfirmedEarningsOfOneBatch() {
    String rider = "RDR-po-" + UUID.randomUUID();
    String today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Seoul")).toString();
    List<String> ids = new java.util.ArrayList<>();
    for (int k = 0; k < 2; k++) {
      String id = "DLV-po-" + UUID.randomUUID();
      created(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID());
      awaitSql("SELECT count(*) FROM execution_delivery WHERE delivery_id = '" + id + "'");
      jdbc.update(
          "INSERT INTO rider_earning (earning_id, delivery_id, rider_id, calculable, rate_table_version,"
              + " base_fee, total, settlement_state, performed_on, payout_date, recorded_at, updated_at)"
              + " VALUES (?, ?, ?, true, 1, 3000, 3000, 'CONFIRMED', CAST(? AS date) - 3, CAST(? AS date),"
              + " now(), now())",
          "ERN-po-" + id,
          id,
          rider,
          today,
          today);
      ids.add(id);
    }
    String adm = TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of());
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    JsonNode list = body(staffGet("/rider-payouts?riderId=" + rider, ops));
    assertThat(list.path("items").get(0).path("count").asInt()).isEqualTo(2);
    assertThat(list.path("items").get(0).path("total").asInt()).isEqualTo(6000);
    assertThat(list.path("items").get(0).path("state").asString()).isEqualTo("CONFIRMED");
    String uri = "/rider-payouts/" + rider + "/" + today + "/results";
    // 라이더 운영은 표시 못 함, 실패는 사유 필수
    assertThat(staffPost(uri, ops, Map.of("result", "PAID")).getStatusCode().value())
        .isEqualTo(403);
    assertThat(staffPost(uri, adm, Map.of("result", "FAILED")).getStatusCode().value())
        .isEqualTo(400);
    ResponseEntity<String> failed =
        staffPost(uri, adm, Map.of("result", "FAILED", "failureReason", "계좌 확인 필요"));
    assertThat(failed.getStatusCode().value()).as(failed.getBody()).isEqualTo(200);
    assertThat(body(failed).path("state").asString()).isEqualTo("PAYMENT_FAILED");
    assertThat(outbox(ids.get(0), "RiderEarningRecorded").path("payload").toString())
        .doesNotContain("계좌");
    ResponseEntity<String> paid =
        staffPost(uri, adm, Map.of("result", "PAID", "reference", "ACC-77"));
    assertThat(body(paid).path("state").asString()).isEqualTo("PAID");
    assertThat(body(paid).path("lastResult").path("reference").asString()).isEqualTo("ACC-77");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM rider_earning WHERE rider_id = ? AND settlement_state = 'PAID'",
                Integer.class,
                rider))
        .isEqualTo(2);
    assertThat(staffPost(uri, adm, Map.of("result", "PAID")).getStatusCode().value())
        .isEqualTo(409);
    // 지급 예정일 전이면 422, 조치 이력에 남음
    String later = java.time.LocalDate.parse(today).plusDays(5).toString();
    jdbc.update(
        "UPDATE rider_earning SET settlement_state = 'CONFIRMED', payout_date = CAST(? AS date) WHERE"
            + " earning_id = ?",
        later,
        "ERN-po-" + ids.get(0));
    assertThat(
            staffPost(
                    "/rider-payouts/" + rider + "/" + later + "/results",
                    adm,
                    Map.of("result", "PAID"))
                .getBody())
        .contains("PAYOUT_NOT_DUE");
    JsonNode acts = body(staffGet("/delivery-staff-actions?riderId=" + rider, adm));
    assertThat(acts.path("items").get(0).path("action").asString()).isEqualTo("PAYOUT_PAID");
  }

  ResponseEntity<String> staffGet(String uri, String token) {
    return http.get()
        .uri(uri)
        .header("Authorization", "Bearer " + token)
        .retrieve()
        .toEntity(String.class);
  }

  /**
   * 도움요청 대화(0.6.0, 라이더 앱 요청 §11-3): 라이더 ↔ 지원센터 메시지. 라이더는 본인 사진만 붙이고 상담원 ID 는 못 본다, after 로 새 메시지만,
   * 쓸 권한 없는 직원 403·남의 라이더 404, 처리되면 409 HELP_REQUEST_CLOSED.
   */
  @Test
  void riderAndSupportExchangeMessagesOnHelpRequest() throws Exception {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String requestId =
        body(post(id, "help-requests", rider, Map.of("category", "ADDRESS_ISSUE")))
            .path("resultId")
            .asString();
    String thread = "help-requests/" + requestId + "/messages";
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    String worker = TestJwt.operator("OP-W", List.of("MFC_WORKER"), List.of("MFC-1"));
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    // 라이더 → (본인 사진) / 남의 사진은 422
    ResponseEntity<String> first =
        post(id, thread, rider, Map.of("body", "건물 입구를 못 찾겠어요", "photoRefs", List.of(photo)));
    assertThat(first.getStatusCode().value()).isEqualTo(201);
    assertThat(body(first).path("senderType").asString()).isEqualTo("RIDER");
    long seq1 = body(first).path("seq").asLong();
    assertThat(
            body(post(id, thread, rider, Map.of("body", "x", "photoRefs", List.of("PH-NOPE"))))
                .path("code")
                .asString())
        .isEqualTo("PHOTO_NOT_FOUND");
    // 상담원 답장 — 쓸 권한 없는 직원 403
    assertThat(
            staffPost("/deliveries/" + id + "/" + thread, worker, Map.of("body", "?"))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    // DE-2: 상담원은 보기만, 센터 관리자는 답장 없음
    String cs = TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of());
    String sup = TestJwt.operator("OP-SV", List.of("MFC_SUPERVISOR"), List.of("MFC-1"));
    assertThat(
            staffPost("/deliveries/" + id + "/" + thread, cs, Map.of("body", "?"))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(
            staffPost("/deliveries/" + id + "/" + thread, sup, Map.of("body", "?"))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(staffGet("/deliveries/" + id + "/" + thread, cs).getStatusCode().value())
        .isEqualTo(200);
    ResponseEntity<String> reply =
        staffPost("/deliveries/" + id + "/" + thread, ops, Map.of("body", "정문 왼쪽 계단으로 가세요"));
    assertThat(reply.getStatusCode().value()).isEqualTo(201);
    assertThat(body(reply).path("senderType").asString()).isEqualTo("SUPPORT");
    // 라이더 알림함용 사건(§11-5) — 메시지 내용 없음, 배송 버전은 그대로
    int versionBefore = body(staffGet("/deliveries/" + id, ops)).path("version").asInt();
    JsonNode replied = outbox(id, "HelpRequestReplied").path("payload");
    assertThat(replied.path("riderId").asString()).isEqualTo(rider);
    assertThat(replied.path("requestId").asString()).isEqualTo(requestId);
    assertThat(replied.toString()).doesNotContain("정문 왼쪽 계단으로 가세요");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'HelpRequestReplied'",
                Integer.class,
                id))
        .isEqualTo(1);
    assertThat(body(staffGet("/deliveries/" + id, ops)).path("version").asInt())
        .isEqualTo(versionBefore);
    // 직원은 둘 다·보낸 사람 ID 보임, 라이더는 after 뒤 상담원 메시지만·상담원 ID 없음
    JsonNode all = body(staffGet("/deliveries/" + id + "/" + thread, ops));
    assertThat(all.path("items")).hasSize(2);
    assertThat(all.path("items").get(1).path("senderId").asString()).isEqualTo("OP-RO");
    JsonNode fresh =
        body(staffGet("/deliveries/" + id + "/" + thread + "?after=" + seq1, TestJwt.rider(rider)));
    assertThat(fresh.path("items")).hasSize(1);
    assertThat(fresh.path("items").get(0).path("body").asString()).isEqualTo("정문 왼쪽 계단으로 가세요");
    assertThat(fresh.path("items").get(0).path("senderId").isNull()).isTrue();
    assertThat(fresh.path("helpState").asString()).isEqualTo("OPEN");
    // DE-2: 직원에게 마지막 메시지 시각, 라이더에게는 없음
    JsonNode staffHelp = body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, ops));
    assertThat(staffHelp.path("lastRiderMessageAt").isString()).isTrue();
    assertThat(staffHelp.path("lastSupportMessageAt").isString()).isTrue();
    JsonNode riderHelp =
        body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, TestJwt.rider(rider)));
    assertThat(riderHelp.path("lastRiderMessageAt").isString()).isFalse();
    // DE-2: 관리자 가림 — 지원센터 403, 사유 없으면 400, 라이더에게도 가려 보임
    String firstId = body(first).path("messageId").asString();
    String maskUri = "/deliveries/" + id + "/" + thread + "/" + firstId + "/maskings";
    String admin = TestJwt.operator("OP-AD", List.of("OPERATOR_ADMIN"), List.of());
    assertThat(staffPost(maskUri, ops, Map.of("reason", "전화번호")).getStatusCode().value())
        .isEqualTo(403);
    assertThat(staffPost(maskUri, admin, Map.of("reason", "x")).getStatusCode().value())
        .isEqualTo(400);
    JsonNode masked = body(staffPost(maskUri, admin, Map.of("reason", "고객 전화번호가 적힘")));
    assertThat(masked.path("masked").asBoolean()).isTrue();
    assertThat(masked.path("body").isNull()).isTrue();
    assertThat(masked.path("maskedBy").asString()).isEqualTo("OP-AD");
    JsonNode riderThread = body(staffGet("/deliveries/" + id + "/" + thread, TestJwt.rider(rider)));
    assertThat(riderThread.path("items").get(0).path("masked").asBoolean()).isTrue();
    assertThat(riderThread.path("items").get(0).path("body").isNull()).isTrue();
    assertThat(riderThread.path("items").get(0).path("maskReason").isNull()).isTrue();
    assertThat(riderThread.toString()).doesNotContain("건물 입구를 못 찾겠어요");
    assertThat(
            staffGet("/deliveries/" + id + "/" + thread, TestJwt.rider("RDR-OTHER"))
                .getStatusCode()
                .value())
        .isEqualTo(404);
    // §10 배송별 내 도움요청 목록 — 남의 라이더는 비어 있다(볼 수 없는 배송이 아니라 본인 것만)
    post(id, "help-requests", rider, Map.of("category", "OTHER"));
    JsonNode mine = body(staffGet("/deliveries/" + id + "/help-requests", TestJwt.rider(rider)));
    assertThat(mine.path("items")).hasSize(2);
    assertThat(mine.path("items").get(0).path("requestId").asString()).isEqualTo(requestId);
    assertThat(mine.path("items").get(0).path("resolvedBy").isNull()).isTrue();
    assertThat(body(staffGet("/deliveries/" + id + "/help-requests", ops)).path("items"))
        .hasSize(2);
    assertThat(
            body(staffGet("/deliveries/" + id + "/help-requests", TestJwt.rider("RDR-OTHER")))
                .path("items"))
        .isEmpty();
    // 처리되면 더 못 쓴다
    assertThat(resolve(id, requestId, ops, "K-" + UUID.randomUUID()).getStatusCode().value())
        .isEqualTo(200);
    ResponseEntity<String> closed = post(id, thread, rider, Map.of("body", "감사합니다"));
    assertThat(closed.getStatusCode().value()).isEqualTo(409);
    assertThat(body(closed).path("code").asString()).isEqualTo("HELP_REQUEST_CLOSED");
  }

  ResponseEntity<String> staffPost(String uri, String token, Object body) {
    return http.post()
        .uri(uri)
        .headers(
            h -> {
              h.setBearerAuth(token);
              h.set("Idempotency-Key", UUID.randomUUID().toString());
              h.set("X-Source-System", "backoffice");
            })
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .retrieve()
        .toEntity(String.class);
  }

  /** DE-CASE(2026-11-16): 사건함이 도움요청으로 사건을 열면 도움요청에 사건 번호·처리 중, 그동안 옛 닫기 API 는 409, 사건을 닫으면 처리됨. */
  @Test
  void supportCaseOpenAndCloseFollowHelpRequest() throws Exception {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String requestId =
        body(post(id, "help-requests", rider, Map.of("category", "CUSTOMER_UNREACHABLE")))
            .path("resultId")
            .asString();
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    // 라이더 앱 요청 §1: 배송 조회에 주문 요약(order 에서 읽음)
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching(
                    "/api/v1/internal/orders/.*/rider-summary"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"orderId\":\"x\",\"orderNumber\":\"48213907\",\"items\":[{\"name\":\"우유\",\"qty\":1,\"lineAmount\":2500}],\"itemAmount\":2500}")));
    JsonNode viewed = body(staffGet("/deliveries/" + id, TestJwt.rider(rider)));
    assertThat(viewed.path("orderSummary").path("items").get(0).path("name").asString())
        .isEqualTo("우유");
    assertThat(viewed.path("orderSummary").path("itemAmount").asInt()).isEqualTo(2500);
    assertThat(viewed.path("orderSummary").path("orderNumber").asString()).isEqualTo("48213907");
    String caseId = "CASE-" + UUID.randomUUID();
    kafka
        .send(
            "platform.case.v1",
            caseId,
            EventEnvelopeFixtures.envelope("CaseOpened")
                .from("case-service")
                .aggregate("Case", caseId, 1)
                .payload(
                    Map.of(
                        "caseId",
                        caseId,
                        "type",
                        "RIDER_HELP",
                        "refs",
                        List.of(
                            Map.of("refType", "incidentId", "refId", id + ":" + requestId),
                            Map.of("refType", "deliveryId", "refId", id),
                            Map.of("refType", "requestId", "refId", requestId)),
                        "openedAt",
                        Instant.now().toString()))
                .buildJson())
        .join();
    JsonNode help = null;
    for (int i = 0; i < 100; i++) {
      help = body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, ops));
      if (caseId.equals(help.path("caseId").asString())) {
        break;
      }
      Thread.sleep(200);
    }
    assertThat(help.path("caseId").asString()).isEqualTo(caseId);
    // DE-2: 열리기만 하면 접수됨 — 상담원이 집을 때 처리 중
    assertThat(help.path("state").asString()).isEqualTo("OPEN");
    caseEvent(
        "CaseAssigned",
        caseId,
        2,
        Map.of("caseId", caseId, "type", "RIDER_HELP", "assignee", "OP-RO"));
    help = awaitHelpState(id, requestId, ops, "IN_PROGRESS");
    assertThat(help.path("state").asString()).isEqualTo("IN_PROGRESS");
    // 관리자 팀으로 넘기면 담당이 비어 접수됨, 답장은 관리자만
    caseEvent(
        "CaseTransferred",
        caseId,
        3,
        Map.of(
            "caseId",
            caseId,
            "type",
            "RIDER_HELP",
            "fromGroup",
            "DELIVERY_OPS",
            "toGroup",
            "ORDER_OPS",
            "reason",
            "x"));
    help = awaitHelpState(id, requestId, ops, "OPEN");
    assertThat(help.path("state").asString()).isEqualTo("OPEN");
    String thread = "/deliveries/" + id + "/help-requests/" + requestId + "/messages";
    assertThat(staffPost(thread, ops, Map.of("body", "지원센터 답장")).getStatusCode().value())
        .isEqualTo(403);
    String admin = TestJwt.operator("OP-AD", List.of("OPERATOR_ADMIN"), List.of());
    assertThat(staffPost(thread, admin, Map.of("body", "관리자 답장")).getStatusCode().value())
        .isEqualTo(201);
    ResponseEntity<String> blocked = resolve(id, requestId, ops, "K-" + UUID.randomUUID());
    assertThat(blocked.getStatusCode().value()).isEqualTo(409);
    assertThat(body(blocked).path("code").asString()).isEqualTo("HELP_REQUEST_HAS_CASE");

    kafka
        .send(
            "platform.case.v1",
            caseId,
            EventEnvelopeFixtures.envelope("CaseClosed")
                .from("case-service")
                .aggregate("Case", caseId, 2)
                .payload(
                    Map.of(
                        "caseId",
                        caseId,
                        "type",
                        "RIDER_HELP",
                        "outcome",
                        "RESOLVED",
                        "closedAt",
                        Instant.now().toString()))
                .buildJson())
        .join();
    for (int i = 0; i < 100; i++) {
      help = body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, ops));
      if ("RESOLVED".equals(help.path("state").asString())) {
        break;
      }
      Thread.sleep(200);
    }
    assertThat(help.path("state").asString()).isEqualTo("RESOLVED");
    assertThat(help.path("resolvedBy").asString()).isEqualTo("case:" + caseId);
    assertThat(help.path("resolutionNote").asString()).contains("RESOLVED");

    kafka
        .send(
            "platform.case.v1",
            caseId,
            EventEnvelopeFixtures.envelope("CaseReopened")
                .from("case-service")
                .aggregate("Case", caseId, 3)
                .payload(
                    Map.of(
                        "caseId",
                        caseId,
                        "type",
                        "RIDER_HELP",
                        "reopenedAt",
                        Instant.now().toString()))
                .buildJson())
        .join();
    // DE-2: 다시 열리면 담당이 없으니 접수됨
    help = awaitHelpState(id, requestId, ops, "OPEN");
    assertThat(help.path("state").asString()).isEqualTo("OPEN");
  }

  void caseEvent(String type, String caseId, int version, Map<String, Object> payload) {
    kafka
        .send(
            "platform.case.v1",
            caseId,
            EventEnvelopeFixtures.envelope(type)
                .from("case-service")
                .aggregate("Case", caseId, version)
                .payload(payload)
                .buildJson())
        .join();
  }

  JsonNode awaitHelpState(String id, String requestId, String token, String state)
      throws InterruptedException {
    JsonNode help = null;
    for (int i = 0; i < 100; i++) {
      help = body(staffGet("/deliveries/" + id + "/help-requests/" + requestId, token));
      if (state.equals(help.path("state").asString())) {
        return help;
      }
      Thread.sleep(200);
    }
    return help;
  }

  ResponseEntity<String> resolve(String id, String requestId, String token, String key) {
    return http.post()
        .uri("/deliveries/{id}/help-requests/{r}/resolutions", id, requestId)
        .headers(
            h -> {
              h.setBearerAuth(token);
              h.set("Idempotency-Key", key);
              h.set("X-Source-System", "backoffice");
            })
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("note", "고객과 통화해 위치 안내"))
        .retrieve()
        .toEntity(String.class);
  }

  JsonNode view(String id, String rider) {
    return body(
        http.get()
            .uri("/deliveries/{id}", id)
            .header("Authorization", "Bearer " + TestJwt.rider(rider))
            .retrieve()
            .toEntity(String.class));
  }

  @Test
  void unassignReassignAndCancelBeforePickup() {
    String id = "DLV-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    created(id, shipment, order);
    assigned(id, order, "RDR-A", 1);
    awaitState(id, "ASSIGNED");
    assertThat(post(id, "store-arrivals", "RDR-A", Map.of()).getStatusCode().value())
        .isEqualTo(200);

    // 배차 취소(재배차) → 다시 배차 대기, 옛 배정 사건(낮은 버전)은 버림, 새 라이더 배정
    send(
        "delivery.delivery.v1",
        "DeliveryAssignmentChanged",
        "Delivery",
        id,
        Map.of(
            "deliveryId",
            id,
            "oldRiderId",
            "RDR-A",
            "assignmentVersion",
            2,
            "reason",
            "CANCELLED_RIDER",
            "changedAt",
            Instant.now().toString()));
    awaitState(id, "UNASSIGNED");
    assigned(id, order, "RDR-A", 1);
    assigned(id, order, "RDR-B", 3);
    awaitSql(
        "SELECT count(*) FROM execution_delivery WHERE delivery_id = '"
            + id
            + "' AND rider_id = 'RDR-B'");
    assertThat(state(id)).isEqualTo("ASSIGNED");
    assertThat(post(id, "store-arrivals", "RDR-A", Map.of()).getStatusCode().value())
        .isEqualTo(403);

    send(
        "delivery.delivery.v1",
        "DeliveryCancelledBeforePickup",
        "Delivery",
        id,
        Map.of("deliveryId", id, "orderId", order, "reason", "ORDER_CANCELLED"));
    awaitState(id, "CANCELLED");
    assertThat(post(id, "store-arrivals", "RDR-B", Map.of()).getStatusCode().value())
        .isEqualTo(409); // 취소된 배송 — 상태 전이 불가
  }

  @Test
  void sameRiderReassignedAfterOperatorCancelStillEarnsOnCompletion() {
    // 운영자 배차 취소(산정 불가 기록) 뒤 같은 라이더가 다시 배정돼 완료 → 완료 수행료가 따로 생긴다(V4, PC 1 검토 후속)
    String id = "DLV-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    created(id, shipment, order);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    send(
        "delivery.delivery.v1",
        "DeliveryAssignmentChanged",
        "Delivery",
        id,
        Map.of(
            "deliveryId",
            id,
            "oldRiderId",
            rider,
            "assignmentVersion",
            2,
            "reason",
            "CANCELLED_OPERATOR",
            "changedAt",
            Instant.now().toString()));
    awaitState(id, "UNASSIGNED");
    awaitSql("SELECT count(*) FROM rider_earning WHERE delivery_id = '" + id + "'");

    assigned(id, order, rider, 3);
    awaitSql(
        "SELECT count(*) FROM execution_delivery WHERE delivery_id = '"
            + id
            + "' AND assignment_version = 3");
    send(
        "mfc.fulfillment.v1",
        "ShipmentReady",
        "Shipment",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            "MFC-1",
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(Map.of("packageId", "PKG-" + id))));
    handedOver(id, shipment, order, rider, "PKG-" + id, "HO-" + id, Map.of("L-1", 1));
    awaitState(id, "PICKED_UP");
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String photoRef = body(upload(id, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(id, "completions", rider, completion(List.of(photoRef), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);

    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT assignment_version, calculable, total, settlement_state FROM rider_earning"
                + " WHERE delivery_id = ? ORDER BY assignment_version",
            id);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("assignment_version")).isEqualTo(1);
    assertThat(rows.get(0).get("calculable")).isEqualTo(false);
    assertThat(rows.get(1).get("assignment_version")).isEqualTo(3);
    assertThat(rows.get(1).get("calculable")).isEqualTo(true);
    assertThat(rows.get(1).get("total")).isEqualTo(3900);
    assertThat(rows.get(1).get("settlement_state")).isEqualTo("PENDING_CONFIRMATION");
    assertThat(view(id, rider).path("earning").path("settlementState").asString())
        .isEqualTo("PENDING_CONFIRMATION"); // 배송 조회는 가장 최근 수행료
  }

  /**
   * DD-2(사용자 2026-11-16 "추천대로"): 오배송 회수 배송 — 회수지에서 사진 → 회수(ReturnPickedUp, 원래 배송이 전달한 줄) → MFC 도착 =
   * 반납(ReturnDelivered, 수행료 = 수락 당시 금액). 원래 고객 전달지는 열지 않고, 일반 배송 단계는 409. 회수 라이더에게 원래 완료 증빙을 보여 준다.
   */
  /**
   * 전달 실패 반납(2026-11-17 — 사용자 "같은 라이더에 회수 배송 자동 배정", "고객 귀책이면 원래 측정돼 있던 수행료", "원배송 수행료 + 반납 이동료
   * 따로", 그 밖 귀책은 "기존대로 수행료 조정"). 확정 → 고객 사유면 실패한 배송 수행료 지급, 반납 배송(dispatch 가 만듦)은 받음 없이 배정에서 바로 MFC
   * 반납 + 반납 수행료. 고객 사유가 아니면 둘 다 산정 불가.
   */
  @Test
  void confirmedFailureGoodsReturnToStoreBySameRider() {
    for (String reason : List.of("CUSTOMER_ABSENT", "DAMAGED")) {
      boolean customer = "CUSTOMER_ABSENT".equals(reason);
      String origin = "DLV-" + UUID.randomUUID();
      String order = "ORD-" + UUID.randomUUID();
      String rider = "RDR-" + UUID.randomUUID();
      pickedUp(origin, "SHP-" + UUID.randomUUID(), order, rider);
      String photo = body(upload(origin, rider, JPEG)).path("photoRef").asString();
      assertThat(
              post(origin, "failures", rider, failure(reason, List.of(photo), 2))
                  .getStatusCode()
                  .value())
          .isEqualTo(200);
      assertThat(decide(origin, "OP-1", List.of("MFC-1"), "CONFIRM").getStatusCode().value())
          .isEqualTo(200);
      assertThat(outbox(origin, "DeliveryFailed").path("payload").path("custody").asString())
          .isEqualTo("RIDER");
      assertThat(
              jdbc.queryForObject(
                  "SELECT calculable FROM rider_earning WHERE delivery_id = ?",
                  Boolean.class,
                  origin))
          .as(reason)
          .isEqualTo(customer);

      boolean legacy = !customer; // V24 전 인계처럼 SKU 를 저장하지 않은 배송 → 주문 줄에서 채움(2026-11-20)
      if (legacy) {
        jdbc.update("UPDATE package_handover SET line_skus = NULL WHERE delivery_id = ?", origin);
        String lineId =
            jdbc.queryForObject(
                "SELECT k FROM package_handover, jsonb_object_keys(lines) k WHERE delivery_id = ?"
                    + " LIMIT 1",
                String.class,
                origin);
        orderApi.stubFor(
            com.github.tomakehurst.wiremock.client.WireMock.get(
                    com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                        "/api/v1/internal/orders/" + order))
                .willReturn(
                    com.github.tomakehurst.wiremock.client.WireMock.okJson(
                        mapper.writeValueAsString(
                            Map.of(
                                "orderId",
                                order,
                                "accountId",
                                "ACC-1",
                                "lines",
                                List.of(Map.of("orderLineId", lineId, "sku", "E-00000002")))))));
      }

      // dispatch 가 만든 반납 배송(같은 라이더 바로 배정)
      String back = "DLV-" + UUID.randomUUID();
      Map<String, Object> created = new java.util.LinkedHashMap<>();
      created.put("deliveryId", back);
      created.put("orderId", order);
      created.put("shipmentId", "FLR-" + origin);
      created.put("type", "RETURN");
      created.put("purpose", "FAILED_DELIVERY_RETURN");
      created.put("originDeliveryId", origin);
      created.put("mfcId", "MFC-1");
      created.put("dropAddressRef", "mfc:MFC-1");
      send("delivery.delivery.v1", "DeliveryCreated", "Delivery", back, created);
      assigned(back, order, rider, 1);
      awaitState(back, "ASSIGNED");
      assertThat(view(origin, rider).path("failure").path("returnDeliveryId").asString())
          .isEqualTo(back);
      assertThat(view(back, rider).path("purpose").asString()).isEqualTo("FAILED_DELIVERY_RETURN");
      assertThat(view(back, rider).path("returnPickupRequired").asBoolean()).isFalse();

      // 받음 없음(상품이 이미 손에) — 배정에서 바로 MFC 반납
      assertThat(
              post(
                      back,
                      "returns/pickups",
                      rider,
                      Map.of("items", List.of(), "evidenceRefs", List.of(photo)))
                  .getStatusCode()
                  .value())
          .isEqualTo(409);
      assertThat(post(back, "returns/store-deliveries", rider, Map.of()).getStatusCode().value())
          .isEqualTo(200);
      assertThat(state(back)).isEqualTo("RETURNED_TO_STORE");
      JsonNode returned = outbox(back, "ReturnDelivered").path("payload");
      assertThat(returned.path("purpose").asString()).isEqualTo("FAILED_DELIVERY_RETURN");
      assertThat(returned.path("originDeliveryId").asString()).isEqualTo(origin);
      assertThat(returned.path("lines").size()).isPositive();
      // 줄마다 인계 때 SKU(2026-11-20 PC 3 요청 — MFC 반납 바코드 대조)
      assertThat(returned.path("lines").get(0).path("sku").asString())
          .isEqualTo(legacy ? "E-00000002" : "E-00000001");
      assertThat(
              jdbc.queryForObject(
                  "SELECT calculable FROM rider_earning WHERE delivery_id = ?",
                  Boolean.class,
                  back))
          .as(reason)
          .isEqualTo(customer);
    }
  }

  /**
   * 다른 라이더 반납을 못 구해 원래 라이더에게 넘어감(2026-11-20 사용자 "가" — dispatch 가 5분 뒤 원래 라이더에게 바로 배정): 반납 배송이 원래
   * 라이더에게 배정되면 원래 배송 실패 기록의 returnBy 가 SAME_RIDER — 라이더 앱이 반납 배송으로 넘어가고, 받음 없이 바로 반납.
   */
  @Test
  void otherRiderReturnFallsBackToOriginalRider() {
    String origin = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String r1 = "RDR-" + UUID.randomUUID();
    pickedUp(origin, "SHP-" + UUID.randomUUID(), order, r1);
    Map<String, Object> direct =
        Map.of(
            "decision", "CONFIRM",
            "custody", "RIDER",
            "returnBy", "OTHER_RIDER",
            "reason", "CUSTOMER_ABSENT",
            "note", "IT");
    assertThat(decideWith(origin, List.of("RIDER_OPS"), direct).getStatusCode().value())
        .isEqualTo(200);
    assertThat(view(origin, r1).path("failure").path("returnBy").asString())
        .isEqualTo("OTHER_RIDER");
    String back = "DLV-" + UUID.randomUUID();
    Map<String, Object> created = new java.util.LinkedHashMap<>();
    created.put("deliveryId", back);
    created.put("orderId", order);
    created.put("shipmentId", "FLR-" + origin);
    created.put("type", "RETURN");
    created.put("purpose", "FAILED_DELIVERY_RETURN");
    created.put("originDeliveryId", origin);
    created.put("mfcId", "MFC-1");
    created.put("dropAddressRef", "mfc:MFC-1");
    send("delivery.delivery.v1", "DeliveryCreated", "Delivery", back, created);
    assigned(back, order, r1, 1); // 원래 라이더에게
    awaitState(back, "ASSIGNED");
    assertThat(view(origin, r1).path("failure").path("returnBy").asString())
        .isEqualTo("SAME_RIDER");
    assertThat(view(back, r1).path("returnPickupRequired").asBoolean()).isFalse();
  }

  /**
   * 라이더가 더 갈 수 없을 때(2026-11-17 — 사용자 "다른 라이더에게 보낼 수 있게"): 지원센터가 등록 없이 직접 확정(RIDER_OPS) +
   * OTHER_RIDER → 반납 배송을 다른 라이더가 받음 사진 → MFC 반납, 수행료는 그 라이더에게 그대로.
   */
  @Test
  void supportFailsForIncapacitatedRiderAndAnotherRiderReturnsGoods() {
    String origin = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String r1 = "RDR-" + UUID.randomUUID();
    String r2 = "RDR-" + UUID.randomUUID();
    pickedUp(origin, "SHP-" + UUID.randomUUID(), order, r1);
    // 사고 수행료(2026-11-18 — 거리 비율): 수락 3,900원·1,800m 중 MFC → 사고 지점 900m → 1,950원
    dispatchApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/deliveries/" + origin + "/incident-distance"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"deliveryId\":\""
                        + origin
                        + "\",\"distanceM\":900,\"source\":\"LAST_LOCATION\",\"recordedAt\":\""
                        + Instant.now()
                        + "\"}")));
    Map<String, Object> direct =
        Map.of(
            "decision", "CONFIRM",
            "custody", "RIDER",
            "returnBy", "OTHER_RIDER",
            "reason", "RIDER_INCIDENT",
            "note", "라이더 사고 — 통화 확인");
    // 상담원은 직접 처리 못 함, 라이더 운영은 됨
    assertThat(decideWith(origin, List.of("CS_AGENT"), direct).getStatusCode().value())
        .isEqualTo(403);
    ResponseEntity<String> ok = decideWith(origin, List.of("RIDER_OPS"), direct);
    assertThat(ok.getStatusCode().value()).as(ok.getBody()).isEqualTo(200);
    assertThat(state(origin)).isEqualTo("FAILED");
    JsonNode failed = outbox(origin, "DeliveryFailed").path("payload");
    assertThat(failed.path("reason").asString()).isEqualTo("RIDER_INCIDENT");
    assertThat(failed.path("returnBy").asString()).isEqualTo("OTHER_RIDER");
    // 배송 조회에도 반납할 사람(2026-11-19 — 라이더 앱이 OTHER_RIDER 면 반납 배송으로 넘어가지 않게)
    assertThat(view(origin, r1).path("failure").path("returnBy").asString())
        .isEqualTo("OTHER_RIDER");
    assertThat(failed.path("deliveryType").asString()).isEqualTo("FORWARD");
    Map<String, Object> earning =
        jdbc.queryForMap(
            "SELECT calculable, total, distance_m, settlement_state FROM rider_earning WHERE"
                + " delivery_id = ?",
            origin);
    assertThat(earning.get("calculable")).isEqualTo(true); // 사고 — 간 거리 비율
    assertThat(((Number) earning.get("total")).intValue()).isEqualTo(1950);
    assertThat(((Number) earning.get("distance_m")).intValue()).isEqualTo(900);
    assertThat(earning.get("settlement_state")).isEqualTo("PENDING_CONFIRMATION");

    String back = "DLV-" + UUID.randomUUID();
    Map<String, Object> created = new java.util.LinkedHashMap<>();
    created.put("deliveryId", back);
    created.put("orderId", order);
    created.put("shipmentId", "FLR-" + origin);
    created.put("type", "RETURN");
    created.put("purpose", "FAILED_DELIVERY_RETURN");
    created.put("originDeliveryId", origin);
    created.put("mfcId", "MFC-1");
    created.put("dropAddressRef", "mfc:MFC-1");
    send("delivery.delivery.v1", "DeliveryCreated", "Delivery", back, created);
    assigned(back, order, r2, 1);
    awaitState(back, "ASSIGNED");
    assertThat(view(back, r2).path("returnPickupRequired").asBoolean()).isTrue();
    // 다른 라이더는 받음부터 — 바로 반납 409
    assertThat(post(back, "returns/store-deliveries", r2, Map.of()).getStatusCode().value())
        .isEqualTo(409);
    String spot = body(upload(back, r2, JPEG)).path("photoRef").asString();
    assertThat(
            post(
                    back,
                    "returns/pickups",
                    r2,
                    Map.of("items", List.of(), "evidenceRefs", List.of(spot)))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(back)).isEqualTo("RETURN_PICKED_UP");
    assertThat(post(back, "returns/store-deliveries", r2, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    assertThat(
            jdbc.queryForObject(
                "SELECT calculable FROM rider_earning WHERE delivery_id = ? AND rider_id = ?",
                Boolean.class,
                back,
                r2))
        .isTrue();
  }

  /** 배차 전 배송도 주문번호로 찾는다(2026-11-18) — 배송이 생길 때 order 내부 주문의 orderNumber 를 읽어 둔다. */
  @Test
  void unassignedDeliveryIsFoundByOrderNumber() {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String number = String.valueOf(20_000_000 + (int) (Math.random() * 70_000_000));
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"orderId\":\"" + order + "\",\"orderNumber\":\"" + number + "\"}")));
    String shipment = "SHP-" + UUID.randomUUID();
    send(
        "delivery.delivery.v1",
        "DeliveryCreated",
        "Delivery",
        id,
        Map.of(
            "deliveryId",
            id,
            "orderId",
            order,
            "shipmentId",
            shipment,
            "type",
            "FORWARD",
            "mfcId",
            "MFC-1",
            "deliverySeq",
            1,
            "dropAddressRef",
            "order:" + order));
    awaitState(id, "UNASSIGNED");
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    awaitSql(
        "SELECT count(*) FROM execution_delivery WHERE delivery_id = '"
            + id
            + "' AND delivery_seq = 1");
    JsonNode found = body(staffGet("/deliveries?orderNumber=" + number, ops));
    assertThat(found.path("items").get(0).path("deliveryNumber").asString())
        .isEqualTo(number + "-1");
    assertThat(found.path("items").size()).isEqualTo(1);
    assertThat(found.path("items").get(0).path("deliveryId").asString()).isEqualTo(id);
    assertThat(found.path("items").get(0).path("orderNumber").asString()).isEqualTo(number);
    // DE-7 상태별 수(2026-11-19): 모든 상태가 0 까지, 거르기 같음
    JsonNode counts = body(staffGet("/deliveries/counts?orderNumber=" + number, ops));
    assertThat(counts.path("counts").size()).isEqualTo(13);
    long unassigned = 0;
    long total = 0;
    for (JsonNode c : counts.path("counts")) {
      total += c.path("count").asLong();
      if ("UNASSIGNED".equals(c.path("key").asString())) {
        unassigned = c.path("count").asLong();
      }
      assertThat(c.path("capped").asBoolean()).isFalse();
    }
    assertThat(unassigned).isEqualTo(1);
    assertThat(total).isEqualTo(1);
    // 화면 번호 묶음 조회(2026-11-19): 쉼표로 이은 ID, 없는 ID 는 빠짐, 51개면 400
    JsonNode batch = body(staffGet("/deliveries?deliveryIds=" + id + ",DLV-none", ops));
    assertThat(batch.path("items").size()).isEqualTo(1);
    assertThat(batch.path("items").get(0).path("deliveryNumber").asString())
        .isEqualTo(number + "-1");
    String many =
        String.join(
            ",", java.util.stream.IntStream.range(0, 51).mapToObj(k -> "DLV-" + k).toList());
    assertThat(staffGet("/deliveries?deliveryIds=" + many, ops).getStatusCode().value())
        .isEqualTo(400);
  }

  /** 실패 확정 되돌리기(2026-11-19 — LAN 28767701-1): 직접 확정(사고) 뒤 라이더 운영이 되돌림 → PICKED_UP, 수행료 지움, 사건. */
  @Test
  void staffReversesMistakenFailureConfirmation() {
    String origin = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String r1 = "RDR-" + UUID.randomUUID();
    pickedUp(origin, "SHP-" + UUID.randomUUID(), order, r1);
    dispatchApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/deliveries/" + origin + "/incident-distance"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.okJson(
                    "{\"deliveryId\":\""
                        + origin
                        + "\",\"distanceM\":900,\"source\":\"LAST_LOCATION\",\"recordedAt\":\""
                        + Instant.now()
                        + "\"}")));
    assertThat(
            decideWith(
                    origin,
                    List.of("RIDER_OPS"),
                    Map.of(
                        "decision", "CONFIRM",
                        "custody", "RIDER",
                        "returnBy", "OTHER_RIDER",
                        "reason", "RIDER_INCIDENT",
                        "note", "잘못 누름"))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(origin)).isEqualTo("FAILED");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM rider_earning WHERE delivery_id = ?", Integer.class, origin))
        .isEqualTo(1);
    String uri = "/deliveries/" + origin + "/failure-reversals";
    Map<String, Object> body = Map.of("reason", "반려하려다 확정을 잘못 누름");
    assertThat(
            staffPost(uri, TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()), body)
                .getStatusCode()
                .value())
        .isEqualTo(403);
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    // 사고 확정은 새 상품 재발송이 이미 걸려 막힌다(사용자 "사고·파손은 막기") — 재발송을 멈췄다는 확인은 관리자만
    assertThat(staffPost(uri, ops, body).getBody()).contains("FAILURE_NOT_REVERSIBLE");
    Map<String, Object> stopped = Map.of("reason", "재발송 출고 중지 확인", "reshipmentStopped", true);
    assertThat(staffPost(uri, ops, stopped).getStatusCode().value()).isEqualTo(403);
    String adm = TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of());
    ResponseEntity<String> ok = staffPost(uri, adm, stopped);
    assertThat(ok.getStatusCode().value()).as(ok.getBody()).isEqualTo(200);
    assertThat(state(origin)).isEqualTo("PICKED_UP");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM rider_earning WHERE delivery_id = ?", Integer.class, origin))
        .isZero();
    JsonNode ev = outbox(origin, "DeliveryFailureReverted").path("payload");
    assertThat(ev.path("restoredState").asString()).isEqualTo("PICKED_UP");
    assertThat(ev.path("originalReason").asString()).isEqualTo("RIDER_INCIDENT");
    assertThat(ev.toString()).doesNotContain("출고 중지");
    // 라이더 앱에는 반려처럼 — 전달을 이어 간다
    JsonNode v = view(origin, r1);
    assertThat(v.path("state").asString()).isEqualTo("PICKED_UP");
    assertThat(v.path("failure").path("decision").asString()).isEqualTo("REJECT");
    // 다시 되돌리기는 409, 조치 이력에 남음
    assertThat(staffPost(uri, adm, stopped).getBody()).contains("FAILURE_NOT_REVERSIBLE");
    JsonNode acts =
        body(
            staffGet(
                "/delivery-staff-actions?deliveryId=" + origin,
                TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of())));
    assertThat(acts.path("items").get(0).path("action").asString()).isEqualTo("FAILURE_REVERSED");

    // 재발송이 걸리지 않는 사유(고객 연락 안 됨)는 라이더 운영이 그냥 되돌린다
    String other = "DLV-" + UUID.randomUUID();
    pickedUp(other, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), r1);
    assertThat(
            decideWith(
                    other,
                    List.of("RIDER_OPS"),
                    Map.of(
                        "decision", "CONFIRM",
                        "custody", "RIDER",
                        "reason", "UNREACHABLE",
                        "note", "잘못 누름"))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    ResponseEntity<String> plain =
        staffPost("/deliveries/" + other + "/failure-reversals", ops, body);
    assertThat(plain.getStatusCode().value()).as(plain.getBody()).isEqualTo(200);
    assertThat(state(other)).isEqualTo("PICKED_UP");
  }

  /** DE-6(2026-11-19): 묶음 앞 배송을 끝내면 다음 차례 고객에게 "가고 있어요" 한 번, 마지막·단건은 없음. */
  @Test
  void completingFirstStopAnnouncesNextCustomer() {
    String rider = "RDR-hd-" + UUID.randomUUID();
    String a = "DLV-hd-" + UUID.randomUUID();
    String b = "DLV-hd-" + UUID.randomUUID();
    pickedUp(a, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    pickedUp(b, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    jdbc.update(
        "UPDATE execution_delivery SET bundle_id = 'BND-hd', stop_seq = 1 WHERE delivery_id = ?",
        a);
    jdbc.update(
        "UPDATE execution_delivery SET bundle_id = 'BND-hd', stop_seq = 2 WHERE delivery_id = ?",
        b);
    String takenAt = Instant.now().toString();
    assertThat(post(a, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String photo = body(upload(a, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(a, "completions", rider, completion(List.of(photo), takenAt))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    JsonNode heading = outbox(b, "DeliveryHeadingToCustomer").path("payload");
    assertThat(heading.path("deliveryId").asString()).isEqualTo(b);
    assertThat(heading.path("purpose").asString()).isEqualTo("DELIVERY");
    // 마지막 배송을 끝내면 다음이 없다
    assertThat(post(b, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String photo2 = body(upload(b, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(b, "completions", rider, completion(List.of(photo2), takenAt))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE type = 'DeliveryHeadingToCustomer' AND"
                    + " aggregateid IN (?, ?)",
                Integer.class,
                a,
                b))
        .isEqualTo(1);
  }

  ResponseEntity<String> decideWith(String id, List<String> roles, Map<String, Object> body) {
    return http.post()
        .uri("/deliveries/{id}/failure-decisions", id)
        .headers(
            h -> {
              h.setBearerAuth(TestJwt.operator("OP-X", roles, List.of()));
              h.set("Idempotency-Key", UUID.randomUUID().toString());
              h.set("X-Source-System", "rider-backoffice-web");
            })
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .retrieve()
        .toEntity(String.class);
  }

  /**
   * 엘리베이터 고장 추가 배달료(2026-11-17 — 원문 §9.2, 15층부터 5층 단위 500원, 다른 사유 없음): 라이더 신청 → 라이더 운영 승인 → 합계에 더함.
   */
  @Test
  void elevatorSurchargeRequestedAndApproved() {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String elevator = body(upload(id, rider, JPEG)).path("photoRef").asString();
    // 완료 전에는 신청 못 함
    assertThat(
            post(
                    id,
                    "surcharge-requests",
                    rider,
                    Map.of("floor", 22, "evidenceRefs", List.of(elevator)))
                .getStatusCode()
                .value())
        .isEqualTo(409);
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String door = body(upload(id, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(id, "completions", rider, completion(List.of(door), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    ResponseEntity<String> low =
        post(
            id,
            "surcharge-requests",
            rider,
            Map.of("floor", 14, "evidenceRefs", List.of(elevator)));
    assertThat(body(low).path("code").asString()).isEqualTo("FLOOR_BELOW_SURCHARGE");
    ResponseEntity<String> req =
        post(
            id,
            "surcharge-requests",
            rider,
            Map.of("floor", 22, "evidenceRefs", List.of(elevator)));
    assertThat(req.getStatusCode().value()).as(req.getBody()).isEqualTo(201);
    assertThat(body(req).path("amount").asInt()).isEqualTo(1000); // 20~24층
    assertThat(body(req).path("state").asString()).isEqualTo("REQUESTED");
    String reqId = body(req).path("requestId").asString();
    assertThat(
            post(
                    id,
                    "surcharge-requests",
                    rider,
                    Map.of("floor", 22, "evidenceRefs", List.of(elevator)))
                .getStatusCode()
                .value())
        .isEqualTo(409); // 배송마다 하나
    int before =
        jdbc.queryForObject(
            "SELECT total FROM rider_earning WHERE delivery_id = ?", Integer.class, id);
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    JsonNode queue = body(staffGet("/surcharge-requests?riderId=" + rider, ops));
    assertThat(queue.path("items").get(0).path("requestId").asString()).isEqualTo(reqId);
    ResponseEntity<String> ok =
        http.post()
            .uri("/surcharge-requests/{id}/decisions", reqId)
            .header("Authorization", "Bearer " + ops)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .header("X-Source-System", "rider-backoffice-web")
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("decision", "APPROVE"))
            .retrieve()
            .toEntity(String.class);
    assertThat(ok.getStatusCode().value()).as(ok.getBody()).isEqualTo(200);
    assertThat(body(ok).path("decidedBy").asString()).isEqualTo("OP-RO");
    // 상태를 비우면 모든 상태(2026-11-18 — 백오피스 "전체" 탭), REQUESTED 로 거르면 승인된 건은 빠진다
    assertThat(
            body(staffGet("/surcharge-requests?riderId=" + rider, ops))
                .path("items")
                .get(0)
                .path("state")
                .asString())
        .isEqualTo("APPROVED");
    assertThat(
            body(staffGet("/surcharge-requests?state=REQUESTED&riderId=" + rider, ops))
                .path("items"))
        .isEmpty();
    assertThat(
            jdbc.queryForObject(
                "SELECT total FROM rider_earning WHERE delivery_id = ?", Integer.class, id))
        .isEqualTo(before + 1000);
    JsonNode v = view(id, rider);
    assertThat(v.path("earning").path("compensation").asInt()).isEqualTo(1000);
  }

  /**
   * 고객 수령 확인·자동 완료(2026-11-17 — 사용자 "5시간 지나면 자동완료", "고객에게 받았는지 물어보고 고객이 전달완료를 눌러주면 라이더앱에도 반영"): 고객
   * 확인 → COMPLETED(CUSTOMER_CONFIRMED)·수행료 정상, 픽업 5시간 지남 → 자동 완료(AUTO_COMPLETED)·수행료 산정 불가(지원센터
   * 조정).
   */
  @Test
  void customerConfirmsReceiptAndStaleDeliveryAutoCompletes() {
    String order = "ORD-" + UUID.randomUUID();
    String me = "ACC-" + UUID.randomUUID();
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), order, rider);
    var stub =
        orderApi.stubFor(
            com.github.tomakehurst.wiremock.client.WireMock.get(
                    com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                        "/api/v1/internal/orders/" + order))
                .willReturn(
                    com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"orderId\":\"" + order + "\",\"accountId\":\"" + me + "\"}")));
    assertThat(receipt(id, TestJwt.customer("ACC-OTHER")).getStatusCode().value()).isEqualTo(404);
    ResponseEntity<String> ok = receipt(id, TestJwt.customer(me));
    assertThat(ok.getStatusCode().value()).as(ok.getBody()).isEqualTo(200);
    assertThat(state(id)).isEqualTo("COMPLETED");
    JsonNode done = outbox(id, "DeliveryCompleted").path("payload");
    assertThat(done.path("method").asString()).isEqualTo("CUSTOMER_CONFIRMED");
    assertThat(done.path("evidenceRef").path("kind").asString()).isEqualTo("DOCUMENT");
    assertThat(view(id, rider).path("state").asString()).isEqualTo("COMPLETED"); // 라이더 앱에도
    assertThat(view(id, rider).path("evidence").path("method").asString())
        .isEqualTo("CUSTOMER_CONFIRMED"); // 요청 #7 — 계약 enum
    assertThat(
            jdbc.queryForObject(
                "SELECT calculable FROM rider_earning WHERE delivery_id = ?", Boolean.class, id))
        .isTrue();
    assertThat(receipt(id, TestJwt.customer(me)).getStatusCode().value()).isEqualTo(409);
    orderApi.removeStub(stub);

    // 픽업 5시간 지남 → 자동 완료, 열린 도움요청이 있으면 그대로
    String old = "DLV-" + UUID.randomUUID();
    pickedUp(old, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String helped = "DLV-" + UUID.randomUUID();
    pickedUp(helped, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    assertThat(
            post(helped, "help-requests", rider, Map.of("category", "ADDRESS_ISSUE"))
                .getStatusCode()
                .value())
        .isIn(200, 201, 202);
    jdbc.update(
        "UPDATE execution_delivery SET picked_up_at = now() - interval '6 hours' WHERE delivery_id"
            + " IN (?, ?)",
        old,
        helped);
    autoCompletion.run();
    assertThat(state(old)).isEqualTo("COMPLETED");
    assertThat(outbox(old, "DeliveryCompleted").path("payload").path("method").asString())
        .isEqualTo("AUTO_COMPLETED");
    JsonNode auto = view(old, rider);
    assertThat(auto.path("evidence").path("method").asString()).isEqualTo("AUTO_COMPLETED");
    assertThat(auto.path("evidence").path("photoRefs")).isEmpty();
    assertThat(auto.path("history").valueStream().map(h -> h.path("actor").asString()))
        .contains("SYSTEM_EVENT");
    assertThat(
            jdbc.queryForObject(
                "SELECT calculable FROM rider_earning WHERE delivery_id = ?", Boolean.class, old))
        .isFalse(); // 수익은 자동 확정하지 않음(원문 §6.4) — 지원센터 조정
    assertThat(state(helped)).isEqualTo("PICKED_UP");
  }

  ResponseEntity<String> receipt(String id, String token) {
    return http.post()
        .uri("/deliveries/{id}/receipt-confirmations", id)
        .header("Authorization", "Bearer " + token)
        .header("Idempotency-Key", UUID.randomUUID().toString())
        .header("X-Source-System", "customer-app")
        .retrieve()
        .toEntity(String.class);
  }

  /** 운영자 상태 정정(2026-11-17 — 사용자 "라이더 운영 관리자가 승인"): 픽업 뒤 고객 도착 ↔ 픽업만, 이력·사건·기록. */
  @Test
  void riderOpsCorrectsMistakenArrival() {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    Map<String, Object> back =
        Map.of("targetState", "PICKED_UP", "reason", "라이더가 도착을 일찍 누름 — 통화 확인");
    String cs = TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of());
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    assertThat(correct(id, cs, back).getStatusCode().value()).isEqualTo(403);
    ResponseEntity<String> ok = correct(id, ops, back);
    assertThat(ok.getStatusCode().value()).as(ok.getBody()).isEqualTo(200);
    assertThat(state(id)).isEqualTo("PICKED_UP");
    JsonNode ev = outbox(id, "DeliveryStatusCorrected").path("payload");
    // 직원 조치 이력(2026-11-18): 관리자만, 정정한 직원·전후 상태
    String adm = TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of());
    JsonNode acts = body(staffGet("/delivery-staff-actions?deliveryId=" + id, adm));
    assertThat(acts.path("items").get(0).path("action").asString()).isEqualTo("STATUS_CORRECTED");
    assertThat(acts.path("items").get(0).path("actorId").asString()).isEqualTo("OP-RO");
    assertThat(acts.path("items").get(0).path("detail").asString())
        .isEqualTo("ARRIVED_AT_CUSTOMER→PICKED_UP");
    assertThat(staffGet("/delivery-staff-actions", ops).getStatusCode().value()).isEqualTo(403);
    assertThat(ev.path("from").asString()).isEqualTo("ARRIVED_AT_CUSTOMER");
    assertThat(ev.path("to").asString()).isEqualTo("PICKED_UP");
    assertThat(ev.path("operatorId").asString()).isEqualTo("OP-RO");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM execution_history WHERE delivery_id = ? AND actor = 'OPERATOR'"
                    + " AND state = 'PICKED_UP'",
                Integer.class,
                id))
        .isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT corrected_by FROM delivery_status_correction WHERE delivery_id = ?",
                String.class,
                id))
        .isEqualTo("OP-RO");
    // 픽업 뒤 → 픽업 전, 같은 상태는 정정할 수 없다
    ResponseEntity<String> bad =
        correct(id, ops, Map.of("targetState", "ASSIGNED", "reason", "되돌리기"));
    assertThat(bad.getStatusCode().value()).isEqualTo(422);
    assertThat(body(bad).path("code").asString()).isEqualTo("CORRECTION_NOT_ALLOWED");
    assertThat(correct(id, ops, back).getStatusCode().value()).isEqualTo(422);
    // 직원 근거 사진(2026-11-18 요청 #6): 올리고 정정에 붙인다, 다른 배송의 사진은 422
    ResponseEntity<String> staffPhoto = staffUpload(id, ops);
    assertThat(staffPhoto.getStatusCode().value()).as(staffPhoto.getBody()).isEqualTo(201);
    String evidence = body(staffPhoto).path("photoRef").asString();
    assertThat(
            staffUpload(id, TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    ResponseEntity<String> wrong =
        correct(
            id,
            ops,
            Map.of(
                "targetState",
                "PICKED_UP",
                "reason",
                "다시 확인",
                "evidenceRefs",
                List.of("EVP-none")));
    assertThat(body(wrong).path("code").asString()).isEqualTo("PHOTO_NOT_FOUND");
    ResponseEntity<String> withPhoto =
        correct(
            id,
            ops,
            Map.of(
                "targetState",
                "PICKED_UP",
                "reason",
                "통화·사진 확인",
                "evidenceRefs",
                List.of(evidence)));
    assertThat(withPhoto.getStatusCode().value()).as(withPhoto.getBody()).isEqualTo(200);
    // 라이더는 다시 고객 도착을 누를 수 있다
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
  }

  ResponseEntity<String> staffUpload(String id, String token) {
    LinkedMultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
    form.add(
        "file",
        new ByteArrayResource(JPEG) {
          @Override
          public String getFilename() {
            return "evidence.jpg";
          }
        });
    return http.post()
        .uri("/deliveries/{id}/staff-evidence-photos", id)
        .header("Authorization", "Bearer " + token)
        .header("Idempotency-Key", UUID.randomUUID().toString())
        .header("X-Source-System", "rider-backoffice-web")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(form)
        .retrieve()
        .toEntity(String.class);
  }

  ResponseEntity<String> correct(String id, String token, Map<String, Object> body) {
    return http.post()
        .uri("/deliveries/{id}/status-corrections", id)
        .header("Authorization", "Bearer " + token)
        .header("Idempotency-Key", UUID.randomUUID().toString())
        .header("X-Source-System", "rider-backoffice-web")
        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
        .body(body)
        .retrieve()
        .toEntity(String.class);
  }

  @Test
  void misdeliveryRecoveryPicksUpAtSpotAndReturnsToStore() {
    String origin = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String r1 = "RDR-" + UUID.randomUUID();
    String r2 = "RDR-" + UUID.randomUUID();
    pickedUp(origin, "SHP-" + UUID.randomUUID(), order, r1);
    assertThat(post(origin, "customer-arrivals", r1, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String originPhoto = body(upload(origin, r1, JPEG)).path("photoRef").asString();
    assertThat(
            post(
                    origin,
                    "completions",
                    r1,
                    completion(List.of(originPhoto), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);

    String caseId = "CASE-" + UUID.randomUUID();
    String rec = "DLV-" + UUID.randomUUID();
    Map<String, Object> created = new java.util.LinkedHashMap<>();
    created.put("deliveryId", rec);
    created.put("orderId", order);
    created.put("shipmentId", "RCV-" + caseId);
    created.put("type", "RETURN");
    created.put("purpose", "MISDELIVERY_RECOVERY");
    created.put("caseId", caseId);
    created.put("originDeliveryId", origin);
    created.put("mfcId", "MFC-1");
    created.put("dropAddressRef", "mfc:MFC-1");
    send("delivery.delivery.v1", "DeliveryCreated", "Delivery", rec, created);
    assigned(rec, order, r2, 1);
    awaitState(rec, "ASSIGNED");
    assertThat(
            jdbc.queryForObject(
                "SELECT type || ':' || purpose FROM execution_delivery WHERE delivery_id = ?",
                String.class,
                rec))
        .isEqualTo("RETURN:MISDELIVERY_RECOVERY");

    // 원래 고객 전달지는 열지 않는다, 일반 배송 단계는 409
    ResponseEntity<String> drop = dropInfo(rec, r2);
    assertThat(drop.getStatusCode().value()).isEqualTo(409);
    assertThat(body(drop).path("code").asString()).isEqualTo("NO_CUSTOMER_DROP");
    assertThat(post(rec, "store-arrivals", r2, Map.of()).getStatusCode().value()).isEqualTo(409);
    // 회수 라이더에게 원래 완료 증빙(놓은 곳·사진 참조)
    JsonNode v = view(rec, r2);
    assertThat(v.path("purpose").asString()).isEqualTo("MISDELIVERY_RECOVERY");
    assertThat(v.path("originDeliveryId").asString()).isEqualTo(origin);
    assertThat(v.path("recoveryOrigin").path("placement").asString()).isEqualTo("FRONT_DOOR");
    assertThat(v.path("recoveryOrigin").path("photoRefs").get(0).asString()).isEqualTo(originPhoto);

    // 원래 고객 전화(사용자 2026-11-16 "원래 고객의 전화는 회수 라이더에게 보여야 해") — 전화만, 주소는 없다
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        mapper.writeValueAsString(
                            Map.of(
                                "orderId",
                                order,
                                "roadAddress",
                                "서울 강남구 테헤란로 1",
                                "detailAddress",
                                "101동 1001호",
                                "receiverName",
                                "김고객",
                                "receiverPhone",
                                "01012345678")))));
    ResponseEntity<String> contact = riderGet("/deliveries/{id}/recovery-contact", rec, r2);
    assertThat(contact.getStatusCode().value()).isEqualTo(200);
    assertThat(contact.getHeaders().getCacheControl()).contains("no-store");
    assertThat(body(contact).path("receiverPhone").asString()).isEqualTo("01012345678");
    assertThat(body(contact).path("originDeliveryId").asString()).isEqualTo(origin);
    // 오배송 회수는 주소를 주지 않는다(null 이거나 없음)
    assertThat(
            body(contact).path("roadAddress").isNull()
                || body(contact).path("roadAddress").isMissingNode())
        .isTrue();
    assertThat(body(contact).path("purpose").asString()).isEqualTo("MISDELIVERY_RECOVERY");
    assertThat(riderGet("/deliveries/{id}/recovery-contact", rec, r1).getStatusCode().value())
        .isEqualTo(404); // 회수 라이더가 아니면 있는지도 모른다
    ResponseEntity<String> notRecovery = riderGet("/deliveries/{id}/recovery-contact", origin, r1);
    assertThat(notRecovery.getStatusCode().value()).isEqualTo(409);
    assertThat(body(notRecovery).path("code").asString()).isEqualTo("NOT_RECOVERY");

    // 사진 보기(사용자 2026-11-16 "사진보기 기능 넣어") — 회수 라이더는 원래 배송 사진을, 원래 라이더·운영자도 본다
    ResponseEntity<byte[]> img = photo(origin, originPhoto, TestJwt.rider(r2));
    assertThat(img.getStatusCode().value()).isEqualTo(200);
    assertThat(img.getBody()).isEqualTo(JPEG);
    assertThat(img.getHeaders().getContentType().toString()).isEqualTo("image/jpeg");
    assertThat(img.getHeaders().getCacheControl()).contains("no-store");
    assertThat(photo(origin, originPhoto, TestJwt.rider(r1)).getStatusCode().value())
        .isEqualTo(200);
    assertThat(
            photo(origin, originPhoto, TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of()))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(photo(origin, originPhoto, TestJwt.rider("RDR-other")).getStatusCode().value())
        .isEqualTo(404);
    assertThat(photo(rec, originPhoto, TestJwt.rider(r2)).getStatusCode().value())
        .isEqualTo(404); // 다른 배송의 사진
    assertThat(photo(origin, "EVP-none", TestJwt.rider(r2)).getStatusCode().value()).isEqualTo(404);

    // 사진 없이 회수 → 400(계약 minItems 1), 남의 사진 → 422, 회수지에서 사진(배정 상태에서 올릴 수 있다) → 회수
    assertThat(
            post(rec, "returns/pickups", r2, Map.of("items", List.of(), "evidenceRefs", List.of()))
                .getStatusCode()
                .value())
        .isEqualTo(400);
    ResponseEntity<String> notMine =
        post(
            rec,
            "returns/pickups",
            r2,
            Map.of("items", List.of(), "evidenceRefs", List.of(originPhoto)));
    assertThat(notMine.getStatusCode().value()).isEqualTo(422);
    assertThat(body(notMine).path("code").asString()).isEqualTo("EVIDENCE_REQUIRED");
    String spotPhoto = body(upload(rec, r2, JPEG)).path("photoRef").asString();
    assertThat(spotPhoto).isNotBlank();
    assertThat(
            post(
                    rec,
                    "returns/pickups",
                    r2,
                    Map.of("items", List.of(), "evidenceRefs", List.of(spotPhoto)))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(rec)).isEqualTo("RETURN_PICKED_UP");
    JsonNode picked = outbox(rec, "ReturnPickedUp").path("payload");
    assertThat(picked.path("purpose").asString()).isEqualTo("MISDELIVERY_RECOVERY");
    assertThat(picked.path("caseId").asString()).isEqualTo(caseId);
    assertThat(picked.path("originDeliveryId").asString()).isEqualTo(origin);
    assertThat(picked.path("riderId").asString()).isEqualTo(r2);
    assertThat(picked.path("lines").get(0).path("orderLineId").asString()).isEqualTo("L-1");
    assertThat(picked.path("lines").get(0).path("qty").asInt()).isEqualTo(1);
    // 전달 완료는 회수 배송에 쓰지 않는다
    assertThat(
            post(rec, "completions", r2, completion(List.of(spotPhoto), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(409);

    // MFC 반납 인증번호(0.27.0): 센터 직원 화면에 번호, 틀리면 422, 5번이면 잠김, 새로 내면 풀림
    String worker = TestJwt.operator("OP-W", List.of("MFC_WORKER"), List.of("MFC-1"));
    assertThat(staffGet("/return-handovers", worker).getStatusCode().value()).isEqualTo(403);
    String code = handoverCode(rec, worker);
    String wrong = code.equals("0000") ? "1111" : "0000";
    ResponseEntity<String> miss =
        post(rec, "returns/store-deliveries", r2, Map.of("handoverCode", wrong));
    assertThat(miss.getStatusCode().value()).isEqualTo(422);
    assertThat(miss.getBody()).contains("RETURN_HANDOVER_CODE_MISMATCH").doesNotContain(code);
    for (int k = 0; k < 4; k++) {
      miss = post(rec, "returns/store-deliveries", r2, Map.of("handoverCode", wrong));
    }
    assertThat(miss.getBody()).contains("RETURN_HANDOVER_LOCKED");
    assertThat(post(rec, "returns/store-deliveries", r2, Map.of("handoverCode", code)).getBody())
        .contains("RETURN_HANDOVER_LOCKED");
    assertThat(state(rec)).isEqualTo("RETURN_PICKED_UP");
    ResponseEntity<String> re =
        http.post()
            .uri("/return-handovers/{id}/code-reissues", rec)
            .header("Authorization", "Bearer " + worker)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .header("X-Source-System", "mfc-app")
            .retrieve()
            .toEntity(String.class);
    assertThat(re.getStatusCode().value()).isEqualTo(200);
    String fresh = body(re).path("code").asString();
    assertThat(body(re).path("locked").asBoolean()).isFalse();
    assertThat(handoverCode(rec, worker)).isEqualTo(fresh);
    // MFC 도착 = 반납 → 배송 끝, 수행료
    assertThat(
            post(rec, "returns/store-deliveries", r2, Map.of("handoverCode", fresh))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(handoverCodeOrNull(rec, worker)).isNull(); // 반납 끝나면 목록에서 빠짐
    assertThat(state(rec)).isEqualTo("RETURNED_TO_STORE");
    JsonNode returned = outbox(rec, "ReturnDelivered").path("payload");
    assertThat(returned.path("mfcId").asString()).isEqualTo("MFC-1");
    assertThat(returned.path("lines")).hasSize(1);
    assertThat(returned.path("lines").get(0).path("sku").asString()).isEqualTo("E-00000001");
    Map<String, Object> earning =
        jdbc.queryForMap(
            "SELECT rider_id, calculable, total, settlement_state FROM rider_earning WHERE"
                + " delivery_id = ?",
            rec);
    assertThat(earning.get("rider_id")).isEqualTo(r2);
    assertThat(earning.get("calculable")).isEqualTo(true);
    assertThat(earning.get("total")).isEqualTo(3900);
    assertThat(earning.get("settlement_state")).isEqualTo("PENDING_CONFIRMATION");
    assertThat(view(rec, r2).path("evidence").path("photoRefs").get(0).asString())
        .isEqualTo(spotPhoto);
    // 반납 뒤에는 원래 고객 전화·원래 배송 사진을 닫는다(자기 회수 사진은 본다)
    assertThat(body(riderGet("/deliveries/{id}/recovery-contact", rec, r2)).path("code").asString())
        .isEqualTo("RECOVERY_CONTACT_CLOSED");
    assertThat(photo(origin, originPhoto, TestJwt.rider(r2)).getStatusCode().value())
        .isEqualTo(404);
    assertThat(photo(rec, spotPhoto, TestJwt.rider(r2)).getStatusCode().value()).isEqualTo(200);
  }

  /**
   * 고객 반품 회수(CL-1 배송 쪽, 2026-11-17 — 사용자 "실패마다 알림, 다시 시도는 반품 쪽", "고객 사정 실패도 수락 당시 금액"): 배정 → 고객 집
   * 전달지(= 회수지) → 사진·받음(lines = claim 요청 줄) → MFC 반납(claimId). 받기 전 실패 등록 → 확정이면 FAILED +
   * ReturnPickupFailed, 고객 사정이면 수행료 지급(산정 가능), 그 밖 사유는 산정 불가.
   */
  @Test
  void claimReturnPicksUpAtCustomerHomeOrFailsWithNotice() {
    String order = "ORD-" + UUID.randomUUID();
    String claimId = "CLM-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        mapper.writeValueAsString(
                            Map.of(
                                "orderId",
                                order,
                                "roadAddress",
                                "서울 강남구 테헤란로 1",
                                "detailAddress",
                                "101동 1001호",
                                "receiverName",
                                "김고객",
                                "receiverPhone",
                                "01012345678")))));

    // 1) 받음 → 반납
    String ok = claimReturn(claimId, order, rider);
    ResponseEntity<String> drop = dropInfo(ok, rider);
    assertThat(drop.getStatusCode().value()).isEqualTo(200); // 회수지 = 고객 집(받기 전에도)
    assertThat(body(drop).path("roadAddress").asString()).isEqualTo("서울 강남구 테헤란로 1");
    assertThat(post(ok, "store-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(409); // 일반 배송 단계 없음
    // 회수지 연락처는 오배송 회수와 같은 경로 — 반품 회수는 고객 집 주소까지(2026-11-17 통합)
    JsonNode spot = body(riderGet("/deliveries/{id}/recovery-contact", ok, rider));
    assertThat(spot.path("purpose").asString()).isEqualTo("CLAIM_RETURN");
    assertThat(spot.path("roadAddress").asString()).isEqualTo("서울 강남구 테헤란로 1");
    assertThat(spot.path("receiverPhone").asString()).isEqualTo("01012345678");
    // 회수 줄에 상품명(2026-11-17 라이더 앱 — 주문 요약의 같은 주문 줄에서)
    var namesStub =
        orderApi.stubFor(
            com.github.tomakehurst.wiremock.client.WireMock.get(
                    com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching(
                        "/api/v1/internal/orders/.*/rider-summary"))
                .atPriority(1)
                .willReturn(
                    com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                            "{\"orderId\":\"x\",\"items\":[{\"orderLineId\":\"L-1\",\"name\":\"우유\",\"qty\":1},{\"orderLineId\":\"L-7\",\"name\":\"사과 1kg\",\"optionLabel\":\"3입\",\"qty\":2}]}")));
    JsonNode cv = view(ok, rider); // 회수 라이더에게 받을 품목(DD-3)
    assertThat(cv.path("returnLines").get(0).path("name").asString()).isEqualTo("사과 1kg");
    assertThat(cv.path("returnLines").get(0).path("optionLabel").asString()).isEqualTo("3입");
    orderApi.removeStub(namesStub); // 다른 시험의 주문 요약에 새지 않게
    assertThat(cv.path("claimId").asString()).isEqualTo(claimId);
    assertThat(cv.path("returnLines").get(0).path("orderLineId").asString()).isEqualTo("L-7");
    assertThat(cv.path("returnLines").get(0).path("qty").asInt()).isEqualTo(2);
    assertThat(cv.path("returnLines").get(0).path("claimLineId").asString()).isEqualTo("CL-7");
    String photo = body(upload(ok, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(
                    ok,
                    "returns/pickups",
                    rider,
                    Map.of("items", List.of(), "evidenceRefs", List.of(photo)))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(ok)).isEqualTo("RETURN_PICKED_UP");
    JsonNode picked = outbox(ok, "ReturnPickedUp").path("payload");
    assertThat(picked.path("purpose").asString()).isEqualTo("CLAIM_RETURN");
    assertThat(picked.path("claimId").asString()).isEqualTo(claimId);
    assertThat(picked.path("lines").get(0).path("orderLineId").asString()).isEqualTo("L-7");
    assertThat(picked.path("lines").get(0).path("qty").asInt()).isEqualTo(2);
    assertThat(picked.path("lines").get(0).path("claimLineId").asString()).isEqualTo("CL-7");
    assertThat(dropInfo(ok, rider).getStatusCode().value()).isEqualTo(409); // 받은 뒤에는 닫힘
    assertThat(post(ok, "returns/store-deliveries", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    JsonNode returned = outbox(ok, "ReturnDelivered").path("payload");
    assertThat(returned.path("claimId").asString()).isEqualTo(claimId);
    assertThat(returned.path("lines").get(0).path("qty").asInt()).isEqualTo(2);
    assertThat(
            jdbc.queryForObject(
                "SELECT total FROM rider_earning WHERE delivery_id = ?", Integer.class, ok))
        .isEqualTo(3900);

    // 2) 고객 부재 → 실패 등록(사진 + 연락 2회) → 확정 → ReturnPickupFailed, 수락 당시 금액 지급
    String absent = claimReturn(claimId, order, rider);
    String p2 = body(upload(absent, rider, JPEG)).path("photoRef").asString();
    ResponseEntity<String> oneCall =
        post(absent, "failures", rider, failure("CUSTOMER_ABSENT", List.of(p2), 1));
    assertThat(body(oneCall).path("code").asString()).isEqualTo("CONTACT_ATTEMPTS_REQUIRED");
    assertThat(
            post(absent, "failures", rider, failure("CUSTOMER_ABSENT", List.of(p2), 2))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(absent)).isEqualTo("FAILURE_REPORTED");
    assertThat(dropInfo(absent, rider).getStatusCode().value()).isEqualTo(200); // 실패 등록 중에도 연락
    assertThat(decide(absent, "OP-1", List.of("MFC-1"), "CONFIRM").getStatusCode().value())
        .isEqualTo(200);
    assertThat(state(absent)).isEqualTo("FAILED");
    JsonNode failed = outbox(absent, "ReturnPickupFailed").path("payload");
    assertThat(failed.path("claimId").asString()).isEqualTo(claimId);
    assertThat(failed.path("reason").asString()).isEqualTo("CUSTOMER_ABSENT");
    assertThat(failed.path("customerReason").asBoolean()).isTrue();
    assertThat(failed.path("contactAttempts").asInt()).isEqualTo(2);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryFailed'",
                Integer.class,
                absent))
        .isZero(); // 상품은 고객에게 그대로 — 전달 실패 사건이 아니다
    Map<String, Object> paid =
        jdbc.queryForMap(
            "SELECT calculable, total FROM rider_earning WHERE delivery_id = ?", absent);
    assertThat(paid.get("calculable")).isEqualTo(true);
    assertThat(paid.get("total")).isEqualTo(3900);

    // 3) 고객 사유가 아닌 실패(사고) → 산정 불가(지원센터 조정)
    String accident = claimReturn(claimId, order, rider);
    String p3 = body(upload(accident, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(accident, "failures", rider, failure("RIDER_INCIDENT", List.of(p3), 0))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(decide(accident, "OP-1", List.of("MFC-1"), "CONFIRM").getStatusCode().value())
        .isEqualTo(200);
    assertThat(
            outbox(accident, "ReturnPickupFailed")
                .path("payload")
                .path("customerReason")
                .asBoolean())
        .isFalse();
    assertThat(
            jdbc.queryForObject(
                "SELECT calculable FROM rider_earning WHERE delivery_id = ?",
                Boolean.class,
                accident))
        .isFalse();
  }

  /** 반품 회수 배송 하나를 만들고 배정한다(DeliveryCreated claimId·returnLines). */
  String claimReturn(String claimId, String order, String rider) {
    String id = "DLV-" + UUID.randomUUID();
    Map<String, Object> created = new java.util.LinkedHashMap<>();
    created.put("deliveryId", id);
    created.put("orderId", order);
    created.put("shipmentId", "CLR-" + UUID.randomUUID());
    created.put("type", "RETURN");
    created.put("purpose", "CLAIM_RETURN");
    created.put("claimId", claimId);
    created.put("mfcId", "MFC-1");
    created.put("dropAddressRef", "mfc:MFC-1");
    created.put(
        "returnLines", List.of(Map.of("orderLineId", "L-7", "qty", 2, "claimLineId", "CL-7")));
    send("delivery.delivery.v1", "DeliveryCreated", "Delivery", id, created);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    long until = System.currentTimeMillis() + 20_000;
    while (System.currentTimeMillis() < until
        && jdbc.queryForObject(
                "SELECT count(*) FROM execution_delivery WHERE delivery_id = ? AND claim_id IS NOT"
                    + " NULL AND return_lines IS NOT NULL",
                Integer.class,
                id)
            == 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return id;
  }

  /**
   * 라이더 앱 기록 화면(2026-11-17): 내 배달 내역에 주문번호·픽업지 지점 이름(배정 사건 값), 운행시간 = 배차 수락 ~ 전달 완료(사용자 결정), 남의 배송·
   * 끝나지 않은 배송은 넣지 않음, 기간 검사.
   */
  @Test
  void myDeliveriesShowOrderNumberAndOperatingTimeCountsAcceptToCompletion() {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), order, rider);
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(id, "completions", rider, completion(List.of(photo), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    // 끝나지 않은 배송(배정만)은 운행시간에 넣지 않는다
    String open = "DLV-" + UUID.randomUUID();
    pickedUp(open, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    // 수락 25분 뒤 완료로 맞춘다
    jdbc.update(
        "UPDATE execution_delivery SET assigned_at = completed_at - interval '25 minutes' WHERE"
            + " delivery_id = ?",
        id);

    JsonNode mine = body(meGet("/riders/me/deliveries", rider));
    JsonNode done = null;
    for (JsonNode n : mine.path("items")) {
      if (id.equals(n.path("deliveryId").asString())) {
        done = n;
      }
    }
    assertThat(done).isNotNull();
    assertThat(done.path("orderNumber").asString()).isEqualTo("12345678");
    assertThat(done.path("mfcName").asString()).isEqualTo("부산진구지점");
    assertThat(done.path("mfcPhone").asString()).isEqualTo("051-000-0000"); // 가게 전화(2026-11-17)

    java.time.LocalDate today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Seoul"));
    JsonNode t =
        body(meGet("/riders/me/operating-time?from=" + today.minusDays(1) + "&to=" + today, rider));
    assertThat(t.path("totalSeconds").asLong()).isEqualTo(25 * 60);
    assertThat(t.path("days")).hasSize(2);
    assertThat(t.path("days").get(1).path("date").asString()).isEqualTo(today.toString());
    JsonNode other = body(meGet("/riders/me/operating-time", "RDR-" + UUID.randomUUID()));
    assertThat(other.path("totalSeconds").asLong()).isZero(); // 남의 배송은 없다
    assertThat(other.path("from").asString()).isEqualTo(today.toString()); // 기본 오늘
    assertThat(
            meGet("/riders/me/operating-time?from=" + today + "&to=" + today.minusDays(1), rider)
                .getStatusCode()
                .value())
        .isEqualTo(400);
    assertThat(
            meGet("/riders/me/operating-time?from=" + today.minusDays(40) + "&to=" + today, rider)
                .getStatusCode()
                .value())
        .isEqualTo(400);

    // 라이더 백오피스(2026-11-17): 직원이 라이더 ID 로 같은 값을 본다, 화면용 주문번호로 배송 찾기
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    JsonNode st =
        body(
            staffGet(
                "/rider-operating-time?riderId="
                    + rider
                    + "&from="
                    + today.minusDays(1)
                    + "&to="
                    + today,
                ops));
    assertThat(st.path("totalSeconds").asLong()).isEqualTo(25 * 60);
    JsonNode se = body(staffGet("/rider-earnings?riderId=" + rider, ops));
    assertThat(se.path("items").valueStream().map(n -> n.path("deliveryId").asString()))
        .contains(id);
    assertThat(
            staffGet(
                    "/rider-earnings?riderId=" + rider,
                    TestJwt.operator("OP-SUP", List.of("MFC_SUPERVISOR"), List.of("MFC-1")))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(
            staffGet("/rider-operating-time?riderId=" + rider, TestJwt.rider(rider))
                .getStatusCode()
                .value())
        .isEqualTo(403);
    JsonNode found = body(staffGet("/deliveries?orderNumber=12345678&riderId=" + rider, ops));
    assertThat(found.path("items").get(0).path("orderNumber").asString())
        .isEqualTo("12345678"); // 목록에도(요청 #5)
    assertThat(found.path("items").valueStream().map(n -> n.path("deliveryId").asString()))
        .contains(id, open);
    assertThat(
            body(staffGet("/deliveries?orderNumber=99999999&riderId=" + rider, ops))
                .path("items")
                .size())
        .isZero();
    // 대시보드 배송 수(2026-11-17): 오늘 완료·진행 중 상태별, 센터 직원은 mfcId 필수
    JsonNode stats = body(staffGet("/delivery-stats?mfcId=MFC-1", ops));
    assertThat(stats.path("completedToday").asInt()).isPositive();
    assertThat(stats.path("inProgress").path("PICKED_UP").asInt()).isPositive(); // open
    assertThat(stats.path("inProgress").has("UNASSIGNED")).isTrue();
    assertThat(
            staffGet(
                    "/delivery-stats",
                    TestJwt.operator("OP-SUP", List.of("MFC_SUPERVISOR"), List.of("MFC-1")))
                .getStatusCode()
                .value())
        .isEqualTo(403);
  }

  /** 라이더 탈퇴를 막는 것(2026-11-17 — rider-service 가 부름): 진행 중 배송 → 완료 뒤 확정 전 수행료, 모르는 라이더는 빈 목록. */
  @Test
  void withdrawalBlockersFollowDeliveryAndEarning() {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    assertThat(blockers(rider)).containsExactly("ACTIVE_DELIVERY");
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(id, "completions", rider, completion(List.of(photo), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(blockers(rider)).containsExactly("UNCONFIRMED_EARNING"); // 완료 5시간 안
    jdbc.update(
        "UPDATE rider_earning SET settlement_state = 'CONFIRMED' WHERE delivery_id = ?", id);
    assertThat(blockers(rider)).isEmpty(); // 확정된 금액은 막지 않는다(탈퇴 뒤 지급)
    assertThat(blockers("RDR-none-" + UUID.randomUUID())).isEmpty();
  }

  /**
   * 산정 불가 수행료 조정(2026-11-17 — 사용자: 라이더 운영·관리자, 귀책에 따라 지급/미지급·지급이면 거리별(수락) 금액, 바로 확정·조정일 + 3영업일).
   */
  @Test
  void supportAdjustsNotCalculableEarningByFault() {
    String rider = "RDR-" + UUID.randomUUID();
    String paid = failedDelivery(rider);
    String withheld = failedDelivery(rider);
    assertThat(blockers(rider)).contains("EARNING_ADJUSTMENT_PENDING");
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    String cs = TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of());

    JsonNode queue = body(staffGet("/rider-earnings/adjustment-queue?riderId=" + rider, cs));
    assertThat(queue.path("items")).hasSize(2); // 상담원은 보기만
    String e1 = earningId(paid);
    String e2 = earningId(withheld);
    assertThat(adjust(e1, cs, "CUSTOMER", "고객 부재 확인", "a-cs").getStatusCode().value())
        .isEqualTo(403);
    assertThat(
            adjust(
                    e1,
                    TestJwt.operator("OP-M", List.of("MFC_SUPERVISOR"), List.of("MFC-1")),
                    "CUSTOMER",
                    "확인",
                    "a-m")
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(adjust(e1, ops, "CUSTOMER", " ", "a-0").getStatusCode().value()).isEqualTo(400);

    // 고객 귀책 → 수락 당시 금액 바로 확정, 지급 예정일 = 오늘 + 3영업일
    ResponseEntity<String> ok = adjust(e1, ops, "CUSTOMER", "고객 부재 — 연락 기록 확인", "a-1");
    assertThat(ok.getStatusCode().value()).isEqualTo(200);
    JsonNode r = body(ok);
    assertThat(r.path("decision").asString()).isEqualTo("PAY");
    assertThat(r.path("earning").path("settlementState").asString()).isEqualTo("CONFIRMED");
    assertThat(r.path("earning").path("calculable").asBoolean()).isTrue();
    assertThat(r.path("earning").path("total").asLong()).isEqualTo(3900);
    assertThat(r.path("earning").path("payoutDate").asString()).isNotBlank();
    assertThat(adjust(e1, ops, "CUSTOMER", "고객 부재 — 연락 기록 확인", "a-1").getStatusCode().value())
        .isEqualTo(200); // 같은 키 재전송
    ResponseEntity<String> twice = adjust(e1, ops, "RIDER", "다시", "a-2");
    assertThat(twice.getStatusCode().value()).isEqualTo(409);
    assertThat(body(twice).path("code").asString()).isEqualTo("NOT_ADJUSTABLE");
    JsonNode rec = outbox(paid, "RiderEarningRecorded").path("payload");
    assertThat(rec.path("adjustment").path("decision").asString()).isEqualTo("PAY");
    assertThat(rec.toString()).doesNotContain("연락 기록 확인"); // 사유는 사건에 없다

    // 라이더 귀책 → 미지급(ADJUSTED)
    JsonNode no = body(adjust(e2, ops, "RIDER", "라이더 연락 없이 이탈", "a-3"));
    assertThat(no.path("decision").asString()).isEqualTo("NO_PAY");
    assertThat(no.path("earning").path("settlementState").asString()).isEqualTo("ADJUSTED");
    assertThat(
            body(staffGet("/rider-earnings/adjustment-queue?riderId=" + rider, ops)).path("items"))
        .isEmpty();
    assertThat(blockers(rider)).doesNotContain("EARNING_ADJUSTMENT_PENDING");
    assertThat(
            jdbc.queryForObject(
                "SELECT reason FROM earning_adjustment WHERE earning_id = ?", String.class, e2))
        .isEqualTo("라이더 연락 없이 이탈");
  }

  /** 픽업 → 고객 도착 → 실패 등록 → 확정(산정 불가 수행료). */
  /** 고객 사유가 아닌 전달 실패 확정 — 수행료 산정 불가(지원센터 조정 대상). */
  String failedDelivery(String rider) {
    String id = "DLV-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(id, "failures", rider, failure("OTHER", List.of(photo), 0))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(decide(id, "OP-1", List.of("MFC-1"), "CONFIRM").getStatusCode().value())
        .isEqualTo(200);
    return id;
  }

  String earningId(String deliveryId) {
    return jdbc.queryForObject(
        "SELECT earning_id FROM rider_earning WHERE delivery_id = ?", String.class, deliveryId);
  }

  ResponseEntity<String> adjust(
      String earningId, String token, String fault, String reason, String key) {
    return http.post()
        .uri("/rider-earnings/{id}/adjustments", earningId)
        .headers(
            h -> {
              h.setBearerAuth(token);
              h.set("Idempotency-Key", key);
              h.set("X-Source-System", "backoffice");
            })
        .body(Map.of("fault", fault, "reason", reason))
        .retrieve()
        .toEntity(String.class);
  }

  /**
   * 전달 불가 = 지원센터 대화(2026-11-17 라이더 앱 요청 §13 — 사용자 "실패 등록이 대화도 열기"): 실패 등록이 같은 ID 의 도움요청을 열고(응답
   * resultId), 라이더 전달 재개 요청은 실패 기록·대화·직원 목록에 남는다. 반려하면 원래 단계로.
   */
  @Test
  void failureOpensHelpChatAndRiderCanAskToResume() throws Exception {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    ResponseEntity<String> reported =
        post(id, "failures", rider, failure("CUSTOMER_ABSENT", List.of(photo), 2));
    assertThat(reported.getStatusCode().value()).isEqualTo(200);
    String helpId = body(reported).path("resultId").asString();
    assertThat(helpId).isNotBlank();

    // 같은 ID 의 도움요청(대화) — 라이더가 바로 메시지를 쓸 수 있다, 사건함 사건은 하나
    JsonNode help = body(riderGet("/deliveries/{id}/help-requests/" + helpId, id, rider));
    assertThat(help.path("category").asString()).isEqualTo("DELIVERY_FAILURE");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type ="
                    + " 'DeliveryIncidentRaised'",
                Integer.class,
                id))
        .isEqualTo(1);
    // 도움요청으로 DELIVERY_FAILURE 를 만들 수는 없다
    assertThat(
            post(id, "help-requests", rider, Map.of("category", "DELIVERY_FAILURE", "detail", "x"))
                .getStatusCode()
                .value())
        .isEqualTo(400);

    // 전달 재개 요청 → 실패 기록·대화·직원 목록
    assertThat(
            post(id, "failures/resume-requests", rider, Map.of("note", "고객이 집에 왔어요"))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(post(id, "failures/resume-requests", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200); // 이미 요청 — 그대로
    JsonNode v = view(id, rider);
    assertThat(v.path("failure").path("helpRequestId").asString()).isEqualTo(helpId);
    assertThat(v.path("failure").path("resumeRequestedAt").asString()).isNotBlank();
    JsonNode thread =
        body(riderGet("/deliveries/{id}/help-requests/" + helpId + "/messages", id, rider));
    assertThat(thread.path("items").get(0).path("body").asString())
        .startsWith("[전달 재개 요청]")
        .contains("고객이 집에 왔어요");
    assertThat(thread.path("items")).hasSize(1);
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    JsonNode queue = body(staffGet("/deliveries?state=FAILURE_REPORTED&riderId=" + rider, ops));
    assertThat(queue.path("items").get(0).path("resumeRequestedAt").asString()).isNotBlank();

    // 2026-11-19: 결정 대기 중에는 처리함으로 닫지 못한다
    ResponseEntity<String> close =
        staffPost(
            "/deliveries/" + id + "/help-requests/" + helpId + "/resolutions",
            TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of()),
            Map.of("note", "처리"));
    assertThat(close.getBody()).contains("FAILURE_DECISION_PENDING");
    // 사건함이 닫아 버린 경우(CaseClosed 흉내) → 라이더가 쓰면 다시 열림 + HelpRequestReopened
    jdbc.update(
        "UPDATE help_request SET state = 'RESOLVED', resolved_at = now(), resolved_by = 'case:x'"
            + " WHERE delivery_id = ? AND request_id = ?",
        id,
        helpId);
    assertThat(
            post(id, "help-requests/" + helpId + "/messages", rider, Map.of("body", "고객이랑 연락됐어요"))
                .getStatusCode()
                .value())
        .isEqualTo(201);
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM help_request WHERE delivery_id = ? AND request_id = ?",
                String.class,
                id,
                helpId))
        .isEqualTo("OPEN");
    JsonNode reopened = outbox(id, "HelpRequestReopened").path("payload");
    assertThat(reopened.path("incidentId").asString()).isEqualTo(id + ":" + helpId);
    assertThat(reopened.toString()).doesNotContain("연락됐어요");
    // 또 닫혀도 재개 요청이 다시 연다(이미 요청했어도)
    jdbc.update(
        "UPDATE help_request SET state = 'RESOLVED' WHERE delivery_id = ? AND request_id = ?",
        id,
        helpId);
    assertThat(post(id, "failures/resume-requests", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM help_request WHERE delivery_id = ? AND request_id = ?",
                String.class,
                id,
                helpId))
        .isEqualTo("OPEN");

    // 지원센터가 반려 → 원래 단계, 재개 요청은 실패 등록 중에만
    assertThat(decide(id, "OP-1", List.of("MFC-1"), "REJECT").getStatusCode().value())
        .isEqualTo(200);
    assertThat(state(id)).isEqualTo("PICKED_UP");
    assertThat(post(id, "failures/resume-requests", rider, Map.of()).getStatusCode().value())
        .isEqualTo(409);
  }

  /**
   * 사건함 처리완료 = 실패 확정(2026-11-20 사용자 "지원센터에서 처리완료하면 아예 배차 취소되게 하고, 회수배차 바로"): 결정 대기 전달 불가가 연 사건이 닫히면
   * FAILED + DeliveryFailed(상품 위치 = 라이더 등록값, 같은 라이더 반납). 그 뒤 재개 요청은 409.
   */
  @Test
  void caseClosedConfirmsPendingFailure() throws Exception {
    String id = "DLV-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    ResponseEntity<String> reported =
        post(id, "failures", rider, failure("CUSTOMER_ABSENT", List.of(photo), 2));
    assertThat(reported.getStatusCode().value()).isEqualTo(200);
    String helpId = body(reported).path("resultId").asString();
    String ops = TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of());
    String caseId = "CASE-" + UUID.randomUUID();
    caseEvent(
        "CaseOpened",
        caseId,
        1,
        Map.of(
            "caseId",
            caseId,
            "type",
            "DELIVERY_INCIDENT",
            "refs",
            List.of(Map.of("refType", "incidentId", "refId", id + ":" + helpId)),
            "openedAt",
            Instant.now().toString()));
    for (int i = 0; i < 100; i++) {
      JsonNode h = body(staffGet("/deliveries/" + id + "/help-requests/" + helpId, ops));
      if (caseId.equals(h.path("caseId").asString())) {
        break;
      }
      Thread.sleep(200);
    }
    caseEvent(
        "CaseClosed",
        caseId,
        2,
        Map.of(
            "caseId",
            caseId,
            "type",
            "DELIVERY_INCIDENT",
            "outcome",
            "RESOLVED",
            "closedAt",
            Instant.now().toString()));
    awaitHelpState(id, helpId, ops, "RESOLVED");
    assertThat(state(id)).isEqualTo("FAILED");
    JsonNode failed = outbox(id, "DeliveryFailed").path("payload");
    assertThat(failed.path("reason").asString()).isEqualTo("CUSTOMER_ABSENT");
    assertThat(failed.path("returnBy").asString()).isEqualTo("SAME_RIDER");
    JsonNode v = view(id, rider);
    assertThat(v.path("failure").path("decision").asString()).isEqualTo("CONFIRM");
    assertThat(post(id, "failures/resume-requests", rider, Map.of()).getStatusCode().value())
        .isEqualTo(409);
    // 다시 받아도(재전달) 두 번 확정하지 않는다
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryFailed'",
                Integer.class,
                id))
        .isEqualTo(1);
  }

  List<String> blockers(String rider) {
    JsonNode b =
        body(
            http.get()
                .uri("/internal/riders/{id}/withdrawal-blockers", rider)
                .retrieve()
                .toEntity(String.class));
    List<String> out = new java.util.ArrayList<>();
    b.path("blockers").forEach(n -> out.add(n.asString()));
    return out;
  }

  ResponseEntity<String> meGet(String uri, String rider) {
    return http.get()
        .uri(uri)
        .header("Authorization", "Bearer " + TestJwt.rider(rider))
        .retrieve()
        .toEntity(String.class);
  }

  ResponseEntity<String> riderGet(String uri, String id, String rider) {
    return http.get()
        .uri(uri, id)
        .header("Authorization", "Bearer " + TestJwt.rider(rider))
        .retrieve()
        .toEntity(String.class);
  }

  ResponseEntity<String> customerGet(String uri, String account) {
    return http.get()
        .uri(uri)
        .header("Authorization", "Bearer " + TestJwt.customer(account))
        .retrieve()
        .toEntity(String.class);
  }

  ResponseEntity<byte[]> photo(String id, String photoRef, String token) {
    return http.get()
        .uri("/deliveries/{id}/evidence-photos/{ref}", id, photoRef)
        .header("Authorization", "Bearer " + token)
        .retrieve()
        .toEntity(byte[].class);
  }

  /** 라이더 귀책 오배송(ADR-0065): 원래 수행료 지급 전 → ADJUSTED·산정 제외, 이미 지급 → 메모만. 시스템 귀책은 그대로. */
  @Test
  void riderFaultMisdeliveryWithholdsOriginalFee() {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    pickedUp(id, "SHP-" + UUID.randomUUID(), order, rider);
    assertThat(post(id, "customer-arrivals", rider, Map.of()).getStatusCode().value())
        .isEqualTo(200);
    String photo = body(upload(id, rider, JPEG)).path("photoRef").asString();
    assertThat(
            post(id, "completions", rider, completion(List.of(photo), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    caseEvent(id, "SYSTEM");
    caseEvent(id, "RIDER");
    awaitSql(
        "SELECT count(*) FROM rider_earning WHERE delivery_id = '"
            + id
            + "' AND settlement_state = 'ADJUSTED'");
    Map<String, Object> e =
        jdbc.queryForMap("SELECT calculable, note FROM rider_earning WHERE delivery_id = ?", id);
    assertThat(e.get("calculable")).isEqualTo(false);
    assertThat((String) e.get("note")).contains("배달료 미지급");

    // 이미 지급된 수행료는 상태 그대로, 메모만
    String id2 = "DLV-" + UUID.randomUUID();
    String rider2 = "RDR-" + UUID.randomUUID();
    pickedUp(id2, "SHP-" + UUID.randomUUID(), "ORD-" + UUID.randomUUID(), rider2);
    post(id2, "customer-arrivals", rider2, Map.of());
    String photo2 = body(upload(id2, rider2, JPEG)).path("photoRef").asString();
    post(id2, "completions", rider2, completion(List.of(photo2), Instant.now().toString()));
    jdbc.update("UPDATE rider_earning SET settlement_state = 'PAID' WHERE delivery_id = ?", id2);
    caseEvent(id2, "RIDER");
    awaitSql(
        "SELECT count(*) FROM rider_earning WHERE delivery_id = '"
            + id2
            + "' AND note LIKE '%회수 필요%'");
    assertThat(
            jdbc.queryForObject(
                "SELECT settlement_state FROM rider_earning WHERE delivery_id = ?",
                String.class,
                id2))
        .isEqualTo("PAID");
  }

  /**
   * ADR-0065 고객 잘못 다시 배송: 잘못 둔 곳에서 사진·받음 → PICKED_UP → 고객 도착 → 완료(DeliveryCompleted purpose, 원래 줄).
   * 가게 도착은 409, 전달지 = 실제 집 주소 + 주문 당시 연락처, 원래 기사는 배달료 0.
   */
  @Test
  void customerFaultRedeliveryPicksUpAtSpotAndDeliversToRealHome() {
    String origin = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String r1 = "RDR-" + UUID.randomUUID();
    pickedUp(origin, "SHP-" + UUID.randomUUID(), order, r1);
    post(origin, "customer-arrivals", r1, Map.of());
    String originPhoto = body(upload(origin, r1, JPEG)).path("photoRef").asString();
    post(origin, "completions", r1, completion(List.of(originPhoto), Instant.now().toString()));

    String rd = "DLV-" + UUID.randomUUID();
    Map<String, Object> created = new java.util.LinkedHashMap<>();
    created.put("deliveryId", rd);
    created.put("orderId", order);
    created.put("shipmentId", "RDL-CASE-1");
    created.put("type", "RESHIPMENT");
    created.put("purpose", "MISDELIVERY_REDELIVERY");
    created.put("caseId", "CASE-RD");
    created.put("originDeliveryId", origin);
    created.put("mfcId", "MFC-1");
    created.put("dropAddressRef", "order:" + order + ":redelivery:RDD-1");
    send("delivery.delivery.v1", "DeliveryCreated", "Delivery", rd, created);
    send(
        "delivery.delivery.v1",
        "DeliveryAssigned",
        "Delivery",
        rd,
        Map.of(
            "deliveryId",
            rd,
            "orderId",
            order,
            "riderId",
            r1,
            "assignmentVersion",
            1,
            "acceptedAt",
            Instant.now().toString(),
            "fee",
            0,
            "vehicleType",
            "MOTORCYCLE",
            "distanceM",
            120));
    awaitState(rd, "ASSIGNED");
    assertThat(
            jdbc.queryForObject(
                "SELECT redelivery_destination_id FROM execution_delivery WHERE delivery_id = ?",
                String.class,
                rd))
        .isEqualTo("RDD-1");
    assertThat(post(rd, "store-arrivals", r1, Map.of()).getStatusCode().value()).isEqualTo(409);
    // 잘못 둔 곳에서 사진(배정 상태) → 받음 → PICKED_UP
    String spot = body(upload(rd, r1, JPEG)).path("photoRef").asString();
    assertThat(
            post(
                    rd,
                    "returns/pickups",
                    r1,
                    Map.of("items", List.of(), "evidenceRefs", List.of(spot)))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(rd)).isEqualTo("PICKED_UP");
    assertThat(outbox(rd, "ReturnPickedUp").path("payload").path("purpose").asString())
        .isEqualTo("MISDELIVERY_REDELIVERY");
    // 전달지: 실제 집 + 주문 당시 연락처
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/drop-info"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        mapper.writeValueAsString(
                            Map.of(
                                "orderId",
                                order,
                                "roadAddress",
                                "잘못 입력한 주소",
                                "detailAddress",
                                "101동",
                                "receiverName",
                                "김고객",
                                "receiverPhone",
                                "01012345678")))));
    orderApi.stubFor(
        com.github.tomakehurst.wiremock.client.WireMock.get(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo(
                    "/api/v1/internal/orders/" + order + "/redelivery-destinations/RDD-1"))
            .willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        mapper.writeValueAsString(
                            Map.of(
                                "destinationId",
                                "RDD-1",
                                "orderId",
                                order,
                                "deliveryId",
                                origin,
                                "roadAddress",
                                "실제 집 주소",
                                "detailAddress",
                                "202동 1501호")))));
    JsonNode drop = body(dropInfo(rd, r1));
    assertThat(drop.path("roadAddress").asString()).isEqualTo("실제 집 주소");
    assertThat(drop.path("detailAddress").asString()).isEqualTo("202동 1501호");
    assertThat(drop.path("receiverPhone").asString()).isEqualTo("01012345678");
    // 고객 도착 → 사진 → 완료
    assertThat(post(rd, "customer-arrivals", r1, Map.of()).getStatusCode().value()).isEqualTo(200);
    String door = body(upload(rd, r1, JPEG)).path("photoRef").asString();
    assertThat(
            post(rd, "completions", r1, completion(List.of(door), Instant.now().toString()))
                .getStatusCode()
                .value())
        .isEqualTo(200);
    assertThat(state(rd)).isEqualTo("COMPLETED");
    JsonNode done = outbox(rd, "DeliveryCompleted").path("payload");
    assertThat(done.path("purpose").asString()).isEqualTo("MISDELIVERY_REDELIVERY");
    assertThat(done.path("lines").get(0).path("orderLineId").asString()).isEqualTo("L-1");
    assertThat(
            jdbc.queryForObject(
                "SELECT total FROM rider_earning WHERE delivery_id = ?", Integer.class, rd))
        .isZero(); // 원래 기사 추가 운행료 없음
  }

  void caseEvent(String deliveryId, String fault) {
    String caseId = "CASE-" + UUID.randomUUID();
    String json =
        EventEnvelopeFixtures.envelope("MisdeliveryConfirmed")
            .from("case-service")
            .aggregate("Case", caseId, 1)
            .payload(
                Map.of(
                    "caseId",
                    caseId,
                    "deliveryId",
                    deliveryId,
                    "confirmedBy",
                    "OP-CS",
                    "confirmedAt",
                    Instant.now().toString(),
                    "fault",
                    fault,
                    "resolution",
                    "RESHIP"))
            .buildJson();
    kafka.send("platform.case.v1", caseId, json).join();
  }

  /**
   * 오배송 재발송 배송(2026-11-16): type RESHIPMENT·purpose MISDELIVERY_RESHIPMENT — 수행은 일반 배송과 같다(고객 전달지
   * 열림).
   */
  @Test
  void reshipmentDeliveryIsExecutedLikeForward() {
    String id = "DLV-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    Map<String, Object> created = new java.util.LinkedHashMap<>();
    created.put("deliveryId", id);
    created.put("orderId", order);
    created.put("shipmentId", order + "-RS-1a2b3c4d");
    created.put("type", "RESHIPMENT");
    created.put("purpose", "MISDELIVERY_RESHIPMENT");
    created.put("caseId", "CASE-1");
    created.put("mfcId", "MFC-1");
    created.put("dropAddressRef", "order:" + order);
    send("delivery.delivery.v1", "DeliveryCreated", "Delivery", id, created);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    JsonNode v = view(id, rider);
    assertThat(v.path("type").asString()).isEqualTo("RESHIPMENT");
    assertThat(v.path("purpose").asString()).isEqualTo("MISDELIVERY_RESHIPMENT");
    assertThat(post(id, "store-arrivals", rider, Map.of()).getStatusCode().value()).isEqualTo(200);
  }

  // ---- 도우미

  void created(String id, String shipment, String order) {
    send(
        "delivery.delivery.v1",
        "DeliveryCreated",
        "Delivery",
        id,
        Map.of(
            "deliveryId",
            id,
            "orderId",
            order,
            "shipmentId",
            shipment,
            "type",
            "FORWARD",
            "mfcId",
            "MFC-1",
            "dropAddressRef",
            "order:" + order));
  }

  void assigned(String id, String order, String rider, int version) {
    Map<String, Object> p = new java.util.LinkedHashMap<>();
    p.put("deliveryId", id);
    p.put("orderId", order);
    p.put("riderId", rider);
    p.put("assignmentVersion", version);
    p.put("acceptedAt", Instant.now().toString());
    p.put("fee", 3900);
    p.put("vehicleType", "MOTORCYCLE");
    p.put("distanceM", 1800);
    p.put("orderNumber", "12345678");
    p.put("mfcName", "부산진구지점");
    p.put("mfcPhone", "051-000-0000");
    send("delivery.delivery.v1", "DeliveryAssigned", "Delivery", id, p);
  }

  /**
   * 재포장 무효(2026-11-20, 주문 23631583): ShipmentReady 뒤 옛 포장이 무효(PackageInvalidated, 두 번 재포장)되면 포장 목록은
   * 새 포장으로 — 새 포장만 인계해도 픽업 완료. 무효 사건이 목록보다 먼저 와도 같다.
   */
  @Test
  void invalidatedPackageIsReplacedForPickup() throws Exception {
    String id = "DLV-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String rider = "RDR-" + UUID.randomUUID();
    String p1 = "PKG-1-" + id;
    String p2 = "PKG-2-" + id;
    String p3 = "PKG-3-" + id;
    String p4 = "PKG-4-" + id;
    created(id, shipment, order);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    shipmentReady(shipment, order, List.of(p1, p2));
    invalidated(p1, shipment, order, p3);
    invalidated(p3, shipment, order, p4);
    handedOver(id, shipment, order, rider, p2, "HO-2-" + id, Map.of("L-1", 1));
    handedOver(id, shipment, order, rider, p4, "HO-4-" + id, Map.of("L-2", 1));
    awaitState(id, "PICKED_UP");
    List<String> pkgs = new java.util.ArrayList<>();
    view(id, rider).path("packageIds").forEach(n -> pkgs.add(n.asString()));
    assertThat(pkgs).containsExactlyInAnyOrder(p2, p4);

    // 무효 사건이 목록보다 먼저 와도
    String id2 = "DLV-" + UUID.randomUUID();
    String shipment2 = "SHP-" + UUID.randomUUID();
    String q1 = "PKG-1-" + id2;
    String q2 = "PKG-2-" + id2;
    created(id2, shipment2, order);
    assigned(id2, order, rider, 1);
    awaitState(id2, "ASSIGNED");
    invalidated(q1, shipment2, order, q2);
    handedOver(id2, shipment2, order, rider, q2, "HO-q2-" + id2, Map.of("L-1", 1));
    shipmentReady(shipment2, order, List.of(q1));
    awaitState(id2, "PICKED_UP");
  }

  void shipmentReady(String shipment, String order, List<String> packageIds) {
    send(
        "mfc.fulfillment.v1",
        "ShipmentReady",
        "Shipment",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            "MFC-1",
            "readyAt",
            Instant.now().toString(),
            "packages",
            packageIds.stream().map(p -> Map.of("packageId", p)).toList()));
  }

  void invalidated(String pkg, String shipment, String order, String newPkg) {
    send(
        "mfc.fulfillment.v1",
        "PackageInvalidated",
        "Package",
        pkg,
        Map.of(
            "packageId",
            pkg,
            "newPackageId",
            newPkg,
            "shipmentId",
            shipment,
            "orderId",
            order,
            "mfcId",
            "MFC-1",
            "reason",
            "REPACK",
            "invalidatedAt",
            Instant.now().toString()));
  }

  /** 배정 → 목록(포장 1개) → 인계 → PICKED_UP 까지. */
  void pickedUp(String id, String shipment, String order, String rider) {
    created(id, shipment, order);
    assigned(id, order, rider, 1);
    awaitState(id, "ASSIGNED");
    send(
        "mfc.fulfillment.v1",
        "ShipmentReady",
        "Shipment",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            "MFC-1",
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(Map.of("packageId", "PKG-" + id))));
    handedOver(id, shipment, order, rider, "PKG-" + id, "HO-" + id, Map.of("L-1", 1));
    awaitState(id, "PICKED_UP");
  }

  static Map<String, Object> failure(String reason, List<String> photos, int contacts) {
    List<Map<String, Object>> attempts = new java.util.ArrayList<>();
    for (int i = 0; i < contacts; i++) {
      attempts.add(
          Map.of(
              "attemptedAt",
              Instant.now().minusSeconds(60L * (i + 1)).toString(),
              "method",
              i % 2 == 0 ? "PHONE_CALL" : "SMS", // 고객 전화번호 그대로(0.11.0)
              "result",
              "NO_ANSWER"));
    }
    return Map.of(
        "reason",
        reason,
        "evidenceRefs",
        photos,
        "goodsLocation",
        "WITH_RIDER",
        "contactAttempts",
        attempts);
  }

  ResponseEntity<String> decide(String id, String operator, List<String> centers, String decision) {
    return decide(id, operator, List.of("RIDER_OPS"), centers, decision); // 2026-11-19 라이더 운영만
  }

  ResponseEntity<String> decide(
      String id, String operator, List<String> roles, List<String> centers, String decision) {
    return http.post()
        .uri("/deliveries/{id}/failure-decisions", id)
        .headers(
            h -> {
              h.setBearerAuth(TestJwt.operator(operator, roles, centers));
              h.set("Idempotency-Key", UUID.randomUUID().toString());
              h.set("X-Source-System", "mfc-backoffice");
            })
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("decision", decision, "note", "IT"))
        .retrieve()
        .toEntity(String.class);
  }

  void handedOver(
      String id,
      String shipment,
      String order,
      String rider,
      String pkg,
      String handoverId,
      Map<String, Integer> lines) {
    send(
        "mfc.fulfillment.v1",
        "PackageHandedOver",
        "Package",
        pkg,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "deliveryId",
            id,
            "packageId",
            pkg,
            "riderId",
            rider,
            "handoverId",
            handoverId,
            "inventoryCommitId",
            "IC-" + handoverId,
            "lines",
            lines.entrySet().stream()
                .map(
                    e ->
                        Map.of("orderLineId", e.getKey(), "sku", "E-00000001", "qty", e.getValue()))
                .toList(),
            "handedAt",
            Instant.now().toString()));
  }

  void send(String topic, String type, String aggregateType, String key, Map<String, Object> p) {
    String json =
        EventEnvelopeFixtures.envelope(type)
            .from(topic.startsWith("mfc") ? "mfc-fulfillment-service" : "delivery-dispatch-service")
            .aggregate(aggregateType, key, 1)
            .payload(p)
            .buildJson();
    kafka.send(topic, key, json).join();
  }

  ResponseEntity<String> post(String id, String action, String rider, Object body) {
    return http.post()
        .uri("/deliveries/{id}/" + action, id)
        .headers(h -> riderHeaders(h, rider))
        .contentType(MediaType.APPLICATION_JSON)
        .body(body)
        .retrieve()
        .toEntity(String.class);
  }

  ResponseEntity<String> upload(String id, String rider, byte[] bytes) {
    return upload(id, rider, bytes, UUID.randomUUID().toString());
  }

  ResponseEntity<String> upload(String id, String rider, byte[] bytes, String idempotencyKey) {
    LinkedMultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
    form.add(
        "file",
        new ByteArrayResource(bytes) {
          @Override
          public String getFilename() {
            return "photo.jpg";
          }
        });
    return http.post()
        .uri("/deliveries/{id}/evidence-photos", id)
        .headers(
            h -> {
              riderHeaders(h, rider);
              h.set("Idempotency-Key", idempotencyKey);
            })
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(form)
        .retrieve()
        .toEntity(String.class);
  }

  static Map<String, Object> completion(List<String> photoRefs, String takenAt) {
    return Map.of(
        "method",
        "CONTACTLESS",
        "evidence",
        Map.of("photoRefs", photoRefs, "takenAt", takenAt, "placement", "FRONT_DOOR"));
  }

  static void riderHeaders(HttpHeaders h, String rider) {
    h.setBearerAuth(TestJwt.rider(rider));
    h.set("Idempotency-Key", UUID.randomUUID().toString());
    h.set("X-Source-System", "rider-app");
  }

  String state(String id) {
    return jdbc.queryForObject(
        "SELECT state FROM execution_delivery WHERE delivery_id = ?", String.class, id);
  }

  void awaitState(String id, String state) {
    awaitSql(
        "SELECT count(*) FROM execution_delivery WHERE delivery_id = '"
            + id
            + "' AND state = '"
            + state
            + "'");
  }

  void awaitSql(String countSql) {
    long until = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < until) {
      Integer n = jdbc.queryForObject(countSql, Integer.class);
      if (n != null && n > 0) {
        return;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    throw new AssertionError("기다린 상태가 오지 않음: " + countSql);
  }

  JsonNode outbox(String id, String type) {
    String payload =
        jdbc.queryForObject(
            "SELECT payload::text FROM outbox_event WHERE aggregateid = ? AND type = ? ORDER BY created_at DESC LIMIT 1",
            String.class,
            id,
            type);
    return mapper.readTree(payload);
  }

  JsonNode body(ResponseEntity<String> r) {
    return mapper.readTree(r.getBody() == null ? "{}" : r.getBody());
  }
}
