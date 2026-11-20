package com.pmart.delivery.dispatch;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.pmart.common.test.EventEnvelopeFixtures;
import com.pmart.common.test.PostgresKafkaContainerSupport;
import com.pmart.common.test.TestJwt;
import com.pmart.common.test.TestJwtConfiguration;
import com.pmart.delivery.dispatch.application.DispatchEngine;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestClient;
import org.wiremock.spring.ConfigureWireMock;
import org.wiremock.spring.EnableWireMock;
import org.wiremock.spring.InjectWireMock;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * DD-1 첫 묶음: MFC 사건(mfc.fulfillment.v1) → 배송 생성(DeliveryCreated) → AI 추천 제안(OR-Tools·카카오 도로 시간) →
 * 수락 (DeliveryAssigned) → 인계(PICKED_UP). 일반 배차모드 배달리스트·반경 단계, 거절 → 재요청 → 실패, 작업 중지 → 픽업 전 취소.
 * rider·tracking· order·center·serviceability 는 WireMock 하나. 판단 주기는 시험이 직접 부른다(스케줄 PT1H).
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.main.banner-mode=off",
      "spring.security.oauth2.resourceserver.jwt.jwk-set-uri=http://localhost:1/jwks",
      "pmart.messaging.outbox.delete-after-insert=false",
      "pmart.dispatch.tick-interval=PT1H",
      "pmart.dispatch.list-notice-interval=PT1H", // 일반 모드 새 배달 알림도 시험이 직접 부른다
      "pmart.dispatch.tick-initial-delay=PT1H",
      "pmart.dispatch.operating-start=00:00",
      "pmart.dispatch.operating-end=24:00"
    })
@Import(TestJwtConfiguration.class)
@EnableWireMock(
    @ConfigureWireMock(
        name = "up",
        baseUrlProperties = {
          "pmart.dispatch.urls.rider",
          "pmart.dispatch.urls.tracking",
          "pmart.dispatch.urls.order",
          "pmart.dispatch.urls.center-master",
          "pmart.dispatch.urls.serviceability"
        }))
class DispatchFlowIT extends PostgresKafkaContainerSupport {

  static final String MFC_POINT = "37.4979,127.0276";

  @LocalServerPort int port;
  @Autowired ObjectMapper mapper;
  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired DispatchEngine engine;
  @Autowired DispatchStore store;

  @InjectWireMock("up")
  WireMockServer up;

  RestClient http;

  @BeforeEach
  void setUp() {
    http =
        RestClient.builder()
            .baseUrl("http://localhost:" + port + "/api/v1")
            .defaultStatusHandler(HttpStatusCode::isError, (q, r) -> {})
            .build();
    up.resetAll();
    up.stubFor(
        get(urlPathMatching("/api/v1/internal/centers/.*"))
            .willReturn(
                json(
                    Map.of(
                        "coordinates", Map.of("lat", 37.4979, "lng", 127.0276),
                        "name", "부산진구지점",
                        "address", "부산 부산진구 동천로 109",
                        "phone", "051-000-0000"))));
    // 라이더용 주문 요약(라이더 앱 요청 §1)
    up.stubFor(
        get(urlPathMatching("/api/v1/internal/orders/.*/rider-summary"))
            .willReturn(
                json(
                    Map.of(
                        "orderId",
                        "x",
                        "orderNumber",
                        "48213907",
                        "items",
                        List.of(Map.of("name", "사과 1kg", "qty", 2, "lineAmount", 9800)),
                        "itemAmount",
                        9800,
                        "deliveryNote",
                        "문 앞에 두세요"))));
    up.stubFor(
        get(urlPathMatching("/api/v1/internal/orders/.*/delivery-destination"))
            .willReturn(
                json(
                    Map.of(
                        "orderId",
                        "x",
                        "mfcId",
                        "x",
                        "status",
                        "CONFIRMED",
                        "deliveryMode",
                        "SINGLE",
                        "roadAddress",
                        "IT-ROAD"))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/address-geocodes"))
            .willReturn(
                json(
                    Map.of(
                        "results",
                        List.of(
                            Map.of(
                                "lat",
                                37.5006,
                                "lng",
                                127.0360,
                                "jibunAddress",
                                "서울 서초구 서초동 1-2"))))));
    // MFC → 고객(1,800m·400초), 라이더 → MFC(행마다 300초)
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.origins[0].lat", equalTo("37.4979")))
            .withRequestBody(matchingJsonPath("$.origins[0].lng", equalTo("127.0276")))
            .willReturn(json(matrix(List.of(List.of(1800, 400))))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(5)
            .willReturn(
                json(matrix(List.of(List.of(900, 300), List.of(900, 300), List.of(900, 300))))));
  }

  /** 직원 조치 이력(2026-11-18, 관리자만): 운영자 배차 취소 기록이 보인다, 라이더 운영은 403. */
  @Test
  void staffActionsListOperatorCancellations() {
    String d = "DLV-sa-" + UUID.randomUUID();
    jdbc.update(
        "INSERT INTO assignment_cancellation (request_id, delivery_id, requested_by, actor, reason,"
            + " reason_detail, reassign, state, previous_rider_id, created_at) VALUES (?, ?, 'OPERATOR',"
            + " 'OP-X', 'OPERATOR', '라이더 연락 안 됨', true, 'CANCELLED_BEFORE_PICKUP', 'RD-sa', now())",
        "req-" + d,
        d);
    JsonNode acts =
        body(
            http.get()
                .uri("/dispatch-staff-actions?deliveryId={d}", d)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of()))
                .retrieve()
                .toEntity(String.class));
    assertThat(acts.path("items").get(0).path("action").asString())
        .isEqualTo("ASSIGNMENT_CANCELLED");
    assertThat(acts.path("items").get(0).path("actorId").asString()).isEqualTo("OP-X");
    assertThat(acts.path("items").get(0).path("riderId").asString()).isEqualTo("RD-sa");
    assertThat(
            http.get()
                .uri("/dispatch-staff-actions")
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of()))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
  }

  @Test
  void aiOfferAcceptAndHandover() {
    String mfc = "MFC-D1";
    String rider = "RD-a-" + UUID.randomUUID();
    String other = "RD-o-" + UUID.randomUUID();
    riders(mfc, rider(rider, "AI_RECOMMENDED", true));
    riderLookup(rider(rider, "AI_RECOMMENDED", true), rider(other, "NORMAL", true));
    locations(
        Map.of(rider, new double[] {37.5010, 127.0280}, other, new double[] {37.5010, 127.0280}));

    String shipment = "SHP-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    send(
        "DispatchRequested",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "trigger",
            "MANUAL",
            "estimatedPackMinutes",
            5,
            "estimatedReadyAt",
            Instant.now().plusSeconds(300).toString(),
            "requestedAt",
            Instant.now().toString()));
    String deliveryId = awaitDelivery(shipment);
    send(
        "ShipmentReady",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(
                Map.of(
                    "packageId",
                    "PKG-1-" + shipment,
                    "bagCount",
                    2,
                    "weightG",
                    3200,
                    "alcohol",
                    false))));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND ready_at IS NOT NULL");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE shipment_id = ?", Integer.class, shipment))
        .isEqualTo(1);
    JsonNode created = outbox(deliveryId, "DeliveryCreated");
    assertThat(created.path("payload").path("shipmentId").asString()).isEqualTo(shipment);
    assertThat(created.path("payload").path("deliverySeq").asInt())
        .isEqualTo(1); // 화면 배송 번호 순번(ADR-0070)
    assertThat(
            body(http.get()
                    .uri("/internal/deliveries/{id}/sequence", deliveryId)
                    .header("X-Source-System", "delivery-execution-service")
                    .retrieve()
                    .toEntity(String.class))
                .path("deliverySeq")
                .asInt())
        .isEqualTo(1);
    assertThat(created.path("payload").path("dropAddressRef").asString())
        .isEqualTo("order:" + order);

    engine.tick();

    JsonNode sent = outbox(deliveryId, "DispatchOfferSent");
    assertThat(sent.path("payload").path("riderId").asString()).isEqualTo(rider);
    assertThat(sent.path("payload").path("fee").asInt()).isEqualTo(3200); // 원문 §7.1 1,350~2,000m

    JsonNode mine =
        body(
            http.get()
                .uri("/offers")
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    assertThat(mine.path("items")).hasSize(1);
    JsonNode offer = mine.path("items").get(0);
    String offerId = offer.path("offerId").asString();
    assertThat(offer.path("summary").path("dropAreaLabel").asString()).isEqualTo("서초구 서초동");
    assertThat(offer.path("summary").path("distanceKm").asDouble()).isEqualTo(1.8);
    assertThat(offer.path("summary").path("pickupEtaSeconds").asInt()).isEqualTo(300);
    // 라이더 앱 요청 §3(2026-11-16): 픽업 → 전달 소요, 픽업지(MFC 정확)·전달지(약 1km 격자로 반올림)
    assertThat(offer.path("summary").path("dropEtaSeconds").asInt()).isPositive();
    assertThat(offer.path("summary").path("pickupPoint").path("lat").isNumber()).isTrue();
    double dropLat = offer.path("summary").path("dropAreaPoint").path("lat").asDouble();
    assertThat(dropLat * 100)
        .isCloseTo(Math.round(dropLat * 100), org.assertj.core.data.Offset.offset(1e-6));
    // 라이더 앱 요청 §1(2026-11-16 "수락 전부터 전부"): 지점·품목·금액·요청사항
    JsonNode orderInfo = offer.path("summary").path("order");
    assertThat(orderInfo.path("mfcName").asString()).isEqualTo("부산진구지점");
    assertThat(orderInfo.path("mfcPhone").asString())
        .isEqualTo("051-000-0000"); // 가게 전화(2026-11-17)
    assertThat(orderInfo.path("orderNumber").asString()).isEqualTo("48213907");
    assertThat(orderInfo.path("items").get(0).path("name").asString()).isEqualTo("사과 1kg");
    assertThat(orderInfo.path("itemAmount").asInt()).isEqualTo(9800);
    assertThat(orderInfo.path("deliveryNote").asString()).isEqualTo("문 앞에 두세요");
    assertThat(offer.path("summary").path("bagCount").asInt()).isEqualTo(2);
    assertThat(offer.path("remainingSec").asInt()).isBetween(1, 60);
    assertThat(mine.path("pollAfterSec").asInt()).isEqualTo(5);
    // DD-5(2026-11-19): 제안을 못 받는 라이더(조회 실패 포함)도 null 이 아니라 긴 간격 — 앱이 멈추지 않게
    JsonNode idle =
        body(
            http.get()
                .uri("/offers")
                .header("Authorization", "Bearer " + TestJwt.rider("RD-idle-" + UUID.randomUUID()))
                .retrieve()
                .toEntity(String.class));
    assertThat(idle.path("pollAfterSec").asInt()).isEqualTo(30);

    // 남의 제안 403, 상세를 열면 viewedAt
    assertThat(
            http.get()
                .uri("/offers/{id}", offerId)
                .header("Authorization", "Bearer " + TestJwt.rider(other))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
    assertThat(
            body(http.get()
                    .uri("/offers/{id}", offerId)
                    .header("Authorization", "Bearer " + TestJwt.rider(rider))
                    .retrieve()
                    .toEntity(String.class))
                .path("viewedAt")
                .isString())
        .isTrue();

    ResponseEntity<String> accept = respond(rider, offerId, "k-" + offerId, "ACCEPT");
    assertThat(body(accept).path("outcome").asString()).isEqualTo("ASSIGNED");
    assertThat(body(respond(rider, offerId, "k-" + offerId, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    JsonNode assigned = outbox(deliveryId, "DeliveryAssigned");
    assertThat(assigned.path("payload").path("riderId").asString()).isEqualTo(rider);
    assertThat(assigned.path("payload").path("assignmentVersion").asInt()).isEqualTo(1);
    // 수락 당시 금액·수단·거리(execution 수행료 기준, 2026-11-13)
    assertThat(assigned.path("payload").path("fee").isNumber()).isTrue();
    assertThat(assigned.path("payload").path("vehicleType").asString()).isEqualTo("MOTORCYCLE");
    assertThat(assigned.path("payload").path("distanceM").asInt()).isEqualTo(1800);
    // 라이더 내 배달 내역(2026-11-17 — 원문 §8.1 배달현황): 주문번호·픽업지 지점 이름
    assertThat(assigned.path("payload").path("orderNumber").asString()).isEqualTo("48213907");
    assertThat(assigned.path("payload").path("mfcName").asString()).isEqualTo("부산진구지점");
    assertThat(assigned.path("payload").path("mfcPhone").asString()).isEqualTo("051-000-0000");

    // 다른 라이더가 배달리스트에서 수락 → 이미 배정
    JsonNode late =
        body(
            http.post()
                .uri("/delivery-list/{id}/acceptances", deliveryId)
                .headers(h -> riderHeaders(h, other, "l-" + deliveryId))
                .retrieve()
                .toEntity(String.class));
    assertThat(late.path("outcome").asString()).isEqualTo("LATE");
    assertThat(late.path("lateReason").asString()).isEqualTo("ALREADY_ASSIGNED");

    JsonNode view =
        body(
            http.get()
                .uri("/deliveries/{id}/dispatch", deliveryId)
                .header(
                    "Authorization", "Bearer " + TestJwt.operator("OP-1", List.of(), List.of(mfc)))
                .retrieve()
                .toEntity(String.class));
    assertThat(view.path("state").asString()).isEqualTo("ASSIGNED");
    assertThat(view.path("assignment").path("riderId").asString()).isEqualTo(rider);
    assertThat(view.path("assignment").path("vehicleType").asString()).isEqualTo("MOTORCYCLE");
    // 그 센터 운영자도 배송지 동 이름까지(0.13.1, MFC 시뮬레이터 R-27) — 원문 주소는 없다
    assertThat(view.path("dropAreaLabel").asString()).isEqualTo("서초구 서초동");
    assertThat(
            http.get()
                .uri("/deliveries/{id}/dispatch", deliveryId)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-2", List.of(), List.of("MFC-OTHER")))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
    // 상담원·라이더 운영은 센터와 상관없이 조회(BOD-1·2, 2026-11-15)
    for (String role : List.of("CS_AGENT", "RIDER_OPS")) {
      JsonNode staffView =
          body(
              http.get()
                  .uri("/deliveries/{id}/dispatch", deliveryId)
                  .header(
                      "Authorization",
                      "Bearer " + TestJwt.operator("OP-" + role, List.of(role), List.of()))
                  .retrieve()
                  .toEntity(String.class));
      assertThat(staffView.path("assignment").path("riderId").asString()).isEqualTo(rider);
    }
    JsonNode internal =
        body(
            http.get()
                .uri("/internal/deliveries/{id}/assignment", deliveryId)
                .header("X-Source-System", "order-orchestration-service")
                .retrieve()
                .toEntity(String.class));
    assertThat(internal.path("riderId").asString()).isEqualTo(rider);
    // TR-1 tracking ETA — 픽업지(MFC)·도착지 좌표, 저장 금지
    ResponseEntity<String> points =
        http.get()
            .uri("/internal/deliveries/{id}/route-points", deliveryId)
            .header("X-Source-System", "delivery-tracking-service")
            .retrieve()
            .toEntity(String.class);
    assertThat(points.getHeaders().getCacheControl()).contains("no-store");
    assertThat(body(points).path("pickup").path("lat").isNumber()).isTrue();
    assertThat(body(points).path("drop").path("lat").isNumber()).isTrue();
    assertThat(body(points).path("pickupToDropS").asInt()).isPositive();
    // 라이더가 맡은 배송(rider-auth 정지 라이더 로그인 끊기 시점, 0.3.3)
    JsonNode active =
        body(
            http.get()
                .uri("/internal/riders/{r}/active-deliveries", rider)
                .header("X-Source-System", "rider-auth-service")
                .retrieve()
                .toEntity(String.class));
    assertThat(active.path("deliveryIds").valueStream().map(JsonNode::asString).toList())
        .contains(deliveryId);
    assertThat(
            body(http.get()
                    .uri("/internal/riders/{r}/active-deliveries", "RD-none")
                    .header("X-Source-System", "rider-auth-service")
                    .retrieve()
                    .toEntity(String.class))
                .path("deliveryIds"))
        .isEmpty();

    send(
        "PackageHandedOver",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "deliveryId",
            deliveryId,
            "packageId",
            "PKG-1-" + shipment,
            "riderId",
            rider,
            "handoverId",
            "HO-1",
            "inventoryCommitId",
            "IC-1",
            "lines",
            List.of(),
            "handedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'PICKED_UP'");

    // 사고 지점까지 간 거리(0.9.0, 2026-11-18 — execution 사고 수행료): 등록 시각의 라이더 점 → MFC 에서 그 점까지 도로 거리
    Instant incidentAt = Instant.now();
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/riders/" + rider + "/location-at"))
            .withQueryParam("at", equalTo(incidentAt.toString()))
            .willReturn(
                json(
                    Map.of(
                        "riderId",
                        rider,
                        "lat",
                        37.4991,
                        "lng",
                        127.0301,
                        "recordedAt",
                        incidentAt.minusSeconds(20).toString(),
                        "quality",
                        "GOOD"))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.destinations[0].lat", equalTo("37.4991")))
            .willReturn(json(matrix(List.of(List.of(800, 200))))));
    JsonNode incident =
        body(
            http.get()
                .uri(
                    "/internal/deliveries/{id}/incident-distance?at={at}",
                    deliveryId,
                    incidentAt.toString())
                .header("X-Source-System", "delivery-execution-service")
                .retrieve()
                .toEntity(String.class));
    assertThat(incident.path("distanceM").asInt()).isEqualTo(800);
    assertThat(incident.path("source").asString()).isEqualTo("LOCATION_AT");
    assertThat(
            http.get()
                .uri(
                    "/internal/deliveries/{id}/incident-distance?at={at}",
                    "DLV-none",
                    incidentAt.toString())
                .header("X-Source-System", "delivery-execution-service")
                .retrieve()
                .onStatus(s -> true, (req, res) -> {})
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(404);

    // 전달 완료(delivery-execution) → CLOSED — 라이더 "진행 중" 수에서 빠진다(PC 1 검토 H2)
    String completed =
        EventEnvelopeFixtures.envelope("DeliveryCompleted")
            .from("delivery-execution-service")
            .aggregate("Delivery", deliveryId, 5)
            .payload(
                Map.of(
                    "deliveryId",
                    deliveryId,
                    "orderId",
                    order,
                    "riderId",
                    rider,
                    "lines",
                    List.of(Map.of("orderLineId", "L-1", "qty", 1)),
                    "method",
                    "CONTACTLESS",
                    "evidenceRef",
                    Map.of("evidenceId", "EVD-1", "kind", "PHOTO"),
                    "deliveredAt",
                    Instant.now().toString()))
            .buildJson();
    kafka.send("delivery.delivery.v1", deliveryId, completed).join();
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'CLOSED'");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE rider_id = ? AND state IN ('ASSIGNED', 'PICKED_UP')",
                Integer.class,
                rider))
        .isZero();
  }

  @Test
  void normalModeRiderSeesListOnlyInsideStageRadius() {
    String mfc = "MFC-D2";
    String normal = "RD-n-" + UUID.randomUUID();
    riders(mfc, rider(normal, "NORMAL", true));
    riderLookup(rider(normal, "NORMAL", true));
    locations(Map.of(normal, new double[] {37.5110, 127.0276})); // MFC 에서 북쪽 약 1.46km
    String shipment = "SHP-" + UUID.randomUUID();
    String deliveryId = readyShipment(mfc, shipment, "PKG-2-" + shipment);

    engine.tick(); // AI 추천 후보 없음 → 1단계에 머문다
    assertThat(
            jdbc.queryForObject(
                "SELECT stage FROM dispatch_attempt WHERE delivery_id = ?",
                Integer.class,
                deliveryId))
        .isEqualTo(1);
    assertThat(listIds(normal)).doesNotContain(deliveryId); // 1km 밖
    listNotifier.run();
    assertThat(listNotices(normal)).isZero(); // 목록에 안 보이면 알림도 없다

    jdbc.update(
        "UPDATE dispatch_attempt SET stage_started_at = ?, next_run_at = ? WHERE delivery_id = ?",
        ago(120),
        ago(0),
        deliveryId);
    engine.tick(); // 응답 대기만큼 머문 뒤 2단계(2km)
    assertThat(
            jdbc.queryForObject(
                "SELECT stage FROM dispatch_attempt WHERE delivery_id = ?",
                Integer.class,
                deliveryId))
        .isEqualTo(2);
    assertThat(listIds(normal)).contains(deliveryId);
    // 일반 모드 새 배달 알림(2026-11-18 "보냄 — 묶어서"): 목록에 새로 보이면 한 번, 1분 안에는 다시 안 보냄
    listNotifier.run();
    assertThat(listNotices(normal)).isEqualTo(1);
    assertThat(outbox(normal, "DeliveryListUpdated").path("payload").path("newCount").asInt())
        .isEqualTo(1);
    listNotifier.run();
    assertThat(listNotices(normal)).isEqualTo(1);

    JsonNode ok =
        body(
            http.post()
                .uri("/delivery-list/{id}/acceptances", deliveryId)
                .headers(h -> riderHeaders(h, normal, "a-" + deliveryId))
                .retrieve()
                .toEntity(String.class));
    assertThat(ok.path("outcome").asString()).isEqualTo("ASSIGNED");
    assertThat(outbox(deliveryId, "DeliveryAssigned").path("payload").path("riderId").asString())
        .isEqualTo(normal);
    assertThat(
            jdbc.queryForObject(
                "SELECT source FROM dispatch_offer WHERE delivery_id = ? AND state = 'ACCEPTED'",
                String.class,
                deliveryId))
        .isEqualTo("DELIVERY_LIST");
  }

  /** DD-4 정시 배송률(2026-11-19): 약속 끝 날짜·센터별 정시·늦음·판정 불가·아직, 주문 취소 제외. */
  @Test
  void onTimeDeliveryMetricsPerDayAndCenter() {
    String mfc = "MFC-OT-" + UUID.randomUUID().toString().substring(0, 6);
    java.time.Instant base = java.time.Instant.now().minusSeconds(3 * 3600);
    String[] ids = new String[5];
    for (int k = 0; k < 5; k++) {
      String shipment = "SHP-" + UUID.randomUUID();
      ids[k] = readyShipment(mfc, shipment, "PKG-ot-" + shipment);
    }
    // 0 정시, 1 늦게 완료, 2 고객 확인(판정 불가), 3 약속 지났는데 전달 전(늦음), 4 주문 취소(빠짐)
    jdbc.update(
        "UPDATE delivery SET promised_at = ?, completed_at = ?, state = 'CLOSED' WHERE delivery_id = ?",
        java.sql.Timestamp.from(base),
        java.sql.Timestamp.from(base.minusSeconds(60)),
        ids[0]);
    jdbc.update(
        "UPDATE delivery SET promised_at = ?, completed_at = ?, state = 'CLOSED' WHERE delivery_id = ?",
        java.sql.Timestamp.from(base),
        java.sql.Timestamp.from(base.plusSeconds(600)),
        ids[1]);
    jdbc.update(
        "UPDATE delivery SET promised_at = ?, completed_at = ?, completion_method = 'CUSTOMER_CONFIRMED', state = 'CLOSED' WHERE delivery_id = ?",
        java.sql.Timestamp.from(base),
        java.sql.Timestamp.from(base.minusSeconds(60)),
        ids[2]);
    jdbc.update(
        "UPDATE delivery SET promised_at = ? WHERE delivery_id = ?",
        java.sql.Timestamp.from(base),
        ids[3]);
    jdbc.update(
        "UPDATE delivery SET promised_at = ?, state = 'CANCELLED_BEFORE_PICKUP', cancel_reason = 'ORDER_CANCELLED' WHERE delivery_id = ?",
        java.sql.Timestamp.from(base),
        ids[4]);
    String day = base.atZone(java.time.ZoneId.of("Asia/Seoul")).toLocalDate().toString();
    JsonNode m =
        body(
            http.get()
                .uri("/metrics/on-time-deliveries?from={d}&to={d}&mfcId={m}", day, day, mfc)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of()))
                .retrieve()
                .toEntity(String.class));
    JsonNode row = m.path("items").get(0);
    assertThat(row.path("mfcId").asString()).isEqualTo(mfc);
    assertThat(row.path("onTime").asInt()).isEqualTo(1);
    assertThat(row.path("late").asInt()).isEqualTo(2);
    assertThat(row.path("due").asInt()).isEqualTo(3);
    assertThat(row.path("unjudged").asInt()).isEqualTo(1);
    // 그 센터 아닌 운영자 403
    assertThat(
            http.get()
                .uri("/metrics/on-time-deliveries?mfcId={m}", mfc)
                .header(
                    "Authorization",
                    "Bearer "
                        + TestJwt.operator("OP-X", List.of("MFC_SUPERVISOR"), List.of("MFC-OTHER")))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
  }

  /** 배차 지연 목록(2026-11-19 — 원문 §3.6): 지연 안내가 난 픽업 전 배송만, 그 센터 직원·고객센터, 다른 센터 직원 403. */
  @Test
  void dispatchDelaysListsAlertedUnpickedDeliveries() {
    String mfc = "MFC-DL-" + UUID.randomUUID().toString().substring(0, 6);
    String shipment = "SHP-" + UUID.randomUUID();
    String deliveryId = readyShipment(mfc, shipment, "PKG-DL-" + shipment);
    jdbc.update(
        "UPDATE delivery SET delay_alerted_at = now(), promised_at = now() - interval '30 minutes'"
            + " WHERE delivery_id = ?",
        deliveryId);
    String sup = TestJwt.operator("OP-SUP", List.of("MFC_SUPERVISOR"), List.of(mfc));
    JsonNode list =
        body(
            http.get()
                .uri("/dispatch-delays?mfcId={m}", mfc)
                .header("Authorization", "Bearer " + sup)
                .retrieve()
                .toEntity(String.class));
    assertThat(list.path("items")).hasSize(1);
    assertThat(list.path("items").get(0).path("deliveryId").asString()).isEqualTo(deliveryId);
    assertThat(list.path("items").get(0).path("orderId").asString()).isNotBlank();
    assertThat(
            http.get()
                .uri("/dispatch-delays?mfcId={m}", mfc)
                .header(
                    "Authorization",
                    "Bearer "
                        + TestJwt.operator("OP-X", List.of("MFC_SUPERVISOR"), List.of("MFC-OTHER")))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
    // 픽업하면 빠진다
    jdbc.update("UPDATE delivery SET state = 'PICKED_UP' WHERE delivery_id = ?", deliveryId);
    assertThat(
            body(http.get()
                    .uri("/dispatch-delays")
                    .header(
                        "Authorization",
                        "Bearer " + TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                    .retrieve()
                    .toEntity(String.class))
                .path("items")
                .toString())
        .doesNotContain(deliveryId);
  }

  @Test
  void rejectionRetriesSameRiderThenFails() {
    String mfc = "MFC-D3";
    String rider = "RD-r-" + UUID.randomUUID();
    riders(mfc, rider(rider, "AI_RECOMMENDED", true));
    riderLookup(rider(rider, "AI_RECOMMENDED", true));
    locations(Map.of(rider, new double[] {37.4990, 127.0280}));
    String shipment = "SHP-" + UUID.randomUUID();
    String deliveryId = readyShipment(mfc, shipment, "PKG-3-" + shipment);

    engine.tick();
    String first =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    JsonNode rej = body(respond(rider, first, "r1-" + first, "REJECT"));
    assertThat(rej.path("outcome").asString()).isEqualTo("REJECTED");
    assertThat(rej.path("consecutiveRejections").asInt()).isEqualTo(1);

    // 같은 회차에는 거절한 라이더를 빼고 찾는다 → 마지막 단계까지 없으면 10분 뒤 재요청
    jdbc.update(
        "UPDATE dispatch_attempt SET stage = 3, stage_started_at = ?, next_run_at = ? WHERE delivery_id = ?",
        ago(120),
        ago(0),
        deliveryId);
    engine.tick();
    Map<String, Object> a =
        jdbc.queryForMap(
            "SELECT stage, retry_count, round, next_run_at > ? AS later FROM dispatch_attempt WHERE delivery_id = ?",
            java.sql.Timestamp.from(Instant.now().plusSeconds(540)),
            deliveryId);
    assertThat(a.get("retry_count")).isEqualTo(1);
    assertThat(a.get("round")).isEqualTo(1);
    assertThat(a.get("stage")).isEqualTo(1);
    assertThat(a.get("later")).isEqualTo(true);

    // 재요청 회차에는 거절했던 라이더에게도 다시(원문 §3.5)
    jdbc.update(
        "UPDATE dispatch_attempt SET next_run_at = ?, stage_started_at = ? WHERE delivery_id = ?",
        ago(0),
        ago(0),
        deliveryId);
    engine.tick();
    String second =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    assertThat(second).isNotEqualTo(first);
    assertThat(
            jdbc.queryForObject(
                "SELECT rider_id FROM dispatch_offer WHERE offer_id = ?", String.class, second))
        .isEqualTo(rider);

    // 응답 없이 만료 → 마지막 재요청까지 다 썼으면 실패(거절이 있었으니 REPEATED_REJECTION) → 10분 뒤 자동으로 다시 배차 요청(M2)
    jdbc.update("UPDATE dispatch_offer SET expires_at = ? WHERE offer_id = ?", ago(1), second);
    jdbc.update(
        "UPDATE dispatch_attempt SET retry_count = 2, stage = 3 WHERE delivery_id = ?", deliveryId);
    engine.tick(); // 만료 → 다시 탐색(후보 없음, 단계에 머묾)
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM dispatch_offer WHERE offer_id = ?", String.class, second))
        .isEqualTo("EXPIRED");
    jdbc.update(
        "UPDATE dispatch_attempt SET stage_started_at = ?, next_run_at = ? WHERE delivery_id = ?",
        ago(120),
        ago(0),
        deliveryId);
    engine.tick();
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("DISPATCHING");
    JsonNode failed = outbox(deliveryId, "DispatchFailed").path("payload");
    assertThat(failed.path("reason").asString()).isEqualTo("REPEATED_REJECTION");
    assertThat(failed.path("mfcId").asString()).isNotBlank(); // DD-MFC — 센터 관리자 사건함
    Map<String, Object> next =
        jdbc.queryForMap(
            "SELECT trigger, state, next_run_at > ? AS later FROM dispatch_attempt WHERE delivery_id = ? AND"
                + " state = 'SEARCHING'",
            java.sql.Timestamp.from(Instant.now().plusSeconds(540)),
            deliveryId);
    assertThat(next.get("trigger")).isEqualTo("RETRY");
    assertThat(next.get("later")).isEqualTo(true);
  }

  @Test
  void allPackagesStoppedCancelsBeforePickup() {
    String mfc = "MFC-D4";
    riders(mfc);
    String shipment = "SHP-" + UUID.randomUUID();
    String pkg = "PKG-4-" + shipment;
    String deliveryId = readyShipment(mfc, shipment, pkg);
    String order =
        jdbc.queryForObject(
            "SELECT order_id FROM delivery WHERE delivery_id = ?", String.class, deliveryId);
    send(
        "WorkStopResult",
        order,
        Map.of(
            "orderId",
            order,
            "stopRequestId",
            "STOP-1",
            "perPackage",
            List.of(Map.of("packageId", pkg, "state", "STOPPED")),
            "decidedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'CANCELLED_BEFORE_PICKUP'");
    assertThat(
            outbox(deliveryId, "DeliveryCancelledBeforePickup")
                .path("payload")
                .path("reason")
                .asString())
        .isEqualTo("ORDER_CANCELLED");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM dispatch_attempt WHERE delivery_id = ?",
                String.class,
                deliveryId))
        .isEqualTo("CANCELLED");
  }

  @Test
  void bundledDeliveriesAreOfferedAsOneBundleAndAcceptedTogether() {
    String mfc = "MFC-D5";
    String rider = "RD-b-" + UUID.randomUUID();
    riders(mfc, rider(rider, "AI_RECOMMENDED", true));
    riderLookup(rider(rider, "AI_RECOMMENDED", true));
    locations(Map.of(rider, new double[] {37.4985, 127.0280}));
    String orderA = "ORD-A-" + UUID.randomUUID();
    String orderB = "ORD-B-" + UUID.randomUUID();
    bundledOrder(orderA, "IT-ROAD-A", 37.5006, 127.0360);
    bundledOrder(orderB, "IT-ROAD-B", 37.5012, 127.0372);
    // 고객 A ↔ B 120초(두 순서 모두)
    for (String q :
        List.of("37.5006,127.036;37.5012,127.0372", "37.5012,127.0372;37.5006,127.036")) {
      up.stubFor(
          post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
              .atPriority(0)
              .withRequestBody(
                  matchingJsonPath("$.origins[0].lat", equalTo(q.split(";")[0].split(",")[0])))
              .withRequestBody(
                  matchingJsonPath("$.origins[1].lat", equalTo(q.split(";")[1].split(",")[0])))
              .willReturn(json(square(120))));
    }
    String a = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-5a-" + orderA, orderA);
    String b = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-5b-" + orderB, orderB);

    engine.tick();

    List<Map<String, Object>> offers =
        jdbc.queryForList(
            "SELECT offer_id, delivery_id, bundle_id, bundle_size, stop_seq, rider_id FROM dispatch_offer"
                + " WHERE delivery_id IN (?, ?) AND state = 'SENT' ORDER BY stop_seq",
            a,
            b);
    assertThat(offers).hasSize(2);
    assertThat(offers.get(0).get("bundle_id"))
        .isNotNull()
        .isEqualTo(offers.get(1).get("bundle_id"));
    assertThat(offers).allSatisfy(o -> assertThat(o.get("bundle_size")).isEqualTo(2));
    assertThat(offers).allSatisfy(o -> assertThat(o.get("rider_id")).isEqualTo(rider));
    assertThat(offers.stream().map(o -> o.get("stop_seq")).toList()).containsExactly(1, 2);

    JsonNode mine =
        body(
            http.get()
                .uri("/offers")
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    assertThat(mine.path("items")).hasSize(2);
    assertThat(mine.path("items").get(0).path("bundleSize").asInt()).isEqualTo(2);

    String second = (String) offers.get(1).get("offer_id");
    JsonNode ok = body(respond(rider, second, "k-" + second, "ACCEPT"));
    assertThat(ok.path("outcome").asString()).isEqualTo("ASSIGNED");
    // DE-6(2026-11-19): 배정 사건에 묶음 전달 순서
    String assignedPayload =
        jdbc.queryForObject(
            "SELECT payload::text FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryAssigned'",
            String.class,
            a);
    assertThat(assignedPayload).contains("\"bundleId\"").contains("\"stopSequence\"");
    for (String id : List.of(a, b)) {
      Map<String, Object> d =
          jdbc.queryForMap(
              "SELECT state, rider_id, bundle_id, stop_seq FROM delivery WHERE delivery_id = ?",
              id);
      assertThat(d.get("state")).isEqualTo("ASSIGNED");
      assertThat(d.get("rider_id")).isEqualTo(rider);
      assertThat(d.get("bundle_id")).isEqualTo(offers.get(0).get("bundle_id"));
      assertThat(outbox(id, "DeliveryAssigned").path("payload").path("riderId").asString())
          .isEqualTo(rider);
    }
    JsonNode view =
        body(
            http.get()
                .uri("/internal/deliveries/{id}/assignment", b)
                .header("X-Source-System", "order-orchestration-service")
                .retrieve()
                .toEntity(String.class));
    assertThat(view.path("stopSequence").asInt())
        .isEqualTo(
            ((Number)
                    jdbc.queryForObject(
                        "SELECT stop_seq FROM delivery WHERE delivery_id = ?", Integer.class, b))
                .intValue());
  }

  @Test
  void rejectingABundleRejectsEveryDeliveryOnce() {
    String mfc = "MFC-D6";
    String rider = "RD-x-" + UUID.randomUUID();
    riders(mfc, rider(rider, "AI_RECOMMENDED", true));
    riderLookup(rider(rider, "AI_RECOMMENDED", true));
    locations(Map.of(rider, new double[] {37.4985, 127.0280}));
    String orderA = "ORD-C-" + UUID.randomUUID();
    String orderB = "ORD-D-" + UUID.randomUUID();
    bundledOrder(orderA, "IT-ROAD-C", 37.5020, 127.0300);
    bundledOrder(orderB, "IT-ROAD-D", 37.5024, 127.0310);
    for (String q : List.of("37.502,127.03;37.5024,127.031", "37.5024,127.031;37.502,127.03")) {
      up.stubFor(
          post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
              .atPriority(0)
              .withRequestBody(
                  matchingJsonPath("$.origins[0].lat", equalTo(q.split(";")[0].split(",")[0])))
              .withRequestBody(
                  matchingJsonPath("$.origins[1].lat", equalTo(q.split(";")[1].split(",")[0])))
              .willReturn(json(square(90))));
    }
    String a = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-6a-" + orderA, orderA);
    String b = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-6b-" + orderB, orderB);
    engine.tick();
    String first =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            a);
    JsonNode rej = body(respond(rider, first, "r-" + first, "REJECT"));
    assertThat(rej.path("outcome").asString()).isEqualTo("REJECTED");
    assertThat(rej.path("consecutiveRejections").asInt()).isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM dispatch_offer WHERE delivery_id IN (?, ?) AND state = 'REJECTED'",
                Integer.class,
                a,
                b))
        .isEqualTo(2);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM dispatch_attempt WHERE delivery_id IN (?, ?) AND state = 'SEARCHING'",
                Integer.class,
                a,
                b))
        .isEqualTo(2);
  }

  void bundledOrder(String orderId, String road, double lat, double lng) {
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/orders/" + orderId + "/delivery-destination"))
            .atPriority(1)
            .willReturn(
                json(
                    Map.of(
                        "orderId",
                        orderId,
                        "mfcId",
                        "x",
                        "status",
                        "CONFIRMED",
                        "deliveryMode",
                        "BUNDLED",
                        "roadAddress",
                        road))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/address-geocodes"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.query", equalTo(road)))
            .willReturn(
                json(
                    Map.of(
                        "results",
                        List.of(Map.of("lat", lat, "lng", lng, "jibunAddress", "서울 강남구 역삼동 1"))))));
  }

  static Map<String, Object> square(int seconds) {
    return Map.of(
        "provider",
        "KAKAO_MOBILITY",
        "fallback",
        false,
        "rows",
        List.of(
            Map.of(
                "elements",
                List.of(
                    Map.of("status", "OK", "distanceM", 0, "durationSeconds", 0),
                    Map.of("status", "OK", "distanceM", 500, "durationSeconds", seconds))),
            Map.of(
                "elements",
                List.of(
                    Map.of("status", "OK", "distanceM", 500, "durationSeconds", seconds),
                    Map.of("status", "OK", "distanceM", 0, "durationSeconds", 0)))));
  }

  @Test
  void riderCancelsBeforePickupAndAnotherRiderIsOffered() {
    String mfc = "MFC-D7";
    String r1 = "RD-c1-" + UUID.randomUUID();
    String r2 = "RD-c2-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true), rider(r2, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true), rider(r2, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}, r2, new double[] {37.4986, 127.0281}));
    String deliveryId =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-7-" + UUID.randomUUID());
    engine.tick();
    Map<String, Object> o =
        jdbc.queryForMap(
            "SELECT offer_id, rider_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            deliveryId);
    String first = (String) o.get("rider_id");
    String other = first.equals(r1) ? r2 : r1;
    assertThat(
            body(respond(first, (String) o.get("offer_id"), "a-" + deliveryId, "ACCEPT"))
                .path("outcome")
                .asString())
        .isEqualTo("ASSIGNED");

    // 배정되지 않은 라이더는 취소할 수 없다
    assertThat(cancel(other, deliveryId, "x-" + deliveryId).getStatusCode().value()).isEqualTo(403);
    ResponseEntity<String> ok = cancel(first, deliveryId, "c-" + deliveryId);
    assertThat(ok.getStatusCode().value()).isEqualTo(202);
    assertThat(cancel(first, deliveryId, "c-" + deliveryId).getStatusCode().value())
        .isEqualTo(202); // 같은 키
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("DISPATCHING");
    JsonNode changed = outbox(deliveryId, "DeliveryAssignmentChanged").path("payload");
    assertThat(changed.path("oldRiderId").asString()).isEqualTo(first);
    assertThat(changed.path("newRiderId").isNull()).isTrue();
    assertThat(changed.path("assignmentVersion").asInt()).isEqualTo(2);
    JsonNode result =
        body(
            http.get()
                .uri("/deliveries/{id}/assignment-cancellations/{r}", deliveryId, "c-" + deliveryId)
                .header("Authorization", "Bearer " + TestJwt.rider(first))
                .retrieve()
                .toEntity(String.class));
    assertThat(result.path("state").asString()).isEqualTo("CANCELLED_BEFORE_PICKUP");
    // 결과는 요청한 본인·고객센터·그 센터 운영자만(L1) — 다른 라이더에게는 없는 것처럼
    assertThat(
            http.get()
                .uri("/deliveries/{id}/assignment-cancellations/{r}", deliveryId, "c-" + deliveryId)
                .header("Authorization", "Bearer " + TestJwt.rider(other))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(404);
    assertThat(
            http.get()
                .uri("/deliveries/{id}/assignment-cancellations/{r}", deliveryId, "c-" + deliveryId)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(200);

    // 재배차 — 취소한 라이더는 빼고 다른 라이더에게
    engine.tick();
    assertThat(
            jdbc.queryForObject(
                "SELECT rider_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
                String.class,
                deliveryId))
        .isEqualTo(other);

    // 라이더 배정 이력(2026-11-18 — 라이더 백오피스 요청 #4): 취소한 라이더는 RELEASED + 취소 사유, 상담원 보기 됨·센터 관리자 403
    JsonNode hist =
        body(
            http.get()
                .uri("/rider-assignments?riderId={r}", first)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                .retrieve()
                .toEntity(String.class));
    JsonNode row = hist.path("items").get(0);
    assertThat(row.path("deliveryId").asString()).isEqualTo(deliveryId);
    assertThat(row.path("outcome").asString()).isEqualTo("RELEASED");
    assertThat(row.path("cancellation").path("requestedBy").asString()).isEqualTo("RIDER");
    assertThat(
            http.get()
                .uri("/rider-assignments?riderId={r}", first)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-S", List.of("MFC_SUPERVISOR"), List.of(mfc)))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);

    // 취소한 라이더는 배달리스트에서도 이 배송을 못 받는다(L5)
    JsonNode again =
        body(
            http.post()
                .uri("/delivery-list/{id}/acceptances", deliveryId)
                .headers(h -> riderHeaders(h, first, "l-" + deliveryId))
                .retrieve()
                .toEntity(String.class));
    assertThat(again.path("outcome").asString()).isEqualTo("LATE");
    assertThat(again.path("lateReason").asString()).isEqualTo("INELIGIBLE");
    // 같은 키를 다른 경로(X-Source-System)로 다시 써도 500 이 아니라 409(L2)
    ResponseEntity<String> otherSource =
        http.post()
            .uri("/deliveries/{id}/assignment-cancellations", deliveryId)
            .headers(
                h -> {
                  h.setBearerAuth(TestJwt.operator("OP-7", List.of(), List.of(mfc)));
                  h.set("Idempotency-Key", "c-" + deliveryId);
                  h.set("X-Source-System", "mfc-backoffice");
                })
            .body(
                Map.of("requestedBy", "STORE_STAFF", "reason", "STORE_REQUEST", "reassign", false))
            .retrieve()
            .toEntity(String.class);
    assertThat(otherSource.getStatusCode().value()).isEqualTo(409);
    assertThat(body(otherSource).path("code").asString()).isEqualTo("IDEMPOTENCY_CONFLICT");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("DISPATCHING"); // 취소는 되돌려졌다(같은 트랜잭션)
    // 다른 라이더가 수락 → 제안만 받았던 첫 라이더는 누가 배정됐는지 못 본다(L4)
    String offerId =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    assertThat(
            body(respond(other, offerId, "a2-" + deliveryId, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    JsonNode seenByFirst =
        body(
            http.get()
                .uri("/deliveries/{id}/dispatch", deliveryId)
                .header("Authorization", "Bearer " + TestJwt.rider(first))
                .retrieve()
                .toEntity(String.class));
    assertThat(seenByFirst.path("state").asString()).isEqualTo("ASSIGNED");
    assertThat(
            seenByFirst.path("assignment").isMissingNode()
                || seenByFirst.path("assignment").isNull())
        .isTrue();
    // 배송 사건마다 aggregateVersion 이 다르다(L3 — 제안 사건이 직전 사건과 같은 버전이던 것)
    List<Long> versions =
        jdbc.queryForList(
            "SELECT (payload->>'aggregateVersion')::bigint FROM outbox_event WHERE aggregateid = ?"
                + " ORDER BY created_at",
            Long.class,
            deliveryId);
    assertThat(versions).doesNotHaveDuplicates();

    // 픽업 뒤는 지원센터로(422)
    jdbc.update(
        "UPDATE delivery SET state = 'PICKED_UP', rider_id = ? WHERE delivery_id = ?",
        other,
        deliveryId);
    ResponseEntity<String> late = cancel(other, deliveryId, "p-" + deliveryId);
    assertThat(late.getStatusCode().value()).isEqualTo(422);
    assertThat(body(late).path("code").asString()).isEqualTo("PICKED_UP_CASE_REQUIRED");
  }

  /** RS-1(사용자 2026-11-14): 라이더 계정 제한 → 픽업 전 배정은 풀고 다시 배차, 픽업한 배송은 끝까지 전달(그대로), 응답 전 제안은 LATE. */
  @Test
  void restrictedRiderLosesPrePickupAssignmentsButKeepsPickedUp() {
    String mfc = "MFC-D13";
    String r1 = "RD-rs1-" + UUID.randomUUID();
    String r2 = "RD-rs2-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true), rider(r2, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}, r2, new double[] {37.4986, 127.0281}));

    String assigned = acceptedBy(r1, mfc);
    String pickedUp = acceptedBy(r1, mfc);
    jdbc.update("UPDATE delivery SET state = 'PICKED_UP' WHERE delivery_id = ?", pickedUp);
    String offeredOrder = "ORD-" + UUID.randomUUID(); // 알뜰 — 두 건을 든 라이더도 받는다(한집은 단독, 2026-11-18)
    bundledOrder(offeredOrder, "IT-ROAD-RS-" + offeredOrder, 37.5006, 127.0360);
    String offered =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-13-" + UUID.randomUUID(), offeredOrder);
    engine.tick();
    String openOffer =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT' AND rider_id = ?",
            String.class,
            offered,
            r1);

    String json =
        EventEnvelopeFixtures.envelope("RiderRestricted")
            .from("rider-service")
            .aggregate("Rider", r1, 3)
            .payload(
                Map.of(
                    "riderId",
                    r1,
                    "restriction",
                    "DISPATCH_RESTRICTED",
                    "from",
                    Instant.now().toString(),
                    "basis",
                    "고객 신고 확인"))
            .buildJson();
    kafka.send("delivery.rider.v1", r1, json).join();
    awaitSql(
        "SELECT count(*) FROM outbox_event WHERE aggregateid = '"
            + assigned
            + "' AND type = 'DeliveryAssignmentChanged'");

    // 픽업 전 배정 → 풀고 재배차(그 라이더는 뺀다)
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, assigned))
        .isEqualTo("DISPATCHING");
    JsonNode changed = outbox(assigned, "DeliveryAssignmentChanged").path("payload");
    assertThat(changed.path("oldRiderId").asString()).isEqualTo(r1);
    assertThat(changed.path("newRiderId").isNull()).isTrue();
    assertThat(changed.path("reason").asString()).isEqualTo("RIDER_RESTRICTED");
    assertThat(changed.path("assignmentVersion").asInt()).isEqualTo(2);
    assertThat(
            jdbc.queryForObject(
                "SELECT excluded_rider_id FROM dispatch_attempt WHERE delivery_id = ? AND trigger ="
                    + " 'REASSIGN'",
                String.class,
                assigned))
        .isEqualTo(r1);
    // 픽업한 배송은 그대로(전달까지)
    Map<String, Object> kept =
        jdbc.queryForMap("SELECT state, rider_id FROM delivery WHERE delivery_id = ?", pickedUp);
    assertThat(kept.get("state")).isEqualTo("PICKED_UP");
    assertThat(kept.get("rider_id")).isEqualTo(r1);
    assertThat(
            jdbc.queryForObject(
                "SELECT restriction FROM restricted_rider WHERE rider_id = ?", String.class, r1))
        .isEqualTo("DISPATCH_RESTRICTED");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type ="
                    + " 'DeliveryAssignmentChanged'",
                Integer.class,
                pickedUp))
        .isZero();
    // 응답 전 제안은 LATE — 뒤늦은 수락도 LATE
    assertThat(
            jdbc.queryForObject(
                "SELECT state || ':' || late_reason FROM dispatch_offer WHERE offer_id = ?",
                String.class,
                openOffer))
        .isEqualTo("LATE:INELIGIBLE");
    JsonNode late = body(respond(r1, openOffer, "late-" + offered, "ACCEPT"));
    assertThat(late.path("outcome").asString()).isEqualTo("LATE");
    // rider 조회가 아직 제한 전(스텁은 NORMAL)이어도 배달리스트 수락은 잠금 아래 제한 사본을 보고 막힌다 — 겹친 수락이 배정되지 않음
    JsonNode raced =
        body(
            http.post()
                .uri("/delivery-list/{id}/acceptances", offered)
                .headers(h -> riderHeaders(h, r1, "race-" + offered))
                .retrieve()
                .toEntity(String.class));
    assertThat(raced.path("outcome").asString()).isEqualTo("LATE");
    assertThat(raced.path("lateReason").asString()).isEqualTo("INELIGIBLE");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, offered))
        .isNotEqualTo("ASSIGNED");

    // 다른 라이더에게 다시 배차
    riders(mfc, rider(r2, "AI_RECOMMENDED", true));
    engine.tick();
    assertThat(
            jdbc.queryForList(
                "SELECT DISTINCT rider_id FROM dispatch_offer WHERE delivery_id IN (?, ?) AND state ="
                    + " 'SENT'",
                String.class,
                assigned,
                offered))
        .containsExactly(r2);

    // 풀기(NORMAL) → 제한 사본 지움
    kafka
        .send(
            "delivery.rider.v1",
            r1,
            EventEnvelopeFixtures.envelope("RiderRestricted")
                .from("rider-service")
                .aggregate("Rider", r1, 4)
                .payload(
                    Map.of(
                        "riderId",
                        r1,
                        "restriction",
                        "NORMAL",
                        "from",
                        Instant.now().toString(),
                        "basis",
                        "소명 확인"))
                .buildJson())
        .join();
    long until = System.currentTimeMillis() + 30_000;
    while (jdbc.queryForObject(
                "SELECT count(*) FROM restricted_rider WHERE rider_id = ?", Integer.class, r1)
            > 0
        && System.currentTimeMillis() < until) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM restricted_rider WHERE rider_id = ?", Integer.class, r1))
        .isZero();
  }

  /**
   * 포장을 일부만 넘겨받은 배송(모두 넘겨받기 전이라 아직 ASSIGNED)은 픽업한 배송처럼 다룬다 — 라이더 배차 취소는 422, 계정 제한도 배정을 풀지 않는다(PC 1
   * PR #13 검토, 사용자 2026-11-14). 이미 결정된 제안은 다른 상태로 덮이지 않는다(만료 → LATE 덮어쓰기 막기).
   */
  @Test
  void partlyHandedOverDeliveryIsTreatedAsPickedUp() {
    String mfc = "MFC-D14";
    String r1 = "RD-ph-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    String partly = acceptedBy(r1, mfc);
    String plain = acceptedBy(r1, mfc);
    // 포장 두 개 중 하나만 넘겨받음 — 목록에는 아직 안 넘긴 포장이 있다
    jdbc.update(
        "UPDATE delivery SET packages = ?::jsonb, handed_over_packages = ?::jsonb WHERE"
            + " delivery_id = ?",
        "[{\"packageId\":\"P-1\",\"bagCount\":1},{\"packageId\":\"P-2\",\"bagCount\":1}]",
        "[\"P-1\"]",
        partly);

    ResponseEntity<String> refused = cancel(r1, partly, "ph-" + partly);
    assertThat(refused.getStatusCode().value()).isEqualTo(422);
    assertThat(body(refused).path("code").asString()).isEqualTo("PICKED_UP_CASE_REQUIRED");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, partly))
        .isEqualTo("ASSIGNED");

    kafka
        .send(
            "delivery.rider.v1",
            r1,
            EventEnvelopeFixtures.envelope("RiderRestricted")
                .from("rider-service")
                .aggregate("Rider", r1, 3)
                .payload(
                    Map.of(
                        "riderId",
                        r1,
                        "restriction",
                        "SUSPENDED",
                        "from",
                        Instant.now().toString(),
                        "basis",
                        "고객 신고 확인"))
                .buildJson())
        .join();
    awaitSql(
        "SELECT count(*) FROM outbox_event WHERE aggregateid = '"
            + plain
            + "' AND type = 'DeliveryAssignmentChanged'");
    // 넘겨받은 것이 없는 배송만 풀린다
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, plain))
        .isEqualTo("DISPATCHING");
    Map<String, Object> kept =
        jdbc.queryForMap("SELECT state, rider_id FROM delivery WHERE delivery_id = ?", partly);
    assertThat(kept.get("state")).isEqualTo("ASSIGNED");
    assertThat(kept.get("rider_id")).isEqualTo(r1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type ="
                    + " 'DeliveryAssignmentChanged'",
                Integer.class,
                partly))
        .isZero();

    // 응답 전(SENT) 제안만 결정된다 — 이미 결정(ACCEPTED·EXPIRED 등)된 제안은 그대로
    String accepted =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'ACCEPTED'",
            String.class,
            partly);
    assertThat(store.decideOffer(accepted, "LATE", Instant.now(), null, "INELIGIBLE")).isFalse();
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM dispatch_offer WHERE offer_id = ?", String.class, accepted))
        .isEqualTo("ACCEPTED");
  }

  /** 배송 하나를 만들어 그 라이더에게 제안 → 수락(ASSIGNED). 그 MFC 의 적격 라이더는 미리 그 한 명만. */
  @org.springframework.beans.factory.annotation.Autowired
  com.pmart.delivery.dispatch.application.ListNotifier listNotifier;

  int listNotices(String rider) {
    Integer n =
        jdbc.queryForObject(
            "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryListUpdated'",
            Integer.class,
            rider);
    return n == null ? 0 : n;
  }

  /** 한 라이더가 여러 건을 맡는 시험용 — 알뜰배달 주문(2026-11-18 "한집은 단독" — 한집은 한 건씩만). */
  String acceptedBy(String rider, String mfc) {
    String order = "ORD-" + UUID.randomUUID();
    bundledOrder(order, "IT-ROAD-AB-" + order, 37.5006, 127.0360);
    String deliveryId =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-a-" + UUID.randomUUID(), order);
    engine.tick();
    String offerId =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    assertThat(
            body(respond(rider, offerId, "a-" + deliveryId, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    return deliveryId;
  }

  @Test
  void longDispatchWaitRaisesOneDelayAlert() {
    String mfc = "MFC-D8";
    riders(mfc);
    String deliveryId =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-8-" + UUID.randomUUID());
    engine.tick(); // 좌표·MFC → 고객 도로 시간 준비
    // 약속(주문 확정 + 50분)이 30분 전에 지났다 — 예상 전달(지금 + 400초)이 약속 + 20분을 넘는다
    jdbc.update(
        "UPDATE delivery SET promised_at = ? WHERE delivery_id = ?", ago(30 * 60), deliveryId);
    engine.tick();
    JsonNode delayed = outbox(deliveryId, "DispatchDelayed").path("payload");
    assertThat(delayed.path("threshold").asInt()).isEqualTo(20);
    assertThat(delayed.path("mfcId").asString()).isEqualTo(mfc); // DD-MFC
    assertThat(delayed.path("delayedMinutes").asInt()).isGreaterThanOrEqualTo(36);
    engine.tick();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DispatchDelayed'",
                Integer.class,
                deliveryId))
        .isEqualTo(1);
  }

  @Test
  void stopBeforePackingCancelsAndStopBeforeRequestBlocks() {
    // 배차 요청 뒤 피킹 전 중지(perPackage 비어 있음) — 전체인지 부분인지 모르므로 배송은 그대로(PC 1 재검토 ①),
    // 주문 전체 취소(OrderCancelled FULL)가 오면 취소, 부분 취소(PARTIAL)는 그대로
    String mfc = "MFC-D9";
    riders(mfc);
    String order = "ORD-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    send(
        "DispatchRequested",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "trigger",
            "MANUAL",
            "estimatedPackMinutes",
            5,
            "estimatedReadyAt",
            Instant.now().toString(),
            "requestedAt",
            Instant.now().toString()));
    String d1 = awaitDelivery(shipment);
    send(
        "WorkStopResult",
        order,
        Map.of(
            "orderId",
            order,
            "stopRequestId",
            "STOP-9",
            "perPackage",
            List.of(),
            "notStartedLines",
            List.of(Map.of("orderLineId", "L-1", "qty", 1)),
            "decidedAt",
            Instant.now().toString()));
    orderCancelled(order, "PARTIAL");
    awaitSql(
        "SELECT count(*) FROM inbox_event WHERE event_type = 'OrderCancelled' AND aggregate_id = '"
            + order
            + "'");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, d1))
        .isEqualTo("DISPATCHING"); // 부분 중지·부분 취소 — 남은 상품은 배차
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM stopped_order WHERE order_id = ?", Integer.class, order))
        .isZero();
    orderCancelled(order, "FULL");
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + d1
            + "' AND state = 'CANCELLED_BEFORE_PICKUP'");

    // 주문 전체 취소가 배차 요청보다 먼저 오면 → 배송은 BLOCKED, DeliveryCreated 없음
    String order2 = "ORD-" + UUID.randomUUID();
    String shipment2 = "SHP-" + UUID.randomUUID();
    orderCancelled(order2, "FULL");
    awaitSql("SELECT count(*) FROM stopped_order WHERE order_id = '" + order2 + "'");
    send(
        "DispatchRequested",
        shipment2,
        Map.of(
            "orderId",
            order2,
            "shipmentId",
            shipment2,
            "mfcId",
            mfc,
            "trigger",
            "MANUAL",
            "estimatedPackMinutes",
            5,
            "estimatedReadyAt",
            Instant.now().toString(),
            "requestedAt",
            Instant.now().toString()));
    String d2 = awaitDelivery(shipment2);
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, d2))
        .isEqualTo("BLOCKED");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryCreated'",
                Integer.class,
                d2))
        .isZero();
  }

  /**
   * 재포장 무효(2026-11-20 — R-28, 주문 23631583): 목록의 옛 포장이 무효(PackageInvalidated)되면 새 포장으로 바뀌고, 새 포장 인계로
   * PICKED_UP. 인계가 무효 사건보다 먼저 와도 무효를 받을 때 다시 판정한다.
   */
  @Test
  void invalidatedPackageIsReplacedForPickup() {
    String mfc = "MFC-D28";
    String rider = "RD-r28-" + UUID.randomUUID();
    riders(mfc, rider(rider, "AI_RECOMMENDED", true));
    riderLookup(rider(rider, "AI_RECOMMENDED", true));
    locations(Map.of(rider, new double[] {37.4985, 127.0280}));
    String order = "ORD-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    send(
        "DispatchRequested",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "trigger",
            "MANUAL",
            "estimatedPackMinutes",
            5,
            "estimatedReadyAt",
            Instant.now().toString(),
            "requestedAt",
            Instant.now().toString()));
    String deliveryId = awaitDelivery(shipment);
    engine.tick();
    String offer =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    assertThat(body(respond(rider, offer, "a-" + offer, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    String oldPkg = "P-OLD-" + shipment;
    String newPkg = "P-NEW-" + shipment;
    send(
        "ShipmentReady",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(Map.of("packageId", oldPkg, "bagCount", 2))));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND ready_at IS NOT NULL");
    send(
        "PackageHandedOver",
        newPkg,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "deliveryId",
            deliveryId,
            "packageId",
            newPkg,
            "riderId",
            rider,
            "handoverId",
            "HO-NEW",
            "inventoryCommitId",
            "IC-NEW",
            "lines",
            List.of(),
            "handedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND handed_over_packages::text LIKE '%P-NEW-%'");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("ASSIGNED"); // 옛 포장이 목록에 남아 있음
    send(
        "PackageInvalidated",
        oldPkg,
        Map.of(
            "packageId",
            oldPkg,
            "newPackageId",
            newPkg,
            "shipmentId",
            shipment,
            "orderId",
            order,
            "mfcId",
            mfc,
            "reason",
            "REPACK",
            "invalidatedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'PICKED_UP'");
    String pkgs =
        jdbc.queryForObject(
            "SELECT packages::text FROM delivery WHERE delivery_id = ?", String.class, deliveryId);
    assertThat(pkgs).contains(newPkg).doesNotContain(oldPkg).contains("\"bagCount\": 2");
  }

  @Test
  void handoverBeforePackageListDoesNotMarkPickedUpUntilListArrives() {
    String mfc = "MFC-D10";
    String rider = "RD-m4-" + UUID.randomUUID();
    riders(mfc, rider(rider, "AI_RECOMMENDED", true));
    riderLookup(rider(rider, "AI_RECOMMENDED", true));
    locations(Map.of(rider, new double[] {37.4985, 127.0280}));
    String order = "ORD-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    send(
        "DispatchRequested",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "trigger",
            "MANUAL",
            "estimatedPackMinutes",
            5,
            "estimatedReadyAt",
            Instant.now().toString(),
            "requestedAt",
            Instant.now().toString()));
    String deliveryId = awaitDelivery(shipment);
    engine.tick();
    String offer =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    assertThat(body(respond(rider, offer, "a-" + offer, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    // 포장 두 개 중 하나 인계 — 목록을 아직 모르니 픽업 완료 아님(M4)
    send(
        "PackageHandedOver",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "deliveryId",
            deliveryId,
            "packageId",
            "P-A-" + shipment,
            "riderId",
            rider,
            "handoverId",
            "HO-A",
            "inventoryCommitId",
            "IC-A",
            "lines",
            List.of(),
            "handedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND handed_over_packages::text LIKE '%P-A-%'");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("ASSIGNED");
    // 목록이 오면(포장 2개) 아직 하나 남아 그대로, 나머지 인계 뒤 PICKED_UP
    send(
        "ShipmentReady",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(
                Map.of("packageId", "P-A-" + shipment, "bagCount", 1),
                Map.of("packageId", "P-B-" + shipment, "bagCount", 1))));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND ready_at IS NOT NULL");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("ASSIGNED");
    send(
        "PackageHandedOver",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "deliveryId",
            deliveryId,
            "packageId",
            "P-B-" + shipment,
            "riderId",
            rider,
            "handoverId",
            "HO-B",
            "inventoryCommitId",
            "IC-B",
            "lines",
            List.of(),
            "handedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'PICKED_UP'");

    // 운영자가 확정한 전달 실패(execution DeliveryFailed) → CLOSED(2026-11-13)
    kafka
        .send(
            "delivery.delivery.v1",
            deliveryId,
            EventEnvelopeFixtures.envelope("DeliveryFailed")
                .from("delivery-execution-service")
                .aggregate("Delivery", deliveryId, 7)
                .payload(
                    Map.of(
                        "deliveryId",
                        deliveryId,
                        "reason",
                        "CUSTOMER_ABSENT",
                        "custody",
                        "RIDER",
                        "failedAt",
                        Instant.now().toString()))
                .buildJson())
        .join();
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'CLOSED'");
  }

  @Test
  void sameRiderTwoListAcceptancesAtOnceRespectOneAtATime() throws Exception {
    String mfc = "MFC-D11";
    String rider = "RD-m1-" + UUID.randomUUID();
    Map<String, Object> r = rider(rider, "NORMAL", true);
    r.put("oneAtATime", true);
    riders(mfc, r);
    riderLookup(r);
    locations(Map.of(rider, new double[] {37.4985, 127.0280}));
    String a = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-11a-" + UUID.randomUUID());
    String b = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-11b-" + UUID.randomUUID());
    engine.tick(); // 좌표 준비
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      var fa =
          pool.submit(
              () ->
                  body(
                      http.post()
                          .uri("/delivery-list/{id}/acceptances", a)
                          .headers(h -> riderHeaders(h, rider, "m1a-" + a))
                          .retrieve()
                          .toEntity(String.class)));
      var fb =
          pool.submit(
              () ->
                  body(
                      http.post()
                          .uri("/delivery-list/{id}/acceptances", b)
                          .headers(h -> riderHeaders(h, rider, "m1b-" + b))
                          .retrieve()
                          .toEntity(String.class)));
      List<String> outcomes =
          List.of(fa.get().path("outcome").asString(), fb.get().path("outcome").asString());
      // 하나씩 배달하기 — 먼저 잡은 수락의 건수를 본 뒤라 둘째는 INELIGIBLE(M1)
      assertThat(outcomes).containsExactlyInAnyOrder("ASSIGNED", "LATE");
    } finally {
      pool.shutdown();
    }
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE rider_id = ? AND state = 'ASSIGNED'",
                Integer.class,
                rider))
        .isEqualTo(1);
  }

  @Test
  void onTimeDeliveryGetsNoDelayAlertAndOperatorCanCancelWaitingDelivery() {
    String mfc = "MFC-D12";
    riders(mfc);
    String deliveryId =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-12-" + UUID.randomUUID());
    engine.tick();
    // 약속까지 10분 남음 — 예상 전달(지금 + 400초)이 약속 + 20분 안이라 안내 없음
    jdbc.update("UPDATE delivery SET promised_at = ? WHERE delivery_id = ?", ago(-600), deliveryId);
    engine.tick();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DispatchDelayed'",
                Integer.class,
                deliveryId))
        .isZero();
    // 라이더는 배정 전 배송을 취소할 수 없다, 운영자(MFC 백오피스)는 재배차 없이 취소(M2)
    ResponseEntity<String> riderTry =
        cancel("RD-x-" + UUID.randomUUID(), deliveryId, "rc-" + deliveryId);
    assertThat(riderTry.getStatusCode().value()).isEqualTo(403);
    // 상담원은 조회만 — 배차 취소는 403(ADR-0060, BOD-2)
    assertThat(
            http.post()
                .uri("/deliveries/{id}/assignment-cancellations", deliveryId)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-CS", List.of("CS_AGENT"), List.of()))
                .header("Idempotency-Key", "cs-" + deliveryId)
                .header("X-Source-System", "backoffice")
                .body(
                    Map.of(
                        "requestedBy", "STORE_STAFF", "reason", "STORE_REQUEST", "reassign", false))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(403);
    ResponseEntity<String> op =
        http.post()
            .uri("/deliveries/{id}/assignment-cancellations", deliveryId)
            .header("Authorization", "Bearer " + TestJwt.operator("OP-M", List.of(), List.of(mfc)))
            .header("Idempotency-Key", "oc-" + deliveryId)
            .header("X-Source-System", "mfc-backoffice")
            .body(
                Map.of("requestedBy", "STORE_STAFF", "reason", "STORE_REQUEST", "reassign", false))
            .retrieve()
            .toEntity(String.class);
    assertThat(op.getStatusCode().value()).isEqualTo(202);
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("CANCELLED_BEFORE_PICKUP");
    assertThat(outboxCount(deliveryId, "DeliveryCancelledBeforePickup")).isEqualTo(1);

    // 점원 배차 요청 전(ShipmentReady 만 — CREATED, DeliveryCreated 를 낸 적 없음) 취소는 사건 없이(PC 1 검토 후속)
    String order = "ORD-" + UUID.randomUUID();
    String shipment = "SHP-" + UUID.randomUUID();
    send(
        "ShipmentReady",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(Map.of("packageId", "PKG-12c-" + shipment, "bagCount", 1))));
    String created = awaitDelivery(shipment);
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, created))
        .isEqualTo("CREATED");
    ResponseEntity<String> op2 =
        http.post()
            .uri("/deliveries/{id}/assignment-cancellations", created)
            .header("Authorization", "Bearer " + TestJwt.operator("OP-M", List.of(), List.of(mfc)))
            .header("Idempotency-Key", "oc-" + created)
            .header("X-Source-System", "mfc-backoffice")
            .body(
                Map.of("requestedBy", "STORE_STAFF", "reason", "STORE_REQUEST", "reassign", false))
            .retrieve()
            .toEntity(String.class);
    assertThat(op2.getStatusCode().value()).isEqualTo(202);
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, created))
        .isEqualTo("CANCELLED_BEFORE_PICKUP");
    assertThat(outboxCount(created, "DeliveryCreated")).isZero();
    assertThat(outboxCount(created, "DeliveryCancelledBeforePickup")).isZero();
  }

  int outboxCount(String deliveryId, String type) {
    Integer n =
        jdbc.queryForObject(
            "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = ?",
            Integer.class,
            deliveryId,
            type);
    return n == null ? 0 : n;
  }

  @Test
  void riderWithRecentCancellationsSeesNewDeliveriesLater() {
    String mfc = "MFC-D13";
    String rider = "RD-p-" + UUID.randomUUID();
    riders(mfc, rider(rider, "NORMAL", true));
    riderLookup(rider(rider, "NORMAL", true));
    locations(Map.of(rider, new double[] {37.4985, 127.0280}));
    String deliveryId =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-13-" + UUID.randomUUID());
    engine.tick();
    assertThat(listIds(rider)).contains(deliveryId);
    // 최근 배차취소 3건 → 새 배송은 3분 뒤에야 보인다(제재 아님 — 알림 감소)
    for (int i = 0; i < 3; i++) {
      jdbc.update(
          "INSERT INTO assignment_cancellation (request_id, delivery_id, requested_by, actor, reason, reassign,"
              + " state, created_at) VALUES (?, ?, 'RIDER', ?, 'RIDER_UNABLE', true, 'CANCELLED_BEFORE_PICKUP', ?)",
          "pc-" + i + "-" + rider,
          "DLV-old-" + i,
          rider,
          ago(3600));
    }
    assertThat(listIds(rider)).doesNotContain(deliveryId);
    jdbc.update(
        "UPDATE dispatch_attempt SET started_at = ? WHERE delivery_id = ?", ago(200), deliveryId);
    assertThat(listIds(rider)).contains(deliveryId);
  }

  /**
   * DD-2(사용자 2026-11-16 "추천대로"): 오배송 확인(case MisdeliveryConfirmed) → 회수
   * 배송(RETURN·MISDELIVERY_RECOVERY) — 회수지 = 원래 라이더가 완료를 누른 시각의 위치 점, 원래 라이더 빼고 회수지 반경 안 가장 빠른 라이더에게
   * 1건 제안, 예상 배달료 = 일반 표(회수지 → MFC). 회수지 좌표는 배정된 라이더·직원에게만. 회수(ReturnPickedUp) → PICKED_UP,
   * 반납(ReturnDelivered) → CLOSED.
   */
  @Test
  void confirmedMisdeliveryDispatchesRecoveryToAnotherNearbyRider() {
    String mfc = "MFC-D20";
    String r1 = "RD-md1-" + UUID.randomUUID();
    String r2 = "RD-md2-" + UUID.randomUUID();
    String r3 = "RD-md3-" + UUID.randomUUID();
    riders(
        mfc,
        rider(r1, "AI_RECOMMENDED", true),
        rider(r2, "AI_RECOMMENDED", true),
        rider(r3, "AI_RECOMMENDED", true));
    riderLookup(
        rider(r1, "AI_RECOMMENDED", true),
        rider(r2, "AI_RECOMMENDED", true),
        rider(r3, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    String origin = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-md-" + UUID.randomUUID());
    engine.tick();
    String offer1 =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            origin);
    assertThat(body(respond(r1, offer1, "a-" + origin, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    // 전달 완료(execution) — 완료 시각이 회수지 위치 점의 기준
    Instant deliveredAt =
        Instant.now().minusSeconds(600).truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    executionEvent(
        "DeliveryCompleted",
        origin,
        Map.of(
            "deliveryId",
            origin,
            "riderId",
            r1,
            "deliveredAt",
            deliveredAt.toString(),
            "lines",
            List.of(Map.of("orderLineId", "L-1", "qty", 1)),
            "method",
            "CONTACTLESS"));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + origin
            + "' AND state = 'CLOSED' AND completed_at IS NOT NULL");

    // tracking — 원래 라이더의 그 시각 위치(회수지), 회수지 → MFC 2,100m·500초, 라이더 → 회수지(가까운 순 150·400초)
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/riders/" + r1 + "/location-at"))
            .withQueryParam("at", equalTo(deliveredAt.toString()))
            .willReturn(
                json(
                    Map.of(
                        "riderId",
                        r1,
                        "lat",
                        37.501,
                        "lng",
                        127.0365,
                        "recordedAt",
                        deliveredAt.plusSeconds(5).toString(),
                        "quality",
                        "GOOD"))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.origins[0].lat", equalTo("37.501")))
            .willReturn(json(matrix(List.of(List.of(2100, 500))))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(2)
            .withRequestBody(matchingJsonPath("$.destinations[0].lat", equalTo("37.501")))
            .willReturn(json(matrix(List.of(List.of(300, 150), List.of(900, 400))))));
    // r1(원래 라이더)은 회수지 바로 옆이어도 빼고, r3 는 회수지 1km 밖(MFC 근처)
    locations(
        Map.of(
            r1, new double[] {37.5010, 127.0365},
            r2, new double[] {37.5013, 127.0368},
            r3, new double[] {37.4900, 127.0200}));

    String caseId = "CASE-" + UUID.randomUUID();
    misdeliveryConfirmed(caseId, origin, r1);
    misdeliveryConfirmed(caseId, origin, r1); // 같은 사건을 다시 받아도 회수는 하나
    awaitSql("SELECT count(*) FROM delivery WHERE case_id = '" + caseId + "'");
    String recovery =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE case_id = ?", String.class, caseId);
    JsonNode created = outbox(recovery, "DeliveryCreated").path("payload");
    assertThat(created.path("deliverySeq").asInt()).isEqualTo(2); // 같은 주문의 두 번째 배송(ADR-0070)
    assertThat(created.path("type").asString()).isEqualTo("RETURN");
    assertThat(created.path("purpose").asString()).isEqualTo("MISDELIVERY_RECOVERY");
    assertThat(created.path("caseId").asString()).isEqualTo(caseId);
    assertThat(created.path("originDeliveryId").asString()).isEqualTo(origin);
    assertThat(created.path("dropAddressRef").asString()).isEqualTo("mfc:" + mfc);
    assertThat(created.path("shipmentId").asString()).isEqualTo("RCV-" + caseId);
    // 아직 픽업 전인 배송·없는 배송의 오배송 확인은 회수를 만들지 않는다
    misdeliveryConfirmed("CASE-" + UUID.randomUUID(), "DLV-none-" + UUID.randomUUID(), r1);
    // 고객 잘못·다시 배송(ADR-0065)은 MFC 회수 없음
    String customerCase = "CASE-" + UUID.randomUUID();
    misdeliveryConfirmed(customerCase, origin, r1, "CUSTOMER", "REDELIVER");

    engine.tick();
    Map<String, Object> row =
        jdbc.queryForMap(
            "SELECT pickup_lat, pickup_source, drop_distance_m, mfc_lat, drop_lat FROM delivery"
                + " WHERE delivery_id = ?",
            recovery);
    assertThat(row.get("pickup_source")).isEqualTo("RIDER_COMPLETION_LOCATION");
    assertThat(((Number) row.get("pickup_lat")).doubleValue()).isEqualTo(37.501);
    assertThat(row.get("drop_distance_m")).isEqualTo(2100);
    assertThat(row.get("drop_lat")).isEqualTo(row.get("mfc_lat")); // 도착지 = MFC
    Map<String, Object> o =
        jdbc.queryForMap(
            "SELECT offer_id, rider_id, fee, bundle_id FROM dispatch_offer WHERE delivery_id = ?"
                + " AND state = 'SENT'",
            recovery);
    assertThat(o.get("rider_id")).isEqualTo(r2);
    assertThat(o.get("bundle_id")).isNull();
    assertThat(o.get("fee"))
        .isEqualTo(
            com.pmart.delivery.dispatch.domain.FeeTable.estimate("MOTORCYCLE", 2100)); // 일반 표
    JsonNode offers =
        body(
            http.get()
                .uri("/offers")
                .header("Authorization", "Bearer " + TestJwt.rider(r2))
                .retrieve()
                .toEntity(String.class));
    JsonNode mine = null;
    for (JsonNode n : offers.path("items")) {
      if (recovery.equals(n.path("deliveryId").asString())) {
        mine = n;
      }
    }
    assertThat(mine).isNotNull();
    assertThat(mine.path("deliveryType").asString()).isEqualTo("RETURN");
    assertThat(mine.path("purpose").asString()).isEqualTo("MISDELIVERY_RECOVERY");
    // 제안만 받은 라이더에게는 회수지 좌표를 아직 보이지 않는다
    JsonNode before = dispatchView(TestJwt.rider(r2), recovery);
    assertThat(before.path("recovery").path("caseId").asString()).isEqualTo(caseId);
    assertThat(
            before.path("recovery").path("pickup").isMissingNode()
                || before.path("recovery").path("pickup").isNull())
        .isTrue();
    // 회수는 배달리스트에 없다
    JsonNode list =
        body(
            http.get()
                .uri("/delivery-list")
                .header("Authorization", "Bearer " + TestJwt.rider(r3))
                .retrieve()
                .toEntity(String.class));
    for (JsonNode n : list.path("items")) {
      assertThat(n.path("deliveryId").asString()).isNotEqualTo(recovery);
    }

    assertThat(
            body(respond(r2, (String) o.get("offer_id"), "a-" + recovery, "ACCEPT"))
                .path("outcome")
                .asString())
        .isEqualTo("ASSIGNED");
    JsonNode after = dispatchView(TestJwt.rider(r2), recovery);
    assertThat(after.path("purpose").asString()).isEqualTo("MISDELIVERY_RECOVERY");
    assertThat(after.path("recovery").path("pickup").path("lat").asDouble()).isEqualTo(37.501);
    assertThat(after.path("recovery").path("pickup").path("source").asString())
        .isEqualTo("RIDER_COMPLETION_LOCATION");
    JsonNode originRider = after.path("recovery").path("originRiderId");
    assertThat(originRider.isMissingNode() || originRider.isNull())
        .isTrue(); // 라이더에게는 원래 라이더를 안 보인다
    JsonNode staff =
        dispatchView(TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of()), recovery);
    assertThat(staff.path("recovery").path("originRiderId").asString()).isEqualTo(r1);

    // 회수(ReturnPickedUp) → PICKED_UP — 픽업 뒤라 라이더 배차 취소는 지원센터로
    executionEvent(
        "ReturnPickedUp",
        recovery,
        Map.of(
            "returnPickupId",
            recovery,
            "deliveryId",
            recovery,
            "riderId",
            r2,
            "pickedUpAt",
            Instant.now().toString(),
            "lines",
            List.of(Map.of("orderLineId", "L-1", "qty", 1))));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + recovery
            + "' AND state = 'PICKED_UP'");
    assertThat(cancel(r2, recovery, "p-" + recovery).getStatusCode().value()).isEqualTo(422);
    executionEvent(
        "ReturnDelivered",
        recovery,
        Map.of(
            "returnPickupId",
            recovery,
            "deliveryId",
            recovery,
            "mfcId",
            mfc,
            "deliveredAt",
            Instant.now().toString(),
            "lines",
            List.of(Map.of("orderLineId", "L-1", "qty", 1))));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + recovery
            + "' AND state = 'CLOSED'");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE type = 'RETURN' AND origin_delivery_id = ?",
                Integer.class,
                origin))
        .isEqualTo(1); // 고객 잘못·다시 배송 사건은 회수를 만들지 않았다
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE case_id = ? AND type = 'RETURN'",
                Integer.class,
                customerCase))
        .isZero(); // 고객 잘못·다시 배송은 MFC 회수가 아니라 다시 배송(ADR-0065)
  }

  /**
   * 고객 반품 회수(CL-1 배송 쪽, 2026-11-17 — 사용자 "바로 배차·일반 표(고객 집 → MFC)·실패마다 알림, 다시 시도는 반품 쪽"): claim
   * ReturnPickupRequested → 회수 배송(RETURN·CLAIM_RETURN) 바로 배차 — 회수지 = 고객 집(원래 배송지 좌표), 예상 배달료 = 일반
   * 표(고객 집 → MFC), 원래 기사도 받을 수 있다. 같은 사건은 하나, 진행 중이면 같은 클레임의 새 요청도 만들지 않는다. 회수
   * 실패(ReturnPickupFailed) → CLOSED, 그 뒤 반품 쪽의 새 요청은 새 회수 배송.
   */
  @Test
  void claimReturnRequestDispatchesPickupFromCustomerHome() {
    String mfc = "MFC-D21";
    String r1 = "RD-cr1-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    String order = "ORD-" + UUID.randomUUID();
    String origin =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-cr-" + UUID.randomUUID(), order);
    engine.tick();
    String offer1 =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            origin);
    assertThat(body(respond(r1, offer1, "a-" + origin, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    executionEvent(
        "DeliveryCompleted",
        origin,
        Map.of(
            "deliveryId",
            origin,
            "riderId",
            r1,
            "deliveredAt",
            Instant.now().minusSeconds(3600).toString(),
            "lines",
            List.of(Map.of("orderLineId", "L-1", "qty", 2)),
            "method",
            "CONTACTLESS"));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '" + origin + "' AND state = 'CLOSED'");

    // 고객 집(37.5006) → MFC 2,400m·600초. 라이더 → 고객 집은 기본(900m·300초)
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.origins[0].lat", equalTo("37.5006")))
            .willReturn(json(matrix(List.of(List.of(2400, 600))))));
    locations(Map.of(r1, new double[] {37.5008, 127.0362}));

    String claimId = "CLM-" + UUID.randomUUID();
    String request = claimReturnRequested(claimId, order, origin, mfc, claimId + "-P1", 1);
    kafka.send("order.claim.v1", claimId, request).join();
    kafka.send("order.claim.v1", claimId, request).join(); // 같은 사건을 다시 받아도 하나
    awaitSql("SELECT count(*) FROM delivery WHERE claim_id = '" + claimId + "'");
    // 진행 중인데 같은 클레임의 새 요청 — 만들지 않는다
    kafka
        .send(
            "order.claim.v1",
            claimId,
            claimReturnRequested(claimId, order, origin, mfc, claimId + "-P2", 2))
        .join();
    String pickup =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE claim_id = ?", String.class, claimId);
    JsonNode created = outbox(pickup, "DeliveryCreated").path("payload");
    assertThat(created.path("type").asString()).isEqualTo("RETURN");
    assertThat(created.path("purpose").asString()).isEqualTo("CLAIM_RETURN");
    assertThat(created.path("claimId").asString()).isEqualTo(claimId);
    assertThat(created.path("originDeliveryId").asString()).isEqualTo(origin);
    assertThat(created.path("dropAddressRef").asString()).isEqualTo("mfc:" + mfc);
    assertThat(created.path("returnLines").get(0).path("qty").asInt()).isEqualTo(2);
    assertThat(created.path("returnLines").get(0).path("claimLineId").asString())
        .isEqualTo("CL-1"); // claim 이 실은 칸은 그대로
    assertThat(created.path("pickupRequestId").asString()).isEqualTo(claimId + "-P1");
    assertThat(created.path("shipmentId").asString()).isEqualTo("CLR-" + claimId + "-P1");

    engine.tick();
    Map<String, Object> row =
        jdbc.queryForMap(
            "SELECT pickup_lat, pickup_source, drop_distance_m, mfc_lat, drop_lat FROM delivery"
                + " WHERE delivery_id = ?",
            pickup);
    assertThat(row.get("pickup_source")).isEqualTo("DROP_ADDRESS"); // 고객 집
    assertThat(((Number) row.get("pickup_lat")).doubleValue()).isEqualTo(37.5006);
    assertThat(row.get("drop_distance_m")).isEqualTo(2400);
    assertThat(row.get("drop_lat")).isEqualTo(row.get("mfc_lat"));
    Map<String, Object> o =
        jdbc.queryForMap(
            "SELECT offer_id, rider_id, fee FROM dispatch_offer WHERE delivery_id = ? AND state ="
                + " 'SENT'",
            pickup);
    assertThat(o.get("rider_id")).isEqualTo(r1); // 원래 기사도 받을 수 있다
    assertThat(o.get("fee"))
        .isEqualTo(com.pmart.delivery.dispatch.domain.FeeTable.estimate("MOTORCYCLE", 2400));
    assertThat(
            body(respond(r1, (String) o.get("offer_id"), "a-" + pickup, "ACCEPT"))
                .path("outcome")
                .asString())
        .isEqualTo("ASSIGNED");
    JsonNode view = dispatchView(TestJwt.rider(r1), pickup);
    assertThat(view.path("purpose").asString()).isEqualTo("CLAIM_RETURN");
    assertThat(view.path("recovery").path("claimId").asString()).isEqualTo(claimId);
    assertThat(view.path("recovery").path("pickup").path("source").asString())
        .isEqualTo("DROP_ADDRESS");

    // 회수 실패 확정(execution) → 배차 끝. 다시 보낼지는 반품 쪽 — 새 요청이면 새 회수 배송
    executionEvent(
        "ReturnPickupFailed",
        pickup,
        Map.of(
            "deliveryId",
            pickup,
            "claimId",
            claimId,
            "reason",
            "CUSTOMER_ABSENT",
            "failedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '" + pickup + "' AND state = 'CLOSED'");
    kafka
        .send(
            "order.claim.v1",
            claimId,
            claimReturnRequested(claimId, order, origin, mfc, claimId + "-P2", 2))
        .join();
    awaitSql(
        "SELECT count(*) FROM delivery WHERE claim_id = '"
            + claimId
            + "' AND pickup_request_id = '"
            + claimId
            + "-P2'");

    // 원래 배송이 전달 완료 전(배차 중)이면 반품 불가(사용자 2026-11-17) — 처리는 하되 만들지 않는다
    String inTransit =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-cr-" + UUID.randomUUID());
    String earlyClaim = "CLM-" + UUID.randomUUID();
    kafka
        .send(
            "order.claim.v1",
            earlyClaim,
            claimReturnRequested(earlyClaim, order, inTransit, mfc, earlyClaim + "-P1", 1))
        .join();
    awaitSql(
        "SELECT count(*) FROM inbox_event WHERE event_type = 'ReturnPickupRequested' AND"
            + " aggregate_id = '"
            + earlyClaim
            + "'");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE claim_id = ?", Integer.class, earlyClaim))
        .isZero();
    // 반품 쪽이 멈추지 않게 회수 실패로 알린다(ORIGIN_NOT_DELIVERED)
    assertThat(
            jdbc.queryForObject(
                "SELECT payload::text FROM outbox_event WHERE type = 'ReturnPickupFailed' AND"
                    + " payload->'payload'->>'claimId' = ?",
                String.class,
                earlyClaim))
        .contains("ORIGIN_NOT_DELIVERED");

    // 회수 요청에 원래 배송이 없어도(claim 은 deliveryId 를 싣지 않는다) 주문의 배송으로 본다
    String order2 = "ORD-" + UUID.randomUUID();
    readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-cr-" + UUID.randomUUID(), order2);
    String noOrigin = "CLM-" + UUID.randomUUID();
    kafka
        .send(
            "order.claim.v1",
            noOrigin,
            claimReturnRequested(noOrigin, order2, null, mfc, noOrigin + "-P1", 1))
        .join();
    awaitSql(
        "SELECT count(*) FROM outbox_event WHERE type = 'ReturnPickupFailed' AND"
            + " payload->'payload'->>'claimId' = '"
            + noOrigin
            + "'");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE claim_id = ?", Integer.class, noOrigin))
        .isZero();
  }

  /**
   * 전달 실패 반납(2026-11-17 — 사용자 "같은 라이더에 회수 배송 자동 배정", "원배송 수행료 + 반납 이동료 따로"): DeliveryFailed custody
   * RIDER → 반납 배송(RETURN·FAILED_DELIVERY_RETURN)을 원래 라이더에게 제안 없이 바로 배정(신규배차 꺼짐이어도), 배달료 = 일반 표(고객 집
   * → MFC). custody STORE 면 만들지 않는다. 같은 사건을 다시 받아도 하나.
   */
  @Test
  void confirmedFailureWithGoodsOnRiderAssignsReturnToSameRider() {
    String mfc = "MFC-D23";
    String r1 = "RD-fr1-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    String order = "ORD-" + UUID.randomUUID();
    String origin =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-fr-" + UUID.randomUUID(), order);
    engine.tick();
    String offer1 =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            origin);
    assertThat(body(respond(r1, offer1, "a-" + origin, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    // 고객 집(37.5006) → MFC 2,400m. 반납 배정 때 라이더는 신규배차를 껐다(그래도 상품을 돌려놓아야 한다)
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.origins[0].lat", equalTo("37.5006")))
            .willReturn(json(matrix(List.of(List.of(2400, 600))))));
    riders(mfc, rider(r1, "AI_RECOMMENDED", false));
    String failed =
        executionEvent(
            "DeliveryFailed",
            origin,
            Map.of(
                "deliveryId",
                origin,
                "reason",
                "CUSTOMER_ABSENT",
                "custody",
                "RIDER",
                "at",
                Instant.now().toString()));
    kafka.send("delivery.delivery.v1", origin, failed).join(); // 다시 받아도 하나
    awaitSql(
        "SELECT count(*) FROM delivery WHERE origin_delivery_id = '"
            + origin
            + "' AND purpose = 'FAILED_DELIVERY_RETURN'");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, origin))
        .isEqualTo("CLOSED");
    String back =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE origin_delivery_id = ? AND purpose ="
                + " 'FAILED_DELIVERY_RETURN'",
            String.class,
            origin);
    JsonNode created = outbox(back, "DeliveryCreated").path("payload");
    assertThat(created.path("type").asString()).isEqualTo("RETURN");
    assertThat(created.path("purpose").asString()).isEqualTo("FAILED_DELIVERY_RETURN");
    assertThat(created.path("originDeliveryId").asString()).isEqualTo(origin);
    assertThat(created.path("dropAddressRef").asString()).isEqualTo("mfc:" + mfc);

    engine.tick();
    Map<String, Object> d =
        jdbc.queryForMap(
            "SELECT state, rider_id, drop_distance_m FROM delivery WHERE delivery_id = ?", back);
    assertThat(d.get("state")).isEqualTo("ASSIGNED");
    assertThat(d.get("rider_id")).isEqualTo(r1);
    assertThat(d.get("drop_distance_m")).isEqualTo(2400);
    Map<String, Object> o =
        jdbc.queryForMap(
            "SELECT source, state, fee FROM dispatch_offer WHERE delivery_id = ?", back);
    assertThat(o.get("source")).isEqualTo("FAILED_RETURN");
    assertThat(o.get("state")).isEqualTo("ACCEPTED");
    assertThat(o.get("fee"))
        .isEqualTo(com.pmart.delivery.dispatch.domain.FeeTable.estimate("MOTORCYCLE", 2400));
    JsonNode assigned = outbox(back, "DeliveryAssigned").path("payload");
    assertThat(assigned.path("riderId").asString()).isEqualTo(r1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE type = 'DispatchOfferSent' AND"
                    + " payload->'payload'->>'deliveryId' = ?",
                Integer.class,
                back))
        .isZero(); // 제안(푸시) 없이

    // 다른 라이더(2026-11-17 — returnBy OTHER_RIDER): 원래 라이더는 빼고 근처 라이더에게 회수 제안, 회수지 = 원래 라이더 위치
    String r2 = "RD-fr2-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true), rider(r2, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.5006, 127.0362}, r2, new double[] {37.5008, 127.0365}));
    // 알뜰 주문 — 반납 배송을 든 라이더도 받을 수 있게(한집은 단독, 2026-11-18)
    bundledOrder(order, "IT-ROAD-FR3-" + order, 37.5006, 127.0360);
    String origin3 =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-fr-" + UUID.randomUUID(), order);
    engine.tick();
    String offer3 =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            origin3);
    String r3rider =
        jdbc.queryForObject(
            "SELECT rider_id FROM dispatch_offer WHERE offer_id = ?", String.class, offer3);
    assertThat(body(respond(r3rider, offer3, "a-" + origin3, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    executionEvent(
        "DeliveryFailed",
        origin3,
        Map.of(
            "deliveryId",
            origin3,
            "reason",
            "RIDER_INCIDENT",
            "custody",
            "RIDER",
            "returnBy",
            "OTHER_RIDER",
            "failedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE origin_delivery_id = '"
            + origin3
            + "' AND request_trigger = 'FAILURE_OTHER'");
    String back3 =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE origin_delivery_id = ?", String.class, origin3);
    engine.tick();
    Map<String, Object> o3 =
        jdbc.queryForMap(
            "SELECT rider_id, state FROM dispatch_offer WHERE delivery_id = ? ORDER BY sent_at DESC"
                + " LIMIT 1",
            back3);
    assertThat(o3.get("state")).isEqualTo("SENT"); // 제안(바로 배정 아님)
    assertThat(o3.get("rider_id")).isNotEqualTo(r3rider); // 원래 라이더 제외
    assertThat(
            jdbc.queryForObject(
                "SELECT pickup_source FROM delivery WHERE delivery_id = ?", String.class, back3))
        .isEqualTo("RIDER_LOCATION");
    // 배차 상세가 이 출처를 계약 enum 으로 낸다(2026-11-19 — 빠져 있어 500 이었음)
    assertThat(
            http.get()
                .uri("/deliveries/{id}/dispatch", back3)
                .header(
                    "Authorization",
                    "Bearer " + TestJwt.operator("OP-ADM", List.of("OPERATOR_ADMIN"), List.of()))
                .retrieve()
                .toEntity(String.class)
                .getStatusCode()
                .value())
        .isEqualTo(200);

    // 실패 확정 되돌리기(2026-11-19): 원래 배송 다시 PICKED_UP(같은 라이더), 반납 배송(제안 중)은 픽업 전 취소
    executionEvent(
        "DeliveryFailureReverted",
        origin3,
        Map.of(
            "deliveryId",
            origin3,
            "deliveryType",
            "FORWARD",
            "originalReason",
            "RIDER_INCIDENT",
            "restoredState",
            "PICKED_UP",
            "returnDeliveryId",
            back3,
            "revertedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + back3
            + "' AND state = 'CANCELLED_BEFORE_PICKUP'");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, origin3))
        .isEqualTo("PICKED_UP");
    assertThat(
            jdbc.queryForObject(
                "SELECT rider_id FROM delivery WHERE delivery_id = ?", String.class, origin3))
        .isEqualTo(r3rider);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type ="
                    + " 'DeliveryCancelledBeforePickup'",
                Integer.class,
                back3))
        .isEqualTo(1);

    // 다른 라이더 반납을 5분 동안 못 맡기면 원래 라이더에게 바로(2026-11-20 사용자 "가" → "5분 안으로")
    String order4 = "ORD-" + UUID.randomUUID();
    bundledOrder(order4, "IT-ROAD-FR4-" + order4, 37.5006, 127.0360);
    String origin4 =
        readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-fr-" + UUID.randomUUID(), order4);
    engine.tick();
    String offer4 =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            origin4);
    String r4rider =
        jdbc.queryForObject(
            "SELECT rider_id FROM dispatch_offer WHERE offer_id = ?", String.class, offer4);
    assertThat(body(respond(r4rider, offer4, "a-" + origin4, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    executionEvent(
        "DeliveryFailed",
        origin4,
        Map.of(
            "deliveryId",
            origin4,
            "reason",
            "CUSTOMER_ABSENT",
            "custody",
            "RIDER",
            "returnBy",
            "OTHER_RIDER",
            "failedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE origin_delivery_id = '"
            + origin4
            + "' AND request_trigger = 'FAILURE_OTHER'");
    String back4 =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE origin_delivery_id = ?", String.class, origin4);
    jdbc.update(
        "UPDATE delivery SET created_at = now() - interval '6 minutes' WHERE delivery_id = ?",
        back4);
    engine.tick();
    Map<String, Object> d4 =
        jdbc.queryForMap(
            "SELECT state, rider_id, request_trigger FROM delivery WHERE delivery_id = ?", back4);
    assertThat(d4.get("state")).isEqualTo("ASSIGNED");
    assertThat(d4.get("rider_id")).isEqualTo(r4rider); // 상품을 든 원래 라이더
    assertThat(d4.get("request_trigger")).isEqualTo("FAILURE");
    assertThat(
            jdbc.queryForObject(
                "SELECT source FROM dispatch_offer WHERE delivery_id = ? AND state = 'ACCEPTED'",
                String.class,
                back4))
        .isEqualTo("FAILED_RETURN");

    // 상품이 매장(custody STORE)이면 라이더 할 일 없음
    String origin2 = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-fr-" + UUID.randomUUID());
    executionEvent(
        "DeliveryFailed",
        origin2,
        Map.of(
            "deliveryId",
            origin2,
            "reason",
            "DAMAGED",
            "custody",
            "STORE",
            "at",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM inbox_event WHERE event_type = 'DeliveryFailed' AND aggregate_id = '"
            + origin2
            + "'");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE origin_delivery_id = ?",
                Integer.class,
                origin2))
        .isZero();
  }

  String claimReturnRequested(
      String claimId,
      String orderId,
      String deliveryId,
      String mfcId,
      String pickupRequestId,
      int attempt) {
    Map<String, Object> payload = new java.util.LinkedHashMap<>();
    payload.put("claimId", claimId);
    payload.put("orderId", orderId);
    if (deliveryId != null) {
      payload.put("deliveryId", deliveryId);
    }
    payload.put("mfcId", mfcId);
    payload.put("pickupRequestId", pickupRequestId);
    payload.put("attempt", attempt);
    payload.put("purpose", "CLAIM_RETURN");
    payload.put("lines", List.of(Map.of("orderLineId", "L-1", "qty", 2, "claimLineId", "CL-1")));
    payload.put("pickupAddressRef", "order:" + orderId);
    payload.put("requestedAt", Instant.now().toString());
    return EventEnvelopeFixtures.envelope("ReturnPickupRequested")
        .from("claim-service")
        .aggregate("Claim", claimId, 1 + attempt)
        .payload(payload)
        .buildJson();
  }

  /**
   * 오배송 재발송(2026-11-16, ADR-0052 결정 4): MFC 재발송 출고(purpose MISDELIVERY_RESHIPMENT·caseId)로 만든 배송은
   * RESHIPMENT, DeliveryCreated 에 purpose·caseId, 약속 = 같은 사건이 처음 보인 때 + 50분. 내부 배정 조회에
   * orderId·mfcId.
   */
  @Test
  void reshipmentShipmentBecomesReshipmentDeliveryWithFreshPromise() {
    String mfc = "MFC-D21";
    String r1 = "RD-rs-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    String caseId = "CASE-" + UUID.randomUUID();
    String order = "ORD-" + UUID.randomUUID();
    String shipment = order + "-RS-1a2b3c4d";
    Map<String, Object> ready = new LinkedHashMap<>();
    ready.put("orderId", order);
    ready.put("shipmentId", shipment);
    ready.put("mfcId", mfc);
    ready.put("readyAt", Instant.now().toString());
    ready.put(
        "packages",
        List.of(
            Map.of(
                "packageId", "PKG-rs-" + caseId, "bagCount", 1, "weightG", 500, "alcohol", false)));
    ready.put("purpose", "MISDELIVERY_RESHIPMENT");
    ready.put("caseId", caseId);
    send("ShipmentReady", shipment, ready);
    String deliveryId = awaitDelivery(shipment);
    Map<String, Object> req = new LinkedHashMap<>();
    req.put("orderId", order);
    req.put("shipmentId", shipment);
    req.put("mfcId", mfc);
    req.put("trigger", "MANUAL");
    req.put("estimatedReadyAt", Instant.now().toString());
    req.put("requestedAt", Instant.now().toString());
    req.put("purpose", "MISDELIVERY_RESHIPMENT");
    req.put("caseId", caseId);
    send("DispatchRequested", shipment, req);
    awaitSql(
        "SELECT count(*) FROM outbox_event WHERE aggregateid = '"
            + deliveryId
            + "' AND type = 'DeliveryCreated'");
    Map<String, Object> row =
        jdbc.queryForMap(
            "SELECT type, purpose, case_id, created_at FROM delivery WHERE delivery_id = ?",
            deliveryId);
    assertThat(row.get("type")).isEqualTo("RESHIPMENT");
    assertThat(row.get("purpose")).isEqualTo("MISDELIVERY_RESHIPMENT");
    assertThat(row.get("case_id")).isEqualTo(caseId);
    JsonNode created = outbox(deliveryId, "DeliveryCreated").path("payload");
    assertThat(created.path("type").asString()).isEqualTo("RESHIPMENT");
    assertThat(created.path("purpose").asString()).isEqualTo("MISDELIVERY_RESHIPMENT");
    assertThat(created.path("caseId").asString()).isEqualTo(caseId);
    assertThat(created.path("dropAddressRef").asString()).isEqualTo("order:" + order);

    engine.tick(); // 좌표 준비 — 약속은 원래 주문 확정이 아니라 이 사건 기준
    java.sql.Timestamp promised =
        jdbc.queryForObject(
            "SELECT promised_at FROM delivery WHERE delivery_id = ?",
            java.sql.Timestamp.class,
            deliveryId);
    assertThat(promised.toInstant())
        .isEqualTo(((java.sql.Timestamp) row.get("created_at")).toInstant().plusSeconds(3000));
    String offerId =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            deliveryId);
    assertThat(body(respond(r1, offerId, "a-" + deliveryId, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    JsonNode a =
        body(
            http.get()
                .uri("/internal/deliveries/{id}/assignment", deliveryId)
                .retrieve()
                .toEntity(String.class));
    assertThat(a.path("orderId").asString()).isEqualTo(order);
    assertThat(a.path("mfcId").asString()).isEqualTo(mfc);
    JsonNode view =
        dispatchView(TestJwt.operator("OP-RO", List.of("RIDER_OPS"), List.of()), deliveryId);
    assertThat(view.path("type").asString()).isEqualTo("RESHIPMENT");
    assertThat(view.path("purpose").asString()).isEqualTo("MISDELIVERY_RESHIPMENT");
  }

  /** 원래 배송 하나를 r1 이 배정받아 전달 완료까지(배차 쪽 기록). 완료 시각을 돌려준다. */
  Instant completedOrigin(String mfc, String r1, String origin) {
    String offerId =
        jdbc.queryForObject(
            "SELECT offer_id FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            String.class,
            origin);
    assertThat(body(respond(r1, offerId, "a-" + origin, "ACCEPT")).path("outcome").asString())
        .isEqualTo("ASSIGNED");
    Instant deliveredAt =
        Instant.now().minusSeconds(600).truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    executionEvent(
        "DeliveryCompleted",
        origin,
        Map.of(
            "deliveryId",
            origin,
            "riderId",
            r1,
            "deliveredAt",
            deliveredAt.toString(),
            "lines",
            List.of(Map.of("orderLineId", "L-1", "qty", 1)),
            "method",
            "CONTACTLESS"));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + origin
            + "' AND state = 'CLOSED' AND completed_at IS NOT NULL");
    return deliveredAt;
  }

  /**
   * ADR-0065 고객 잘못 다시 배송: 잘못 둔 곳 ↔ 실제 집 200m 미만이면 원래 기사에게 먼저(배달료 0), 200m 이상이면 9,000원 결제 대기(사건만 내고
   * 배차 안 함). MFC 회수는 없다.
   */
  @Test
  void customerFaultRedeliveryOriginalRiderWithin200mOrWaitsForPayment() {
    String mfc = "MFC-D22";
    String r1 = "RD-rd1-" + UUID.randomUUID();
    riders(mfc, rider(r1, "AI_RECOMMENDED", true));
    riderLookup(rider(r1, "AI_RECOMMENDED", true));
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    String origin = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-rd-" + UUID.randomUUID());
    engine.tick();
    Instant done = completedOrigin(mfc, r1, origin);
    String orderId =
        jdbc.queryForObject(
            "SELECT order_id FROM delivery WHERE delivery_id = ?", String.class, origin);
    // 잘못 둔 곳 = r1 완료 위치(실제 집 좌표 37.5006,127.0360 과 약 60m), 실제 집 주소는 order 에
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/riders/" + r1 + "/location-at"))
            .withQueryParam("at", equalTo(done.toString()))
            .willReturn(
                json(
                    Map.of(
                        "riderId",
                        r1,
                        "lat",
                        37.501,
                        "lng",
                        127.0365,
                        "recordedAt",
                        done.toString(),
                        "quality",
                        "GOOD"))));
    up.stubFor(
        get(urlPathMatching("/api/v1/internal/orders/.*/redelivery-destinations/RDD-NEAR"))
            .willReturn(json(Map.of("destinationId", "RDD-NEAR", "roadAddress", "IT-HOME"))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.origins[0].lat", equalTo("37.501")))
            .willReturn(json(matrix(List.of(List.of(120, 60))))));
    locations(Map.of(r1, new double[] {37.5012, 127.0366}));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(2)
            .withRequestBody(matchingJsonPath("$.destinations[0].lat", equalTo("37.501")))
            .willReturn(json(matrix(List.of(List.of(30, 20))))));
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
                    origin,
                    "riderId",
                    r1,
                    "confirmedBy",
                    "OP-CS",
                    "confirmedAt",
                    Instant.now().toString(),
                    "fault",
                    "CUSTOMER",
                    "resolution",
                    "REDELIVER",
                    "redeliveryDestinationId",
                    "RDD-NEAR"))
            .buildJson();
    kafka.send("platform.case.v1", caseId, json).join();
    awaitSql("SELECT count(*) FROM delivery WHERE case_id = '" + caseId + "'");
    Map<String, Object> rd =
        jdbc.queryForMap(
            "SELECT delivery_id, type, purpose FROM delivery WHERE case_id = ?", caseId);
    String redelivery = (String) rd.get("delivery_id");
    assertThat(rd.get("type")).isEqualTo("RESHIPMENT");
    assertThat(rd.get("purpose")).isEqualTo("MISDELIVERY_REDELIVERY");
    JsonNode created = outbox(redelivery, "DeliveryCreated").path("payload");
    assertThat(created.path("dropAddressRef").asString())
        .isEqualTo("order:" + orderId + ":redelivery:RDD-NEAR");
    engine.tick();
    Map<String, Object> geo =
        jdbc.queryForMap(
            "SELECT straight_distance_m, payment_required, drop_distance_m FROM delivery WHERE"
                + " delivery_id = ?",
            redelivery);
    assertThat((Integer) geo.get("straight_distance_m")).isLessThan(200);
    assertThat(geo.get("payment_required")).isEqualTo(false);
    Map<String, Object> o =
        jdbc.queryForMap(
            "SELECT rider_id, fee FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            redelivery);
    assertThat(o.get("rider_id")).isEqualTo(r1); // 원래 기사
    assertThat(o.get("fee")).isEqualTo(0); // 추가 운행료 없음
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM delivery WHERE type = 'RETURN' AND origin_delivery_id = ?",
                Integer.class,
                origin))
        .isZero(); // MFC 회수 없음

    // 200m 이상 — 결제 대기(사건 RedeliveryPaymentRequired 9,000원), 배차 안 함
    String origin2 = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-rd2-" + UUID.randomUUID());
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    jdbc.update("UPDATE dispatch_offer SET state = 'EXPIRED' WHERE delivery_id = ?", redelivery);
    engine.tick();
    Instant done2 = completedOrigin(mfc, r1, origin2);
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/riders/" + r1 + "/location-at"))
            .withQueryParam("at", equalTo(done2.toString()))
            .willReturn(
                json(
                    Map.of(
                        "riderId",
                        r1,
                        "lat",
                        37.512,
                        "lng",
                        127.0365,
                        "recordedAt",
                        done2.toString(),
                        "quality",
                        "GOOD"))));
    up.stubFor(
        get(urlPathMatching("/api/v1/internal/orders/.*/redelivery-destinations/RDD-FAR"))
            .willReturn(json(Map.of("destinationId", "RDD-FAR", "roadAddress", "IT-HOME"))));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.origins[0].lat", equalTo("37.512")))
            .willReturn(json(matrix(List.of(List.of(1500, 400))))));
    String case2 = "CASE-" + UUID.randomUUID();
    kafka
        .send(
            "platform.case.v1",
            case2,
            EventEnvelopeFixtures.envelope("MisdeliveryConfirmed")
                .from("case-service")
                .aggregate("Case", case2, 1)
                .payload(
                    Map.of(
                        "caseId",
                        case2,
                        "deliveryId",
                        origin2,
                        "riderId",
                        r1,
                        "confirmedBy",
                        "OP-CS",
                        "confirmedAt",
                        Instant.now().toString(),
                        "fault",
                        "CUSTOMER",
                        "resolution",
                        "REDELIVER",
                        "redeliveryDestinationId",
                        "RDD-FAR"))
                .buildJson())
        .join();
    awaitSql("SELECT count(*) FROM delivery WHERE case_id = '" + case2 + "'");
    String redelivery2 =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE case_id = ?", String.class, case2);
    engine.tick();
    assertThat(
            jdbc.queryForObject(
                "SELECT payment_required FROM delivery WHERE delivery_id = ?",
                Boolean.class,
                redelivery2))
        .isTrue();
    JsonNode pay = outbox(redelivery2, "RedeliveryPaymentRequired").path("payload");
    assertThat(pay.path("fee").asInt()).isEqualTo(9000);
    assertThat(pay.path("caseId").asString()).isEqualTo(case2);
    assertThat(pay.path("straightDistanceM").asInt()).isGreaterThanOrEqualTo(200);
    engine.tick();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM dispatch_offer WHERE delivery_id = ?",
                Integer.class,
                redelivery2))
        .isZero(); // 결제 전 배차 없음

    // 4단계 — 결제 안 됨(30분·거절): 다시 배송 픽업 전 취소 + MFC 회수 배송
    String origin3 = readyShipment(mfc, "SHP-" + UUID.randomUUID(), "PKG-rd3-" + UUID.randomUUID());
    locations(Map.of(r1, new double[] {37.4985, 127.0280}));
    engine.tick();
    Instant done3 = completedOrigin(mfc, r1, origin3);
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/riders/" + r1 + "/location-at"))
            .withQueryParam("at", equalTo(done3.toString()))
            .willReturn(
                json(
                    Map.of(
                        "riderId",
                        r1,
                        "lat",
                        37.512,
                        "lng",
                        127.0365,
                        "recordedAt",
                        done3.toString(),
                        "quality",
                        "GOOD"))));
    String case3 = "CASE-" + UUID.randomUUID();
    kafka
        .send(
            "platform.case.v1",
            case3,
            EventEnvelopeFixtures.envelope("MisdeliveryConfirmed")
                .from("case-service")
                .aggregate("Case", case3, 1)
                .payload(
                    Map.of(
                        "caseId",
                        case3,
                        "deliveryId",
                        origin3,
                        "riderId",
                        r1,
                        "confirmedBy",
                        "OP-CS",
                        "confirmedAt",
                        Instant.now().toString(),
                        "fault",
                        "CUSTOMER",
                        "resolution",
                        "REDELIVER",
                        "redeliveryDestinationId",
                        "RDD-FAR"))
                .buildJson())
        .join();
    awaitSql("SELECT count(*) FROM delivery WHERE case_id = '" + case3 + "'");
    String redelivery3 =
        jdbc.queryForObject(
            "SELECT delivery_id FROM delivery WHERE case_id = ?", String.class, case3);
    engine.tick();
    assertThat(outbox(redelivery3, "RedeliveryPaymentRequired").path("payload").path("fee").asInt())
        .isEqualTo(9000);
    JsonNode cancelled =
        body(redeliveryCommand(redelivery3, "redelivery-cancellations", case3, "TIMED_OUT"));
    String recovery3 = cancelled.path("resultId").asString();
    assertThat(cancelled.path("status").asString()).isEqualTo("APPLIED");
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, redelivery3))
        .isEqualTo("CANCELLED_BEFORE_PICKUP");
    assertThat(
            outbox(redelivery3, "DeliveryCancelledBeforePickup")
                .path("payload")
                .path("reason")
                .asString())
        .isEqualTo("MISDELIVERY_REDELIVERY_TIMED_OUT");
    Map<String, Object> rec =
        jdbc.queryForMap(
            "SELECT type, purpose, origin_delivery_id FROM delivery WHERE delivery_id = ?",
            recovery3);
    assertThat(rec.get("type")).isEqualTo("RETURN");
    assertThat(rec.get("purpose")).isEqualTo("MISDELIVERY_RECOVERY");
    assertThat(rec.get("origin_delivery_id")).isEqualTo(origin3);
    // 같은 요청 다시 → 같은 회수 배송, 결제 풀기는 409
    assertThat(
            body(redeliveryCommand(redelivery3, "redelivery-cancellations", case3, "TIMED_OUT"))
                .path("resultId")
                .asString())
        .isEqualTo(recovery3);
    assertThat(
            redeliveryCommand(redelivery3, "redelivery-releases", case3, null)
                .getStatusCode()
                .value())
        .isEqualTo(409);

    // 4단계 — 결제됨: 배차 풀기 → 근처 라이더(원래 기사도 됨, 일반 배달료), 약속 = 풀린 때 + 50분
    locations(Map.of(r1, new double[] {37.5121, 127.0366}));
    up.stubFor(
        post(urlPathEqualTo("/api/v1/internal/distance-matrix"))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.destinations[0].lat", equalTo("37.512")))
            .willReturn(json(matrix(List.of(List.of(30, 20))))));
    assertThat(
            body(redeliveryCommand(redelivery2, "redelivery-releases", case2, null))
                .path("resultId")
                .asString())
        .isEqualTo(redelivery2);
    engine.tick();
    Map<String, Object> released =
        jdbc.queryForMap(
            "SELECT released_at, promised_at FROM delivery WHERE delivery_id = ?", redelivery2);
    assertThat(released.get("released_at")).isNotNull();
    assertThat(released.get("promised_at")).isNotNull();
    Map<String, Object> paidOffer =
        jdbc.queryForMap(
            "SELECT rider_id, fee FROM dispatch_offer WHERE delivery_id = ? AND state = 'SENT'",
            redelivery2);
    assertThat(paidOffer.get("rider_id")).isEqualTo(r1);
    assertThat((Integer) paidOffer.get("fee")).isPositive();
    assertThat(
            redeliveryCommand(redelivery2, "redelivery-cancellations", case2, "DECLINED")
                .getStatusCode()
                .value())
        .isEqualTo(409); // 이미 풀림
  }

  /** orchestration → dispatch 다시 배송 결제 명령(내부). 4xx 도 응답으로. */
  ResponseEntity<String> redeliveryCommand(
      String deliveryId,
      String command,
      String caseId,
      @org.jspecify.annotations.Nullable String reason) {
    Map<String, Object> b = new java.util.LinkedHashMap<>();
    b.put("caseId", caseId);
    if (reason != null) {
      b.put("reason", reason);
    }
    return http.post()
        .uri("/internal/deliveries/{id}/" + command, deliveryId)
        .header("Idempotency-Key", UUID.randomUUID().toString())
        .header("X-Source-System", "order-orchestration-service")
        .body(b)
        .exchange(
            (req, res) ->
                ResponseEntity.status(res.getStatusCode())
                    .body(
                        new String(
                            res.getBody().readAllBytes(),
                            java.nio.charset.StandardCharsets.UTF_8)));
  }

  void misdeliveryConfirmed(
      String caseId, String deliveryId, String riderId, String fault, String resolution) {
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
                    "riderId",
                    riderId,
                    "confirmedBy",
                    "OP-CS",
                    "confirmedAt",
                    Instant.now().toString(),
                    "fault",
                    fault,
                    "resolution",
                    resolution,
                    "redeliveryDestinationId",
                    "RDD-1"))
            .buildJson();
    kafka.send("platform.case.v1", caseId, json).join();
  }

  void misdeliveryConfirmed(String caseId, String deliveryId, String riderId) {
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
                    "riderId",
                    riderId,
                    "confirmedBy",
                    "OP-CS",
                    "confirmedAt",
                    Instant.now().toString()))
            .buildJson();
    kafka.send("platform.case.v1", caseId, json).join();
  }

  String executionEvent(String type, String deliveryId, Map<String, Object> payload) {
    String json =
        EventEnvelopeFixtures.envelope(type)
            .from("delivery-execution-service")
            .aggregate("Delivery", deliveryId, 9)
            .payload(payload)
            .buildJson();
    kafka.send("delivery.delivery.v1", deliveryId, json).join();
    return json;
  }

  JsonNode dispatchView(String token, String deliveryId) {
    return body(
        http.get()
            .uri("/deliveries/{id}/dispatch", deliveryId)
            .header("Authorization", "Bearer " + token)
            .retrieve()
            .toEntity(String.class));
  }

  ResponseEntity<String> cancel(String rider, String deliveryId, String key) {
    return http.post()
        .uri("/deliveries/{id}/assignment-cancellations", deliveryId)
        .headers(h -> riderHeaders(h, rider, key))
        .body(Map.of("requestedBy", "RIDER", "reason", "RIDER_UNABLE"))
        .retrieve()
        .toEntity(String.class);
  }

  // ---------- 도우미 ----------

  String readyShipment(String mfc, String shipment, String packageId) {
    return readyShipment(mfc, shipment, packageId, "ORD-" + UUID.randomUUID());
  }

  String readyShipment(String mfc, String shipment, String packageId, String order) {
    send(
        "ShipmentReady",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "readyAt",
            Instant.now().toString(),
            "packages",
            List.of(
                Map.of("packageId", packageId, "bagCount", 1, "weightG", 1000, "alcohol", false))));
    String deliveryId = awaitDelivery(shipment);
    // 출하 대기만으로는 배차하지 않는다 — 점원 요청만(BR-51, 사용자 2026-11-13 "자동요청빼")
    assertThat(
            jdbc.queryForObject(
                "SELECT state FROM delivery WHERE delivery_id = ?", String.class, deliveryId))
        .isEqualTo("CREATED");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM dispatch_attempt WHERE delivery_id = ?",
                Integer.class,
                deliveryId))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE aggregateid = ? AND type = 'DeliveryCreated'",
                Integer.class,
                deliveryId))
        .isZero();
    send(
        "DispatchRequested",
        shipment,
        Map.of(
            "orderId",
            order,
            "shipmentId",
            shipment,
            "mfcId",
            mfc,
            "trigger",
            "MANUAL",
            "estimatedPackMinutes",
            1,
            "estimatedReadyAt",
            Instant.now().toString(),
            "requestedAt",
            Instant.now().toString()));
    awaitSql(
        "SELECT count(*) FROM delivery WHERE delivery_id = '"
            + deliveryId
            + "' AND state = 'DISPATCHING'");
    assertThat(outbox(deliveryId, "DeliveryCreated").path("payload").path("shipmentId").asString())
        .isEqualTo(shipment);
    return deliveryId;
  }

  List<String> listIds(String rider) {
    JsonNode n =
        body(
            http.get()
                .uri("/delivery-list")
                .header("Authorization", "Bearer " + TestJwt.rider(rider))
                .retrieve()
                .toEntity(String.class));
    List<String> out = new ArrayList<>();
    n.path("items").forEach(i -> out.add(i.path("deliveryId").asString()));
    return out;
  }

  ResponseEntity<String> respond(String rider, String offerId, String key, String response) {
    return http.post()
        .uri("/offers/{id}/responses", offerId)
        .headers(h -> riderHeaders(h, rider, key))
        .body(Map.of("response", response))
        .retrieve()
        .toEntity(String.class);
  }

  void orderCancelled(String orderId, String scope) {
    String json =
        EventEnvelopeFixtures.envelope("OrderCancelled")
            .from("order-service")
            .aggregate("Order", orderId, scope.equals("FULL") ? 3 : 2)
            .payload(
                Map.of(
                    "orderId",
                    orderId,
                    "scope",
                    scope,
                    "lines",
                    List.of(Map.of("orderLineId", "L-1", "qty", 1)),
                    "reason",
                    "SHORTAGE",
                    "cancelledAt",
                    Instant.now().toString()))
            .buildJson();
    kafka.send("order.order.v1", orderId, json).join();
  }

  void send(String type, String key, Map<String, Object> payload) {
    String aggregateType =
        switch (type) {
          case "PackageHandedOver", "PackageInvalidated" -> "Package";
          case "WorkStopResult" -> "Fulfillment";
          default -> "Shipment";
        };
    String json =
        EventEnvelopeFixtures.envelope(type)
            .from("mfc-fulfillment-service")
            .aggregate(aggregateType, key, 1)
            .payload(payload)
            .buildJson();
    kafka.send("mfc.fulfillment.v1", key, json).join();
  }

  @SafeVarargs
  final void riders(String mfc, Map<String, Object>... items) {
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/riders/eligible"))
            .withQueryParam("mfcId", equalTo(mfc))
            .willReturn(json(Map.of("items", List.of(items)))));
  }

  @SafeVarargs
  final void riderLookup(Map<String, Object>... items) {
    for (Map<String, Object> r : items) {
      up.stubFor(
          get(urlPathEqualTo("/api/v1/internal/riders/" + r.get("riderId"))).willReturn(json(r)));
    }
  }

  void locations(Map<String, double[]> at) {
    List<Map<String, Object>> items = new ArrayList<>();
    at.forEach(
        (id, ll) ->
            items.add(
                Map.of(
                    "riderId",
                    id,
                    "lat",
                    ll[0],
                    "lng",
                    ll[1],
                    "receivedAt",
                    Instant.now().toString(),
                    "quality",
                    "GOOD")));
    for (String id : at.keySet()) {
      up.stubFor(
          get(urlPathEqualTo("/api/v1/internal/rider-locations"))
              .atPriority(1)
              .withQueryParam("riderIds", equalTo(id))
              .willReturn(
                  json(
                      Map.of(
                          "items",
                          items.stream().filter(i -> i.get("riderId").equals(id)).toList()))));
    }
    up.stubFor(
        get(urlPathEqualTo("/api/v1/internal/rider-locations"))
            .atPriority(5)
            .willReturn(json(Map.of("items", items))));
  }

  static Map<String, Object> rider(String id, String mode, boolean accepts) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("riderId", id);
    m.put("vehicleType", "MOTORCYCLE");
    m.put("dispatchMode", mode);
    m.put("oneAtATime", false);
    m.put("restriction", "NORMAL");
    m.put(
        "operating",
        Map.of("status", accepts ? "ON_DUTY" : "OFF_DUTY", "newDispatchEnabled", accepts));
    return m;
  }

  static Map<String, Object> matrix(List<List<Integer>> rows) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (List<Integer> r : rows) {
      out.add(
          Map.of(
              "elements",
              List.of(Map.of("status", "OK", "distanceM", r.get(0), "durationSeconds", r.get(1)))));
    }
    return Map.of("provider", "KAKAO_MOBILITY", "fallback", false, "rows", out);
  }

  String awaitDelivery(String shipment) {
    awaitSql("SELECT count(*) FROM delivery WHERE shipment_id = '" + shipment + "'");
    return jdbc.queryForObject(
        "SELECT delivery_id FROM delivery WHERE shipment_id = ?", String.class, shipment);
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

  JsonNode outbox(String deliveryId, String type) {
    String payload =
        jdbc.queryForObject(
            "SELECT payload::text FROM outbox_event WHERE aggregateid = ? AND type = ? ORDER BY created_at DESC LIMIT 1",
            String.class,
            deliveryId,
            type);
    return mapper.readTree(payload);
  }

  static void riderHeaders(org.springframework.http.HttpHeaders h, String rider, String key) {
    h.setBearerAuth(TestJwt.rider(rider));
    h.set("Idempotency-Key", key);
    h.set("X-Source-System", "rider-app");
  }

  JsonNode body(ResponseEntity<String> r) {
    return mapper.readTree(r.getBody() == null ? "{}" : r.getBody());
  }

  /** 시험의 시각은 JVM 시계로(Docker DB 시계와 어긋날 수 있어 DB now() 를 쓰지 않는다). */
  static java.sql.Timestamp ago(long seconds) {
    return java.sql.Timestamp.from(Instant.now().minusSeconds(seconds));
  }

  ResponseDefinitionBuilder json(Object body) {
    return aResponse()
        .withHeader("Content-Type", "application/json")
        .withBody(mapper.writeValueAsString(body));
  }
}
