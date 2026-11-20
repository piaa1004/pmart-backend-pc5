package com.pmart.delivery.execution.application;

import com.pmart.common.messaging.EventEnvelope;
import com.pmart.delivery.execution.domain.ExecutionRules;
import com.pmart.delivery.execution.infrastructure.ExecutionStore;
import com.pmart.delivery.execution.infrastructure.ExecutionStore.DeliveryRow;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 구독 사건 → 배송 수행. inbox 트랜잭션 안에서 부른다(외부 호출 없음).
 *
 * <ul>
 *   <li>delivery.delivery.v1(dispatch): {@code DeliveryCreated} → UNASSIGNED, {@code
 *       DeliveryAssigned} → ASSIGNED, {@code DeliveryAssignmentChanged} → 다른 라이더 ASSIGNED / 해제면
 *       UNASSIGNED(픽업 전만), {@code DeliveryCancelledBeforePickup} → CANCELLED(픽업 전만). 배정 버전이 낮은 사건은
 *       버린다.
 *   <li>mfc.fulfillment.v1: {@code ShipmentReady}·{@code PackingFinished} → 출고 포장 목록, {@code
 *       PackageHandedOver} → 인계 사실(같은 handoverId 한 번). 그 뒤 픽업 판정({@link PickupJudgement}).
 * </ul>
 */
@Component
public class ExecutionEventHandler {

  public static final Set<String> DELIVERY_TYPES =
      Set.of(
          "DeliveryCreated",
          "DeliveryAssigned",
          "DeliveryAssignmentChanged",
          "DeliveryCancelledBeforePickup");

  public static final Set<String> FULFILLMENT_TYPES =
      Set.of("ShipmentReady", "PackingFinished", "PackageHandedOver", "PackageInvalidated");

  private static final Logger log = LoggerFactory.getLogger(ExecutionEventHandler.class);

  private final ExecutionStore store;
  private final PickupJudgement pickup;
  private final EarningService earnings;
  private final ObjectMapper mapper;
  private final Clock clock;
  private final com.pmart.delivery.execution.infrastructure.OrderClient orders;
  private final FailureDecisionService failures;

  public ExecutionEventHandler(
      ExecutionStore store,
      PickupJudgement pickup,
      EarningService earnings,
      ObjectMapper mapper,
      Clock clock,
      com.pmart.delivery.execution.infrastructure.OrderClient orders,
      FailureDecisionService failures) {
    this.failures = failures;
    this.store = store;
    this.pickup = pickup;
    this.earnings = earnings;
    this.mapper = mapper;
    this.clock = clock;
    this.orders = orders;
  }

  /**
   * case 오배송 확인(platform.case.v1 MisdeliveryConfirmed, ADR-0065): 귀책 RIDER 면 원래 배송 수행료
   * 미지급(SYSTEM·CUSTOMER 는 지급). inbox 트랜잭션 안.
   */
  /** 사건함 사건(platform.case.v1) — 오배송 확인(라이더 귀책 수행료), 사건 열림·닫힘(도움요청 사건 번호·상태, DE-CASE). */
  public void onCaseEvent(EventEnvelope e) {
    switch (e.eventType()) {
      case "MisdeliveryConfirmed" -> onCase(e);
      case "CaseOpened", "CaseClosed", "CaseReopened", "CaseAssigned", "CaseTransferred" ->
          onHelpCase(e);
      default -> {}
    }
  }

  /**
   * DE-CASE(2026-11-16): case 가 도움요청·전달 실패 등록으로 연 사건(RIDER_HELP·DELIVERY_INCIDENT) — 열리면 도움요청에 사건
   * 번호·처리 중, 닫히면 처리됨. 도움요청은 refs 의 deliveryId·requestId(없으면 incidentId = 배송 ID:요청 ID)로 찾는다.
   */
  void onHelpCase(EventEnvelope e) {
    JsonNode p = e.payload();
    if (!HELP_CASE_TYPES.contains(p.path("type").asString(""))) {
      return;
    }
    String caseId = p.path("caseId").asString("");
    if (caseId.isBlank()) {
      return;
    }
    if ("CaseReopened".equals(e.eventType())) {
      log.info("사건 {} 다시 열림 → 도움요청 {}건 접수됨", caseId, store.reopenHelpByCase(caseId));
      return;
    }
    if ("CaseAssigned".equals(e.eventType())) {
      // DE-2: 상담원이 집을 때 처리 중
      log.info("사건 {} 담당 지정 → 도움요청 {}건 처리 중", caseId, store.assignHelpByCase(caseId));
      return;
    }
    if ("CaseTransferred".equals(e.eventType())) {
      String toGroup = p.path("toGroup").asString("");
      if (!toGroup.isBlank()) {
        log.info(
            "사건 {} 이관 {} → 도움요청 {}건", caseId, toGroup, store.transferHelpByCase(caseId, toGroup));
      }
      return;
    }
    if ("CaseClosed".equals(e.eventType())) {
      Instant at = instantOr(p.path("closedAt").asString(null), clock.instant());
      // 2026-11-20 사용자 "지원센터에서 처리완료하면 아예 배차 취소, 회수배차 바로": 결정 대기 전달 불가 대화의 사건이 닫히면 실패 확정
      String closedBy =
          e.actor() != null && e.actor().type() == EventEnvelope.ActorType.OPERATOR
              ? e.actor().id()
              : null;
      for (ExecutionStore.HelpRow h : store.openHelpsByCase(caseId)) {
        if ("DELIVERY_FAILURE".equals(h.category())) {
          failures.confirmOnCaseClosed(h.deliveryId(), h.requestId(), caseId, closedBy);
        }
      }
      int n = store.closeHelpByCase(caseId, p.path("outcome").asString(null), at);
      log.info("사건 {} 닫힘 → 도움요청 {}건 처리됨", caseId, n);
      return;
    }
    String deliveryId = null;
    String requestId = null;
    String incidentId = null;
    for (JsonNode r : p.path("refs")) {
      switch (r.path("refType").asString("")) {
        case "deliveryId" -> deliveryId = r.path("refId").asString(null);
        case "requestId" -> requestId = r.path("refId").asString(null);
        case "incidentId" -> incidentId = r.path("refId").asString(null);
        default -> {}
      }
    }
    if ((deliveryId == null || requestId == null) && incidentId != null) {
      int i = incidentId.indexOf(':');
      if (i > 0 && i < incidentId.length() - 1) {
        deliveryId = incidentId.substring(0, i);
        requestId = incidentId.substring(i + 1);
      }
    }
    if (deliveryId == null || requestId == null) {
      return; // 이 서비스 도움요청이 아닌 사건
    }
    int n = store.linkHelpCase(deliveryId, requestId, caseId);
    log.info("사건 {} 열림 → 배송 {} 도움요청 연결 {}건", caseId, deliveryId, n);
  }

  static final java.util.Set<String> HELP_CASE_TYPES =
      java.util.Set.of("RIDER_HELP", "DELIVERY_INCIDENT");

  private static Instant instantOr(@Nullable String s, Instant fallback) {
    try {
      return s == null ? fallback : Instant.parse(s);
    } catch (RuntimeException ex) {
      return fallback;
    }
  }

  public void onCase(EventEnvelope e) {
    JsonNode p = e.payload();
    if (!"RIDER".equals(p.path("fault").asString(""))) {
      return;
    }
    String deliveryId = p.path("deliveryId").asString("");
    DeliveryRow d = store.delivery(deliveryId).orElse(null);
    if (d == null || d.riderId() == null) {
      log.warn("라이더 귀책 오배송 {} — 배송 {} 의 라이더를 몰라 수행료를 바꾸지 않음", e.eventId(), deliveryId);
      return;
    }
    earnings.withholdForMisdelivery(
        deliveryId, d.riderId(), p.path("caseId").asString(""), clock.instant());
  }

  /**
   * dropAddressRef {@code order:<orderId>:redelivery:<destinationId>} 의 실제 집 주소 ID(ADR-0065). 아니면
   * null.
   */
  static @Nullable String redeliveryDestination(String dropAddressRef) {
    int i = dropAddressRef.indexOf(":redelivery:");
    return i < 0 ? null : dropAddressRef.substring(i + ":redelivery:".length());
  }

  public void onDelivery(EventEnvelope e) {
    JsonNode p = e.payload();
    Instant now = clock.instant();
    String deliveryId = p.path("deliveryId").asString();
    store.ensureDelivery(
        deliveryId,
        p.path("type").asString(null),
        p.path("orderId").asString(null),
        p.path("shipmentId").asString(null),
        p.path("mfcId").asString(null),
        now);
    DeliveryRow d = store.lockDelivery(deliveryId).orElseThrow();
    switch (e.eventType()) {
      case "DeliveryCreated" -> {
        // 유형·목적(오배송 회수 — DD-2). 배정 사건이 먼저 와 FORWARD 로 만들어졌어도 여기서 바로잡는다
        store.describe(
            deliveryId,
            p.path("type").asString(null),
            p.path("purpose").asString(null),
            p.path("caseId").asString(null),
            p.path("originDeliveryId").asString(null),
            redeliveryDestination(p.path("dropAddressRef").asString("")),
            p.path("claimId").asString(null), // 고객 반품 회수(CL-1, 2026-11-17)
            p.path("returnLines").isArray()
                ? mapper.writeValueAsString(p.path("returnLines"))
                : null);
        // 주문번호(2026-11-18): 배정 사건만 싣던 값이라 배차 전 배송은 주문번호로 못 찾았다 — 생길 때 order 에서 한 번 읽는다
        if (p.path("deliverySeq").isInt()) {
          store.setDeliverySeq(deliveryId, p.path("deliverySeq").asInt()); // 화면 배송 번호(ADR-0070)
        }
        String orderId = p.path("orderId").asString("");
        if (d.orderNumber() == null && !orderId.isBlank()) {
          orders.orderNumber(orderId).ifPresent(n -> store.fillOrderNumber(deliveryId, n));
        }
        List<String> pkgs = strings(p.path("packageIds"));
        if (d.shipmentId() != null
            && !pkgs.isEmpty()
            && store.shipmentPackages(d.shipmentId()).isEmpty()) {
          store.putShipmentPackages(d.shipmentId(), mapper.writeValueAsString(pkgs), now);
        }
      }
      case "DeliveryAssigned" -> {
        int v = p.path("assignmentVersion").asInt();
        if (v <= d.assignmentVersion() || !ExecutionRules.BEFORE_PICKUP.contains(d.state())) {
          log.info(
              "배송 {} 배정 사건 무시 — 배정 버전 {} (지금 {} {})",
              deliveryId,
              v,
              d.assignmentVersion(),
              d.state());
          return;
        }
        Instant at = instant(p.path("acceptedAt"), e.occurredAt());
        store.assign(deliveryId, p.path("riderId").asString(), v, at, now);
        // 수락 당시 금액·조건(원문 §7.1) — 수행료 기준(2026-11-13)
        store.accepted(
            deliveryId,
            p.path("fee").isNumber() ? p.path("fee").asInt() : null,
            p.path("vehicleType").asString(null),
            p.path("distanceM").isNumber() ? p.path("distanceM").asInt() : null,
            p.path("orderNumber").asString(null), // 내 배달 내역(2026-11-17)
            p.path("mfcName").asString(null),
            p.path("mfcPhone").asString(null)); // 지점 전화(2026-11-17)
        store.history(
            deliveryId, "ASSIGNED", at, "SYSTEM_EVENT", p.path("offerId").asString(null), now);
        // DE-6(2026-11-19) — 묶음 전달 순서
        store.bundle(
            deliveryId,
            p.path("bundleId").asString(null),
            p.path("stopSequence").isInt() ? p.path("stopSequence").asInt() : null);
        sameRiderReturn(d, p.path("riderId").asString(""), now);
      }
      case "DeliveryAssignmentChanged" -> {
        int v = p.path("assignmentVersion").asInt();
        if (v <= d.assignmentVersion()) {
          return;
        }
        if (!ExecutionRules.BEFORE_PICKUP.contains(d.state())) {
          log.warn("배송 {} 픽업 뒤 배정 변경 사건 — 수행 상태 {} 유지(지원센터 처리)", deliveryId, d.state());
          return;
        }
        Instant at = instant(p.path("changedAt"), e.occurredAt());
        String newRider = p.path("newRiderId").asString(null);
        if (d.riderId() != null && !byRider(e)) {
          earnings.notCalculable(d, "UNASSIGNED", now); // 운영자·주문 사유 배차 취소 — 기준 없음(본인 취소는 수행료 없음)
        }
        if (newRider == null || newRider.isBlank()) {
          store.unassign(deliveryId, v, now);
          store.history(
              deliveryId, "UNASSIGNED", at, "SYSTEM_EVENT", p.path("reason").asString(null), now);
        } else {
          store.assign(deliveryId, newRider, v, at, now);
          store.history(
              deliveryId, "ASSIGNED", at, "SYSTEM_EVENT", p.path("reason").asString(null), now);
        }
      }
      case "DeliveryCancelledBeforePickup" -> {
        if (!ExecutionRules.BEFORE_PICKUP.contains(d.state())) {
          log.warn("배송 {} 픽업 전 취소 사건 — 이미 {}, 그대로 둔다", deliveryId, d.state());
          return;
        }
        if (d.riderId() != null && !"UNASSIGNED".equals(d.state()) && !byRider(e)) {
          earnings.notCalculable(d, "CANCELLED_BEFORE_PICKUP", now);
        }
        store.setState(deliveryId, "CANCELLED", now);
        store.history(
            deliveryId,
            "CANCELLED",
            instant(p.path("cancelledAt"), e.occurredAt()),
            "SYSTEM_EVENT",
            p.path("reason").asString(null),
            now);
        return;
      }
      default -> {
        return;
      }
    }
    pickup.judge(deliveryId, now);
  }

  /**
   * 다른 라이더 반납이 원래 라이더에게 넘어감(2026-11-20 — dispatch 가 5분 못 구하면 원래 라이더에게 바로, 사용자 "가"): 원래 배송 실패 기록의
   * returnBy 를 SAME_RIDER 로 — 라이더 앱이 "다른 라이더에게 넘겨 주세요" 대신 반납 배송을 연다. 실패 기록에 바뀐 시각을 남긴다.
   */
  private void sameRiderReturn(DeliveryRow d, String riderId, Instant now) {
    if (!d.failedReturn() || d.originDeliveryId() == null || riderId.isBlank()) {
      return;
    }
    DeliveryRow origin = store.delivery(d.originDeliveryId()).orElse(null);
    if (origin == null || !riderId.equals(origin.riderId()) || origin.failureJson() == null) {
      return;
    }
    JsonNode f = mapper.readTree(origin.failureJson());
    if (!"OTHER_RIDER".equals(f.path("returnBy").asString(""))) {
      return;
    }
    Map<String, Object> m = new LinkedHashMap<>();
    f.properties().forEach(en -> m.put(en.getKey(), en.getValue()));
    m.put("returnBy", "SAME_RIDER");
    m.put("returnByChangedAt", now.toString());
    store.replaceFailureRecord(origin.deliveryId(), mapper.writeValueAsString(m), now);
    log.info("배송 {} 반납을 원래 라이더가 하게 됨(다른 라이더 못 구함) — 반납 배송 {}", origin.deliveryId(), d.deliveryId());
  }

  public void onFulfillment(EventEnvelope e) {
    JsonNode p = e.payload();
    Instant now = clock.instant();
    switch (e.eventType()) {
      case "ShipmentReady", "PackingFinished" -> {
        String shipmentId = p.path("shipmentId").asString();
        List<String> ids = new ArrayList<>();
        for (JsonNode n : p.path("packages")) {
          ids.add(n.path("packageId").asString());
        }
        if (ids.isEmpty()) {
          return;
        }
        store.putShipmentPackages(shipmentId, mapper.writeValueAsString(ids), now);
        for (String deliveryId : store.deliveriesOfShipment(shipmentId)) {
          pickup.judge(deliveryId, now);
        }
      }
      case "PackageInvalidated" -> {
        // 2026-11-20(주문 23631583): 재포장으로 옛 포장 무효 — 포장 목록에서 새 포장으로 바꿔 보고 픽업을 다시 판정한다.
        // 처리하지 않던 때는 옛 포장이 목록에 남아 새 포장을 인계해도 픽업 완료가 되지 않았다
        String packageId = p.path("packageId").asString("");
        if (packageId.isBlank()) {
          return;
        }
        List<String> next = strings(p.path("newPackageIds"));
        if (next.isEmpty()) {
          String one = p.path("newPackageId").asString("");
          if (!one.isBlank()) {
            next = List.of(one);
          }
        }
        String shipmentId = p.path("shipmentId").asString(null);
        Instant at = instantOr(p.path("invalidatedAt").asString(null), now);
        store.invalidatePackage(packageId, shipmentId, next, at, now);
        log.info("포장 {} 무효(재포장) → 새 포장 {}개", packageId, next.size());
        if (shipmentId != null && !shipmentId.isBlank()) {
          for (String deliveryId : store.deliveriesOfShipment(shipmentId)) {
            pickup.judge(deliveryId, now);
          }
        }
      }
      case "PackageHandedOver" -> {
        String deliveryId = p.path("deliveryId").asString(null);
        if (deliveryId == null || deliveryId.isBlank()) {
          return;
        }
        Map<String, Integer> lines = new LinkedHashMap<>();
        Map<String, String> skus =
            new LinkedHashMap<>(); // 줄 → SKU(2026-11-20 — ReturnDelivered 줄 sku)
        for (JsonNode l : p.path("lines")) {
          lines.merge(l.path("orderLineId").asString(), l.path("qty").asInt(), Integer::sum);
          String sku = l.path("sku").asString("");
          if (!sku.isBlank()) {
            skus.putIfAbsent(l.path("orderLineId").asString(), sku);
          }
        }
        boolean fresh =
            store.recordHandover(
                p.path("handoverId").asString(),
                deliveryId,
                p.path("packageId").asString(),
                p.path("riderId").asString(null),
                mapper.writeValueAsString(lines),
                skus.isEmpty() ? null : mapper.writeValueAsString(skus),
                instant(p.path("handedAt"), e.occurredAt()),
                now);
        if (fresh) {
          pickup.judge(deliveryId, now);
        }
      }
      default -> {}
    }
  }

  /** 라이더 본인이 낸 배차 취소인가(사건 행위자) — 본인 취소는 수행료를 만들지 않는다. */
  private static boolean byRider(EventEnvelope e) {
    return e.actor() != null && e.actor().type() == EventEnvelope.ActorType.RIDER;
  }

  private static List<String> strings(JsonNode arr) {
    List<String> out = new ArrayList<>();
    for (JsonNode n : arr) {
      out.add(n.asString());
    }
    return out;
  }

  private static Instant instant(JsonNode n, @Nullable Instant fallback) {
    if (n.isString()) {
      return Instant.parse(n.asString());
    }
    return fallback == null ? Instant.now() : fallback;
  }
}
