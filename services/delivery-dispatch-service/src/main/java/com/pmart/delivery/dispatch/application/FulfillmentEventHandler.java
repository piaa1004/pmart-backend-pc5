package com.pmart.delivery.dispatch.application;

import com.pmart.common.messaging.EventEnvelope;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore.DeliveryRow;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * MFC 사건(mfc.fulfillment.v1) → 배송. inbox 트랜잭션 안에서 부른다(외부 호출 없음).
 *
 * <ul>
 *   <li>출고당 배송 하나. **배차는 점원 요청({@code DispatchRequested})으로만 시작한다**(사용자 2026-11-13 "자동요청빼", BR-51 —
 *       mfc-fulfillment 0.4.11 은 READY 자동 요청을 내지 않는다). 요청이 오면 DISPATCHING +
 *       DeliveryCreated(shipmentId — MFC 배정 투영) + 배차 시도(SEARCHING, 바로 판단).
 *   <li>{@code ShipmentReady} 가 요청보다 먼저 오면 배송을 CREATED 로만 둔다(출하 대기·포장 목록 기록, 사건·배차 없음). 점원이 READY
 *       뒤에 요청하면 그때 배차.
 *   <li>{@code PackingFinished} — 포장 목록(결품으로 줄어든 내용) 갱신.
 *   <li>{@code WorkStopResult} — 그 배송이 아는 포장이 모두 STOPPED 면 픽업 전 취소(cancel-vs-handover — 하나라도 인계·인계
 *       중이면 그대로). 포장을 아직 모르면 판정하지 않는다(경고).
 *   <li>{@code PackageHandedOver} — 인계된 포장 기록, 배송의 모든 포장이 인계되면 PICKED_UP(D-6).
 * </ul>
 */
@Component
public class FulfillmentEventHandler {

  public static final Set<String> TYPES =
      Set.of(
          "DispatchRequested",
          "ShipmentReady",
          "PackingFinished",
          "WorkStopResult",
          "PackageHandedOver",
          "PackageInvalidated");

  private static final Logger log = LoggerFactory.getLogger(FulfillmentEventHandler.class);
  private static final Set<String> CLOSED =
      Set.of("PICKED_UP", "CANCELLED_BEFORE_PICKUP", "CLOSED", "BLOCKED");

  private final DispatchStore store;
  private final DispatchEvents events;
  private final DeliveryTransitions transitions;
  private final ObjectMapper mapper;
  private final Clock clock;

  public FulfillmentEventHandler(
      DispatchStore store,
      DispatchEvents events,
      DeliveryTransitions transitions,
      ObjectMapper mapper,
      Clock clock) {
    this.store = store;
    this.events = events;
    this.transitions = transitions;
    this.mapper = mapper;
    this.clock = clock;
  }

  public void on(EventEnvelope e) {
    JsonNode p = e.payload();
    Instant now = clock.instant();
    // 오배송 재발송 출고(2026-11-16 — mfc-fulfillment 재발송 작업 지시의 출고)
    String reshipmentCase =
        "MISDELIVERY_RESHIPMENT".equals(p.path("purpose").asString(""))
            ? p.path("caseId").asString(null)
            : null;
    switch (e.eventType()) {
      case "DispatchRequested" ->
          ensure(
              p.path("shipmentId").asString(),
              p.path("orderId").asString(),
              p.path("mfcId").asString(),
              p.path("trigger").asString(null),
              instant(p.path("estimatedReadyAt")),
              null,
              null,
              null,
              true,
              reshipmentCase,
              now);
      case "ShipmentReady" ->
          ensure(
              p.path("shipmentId").asString(),
              p.path("orderId").asString(),
              p.path("mfcId").asString(),
              null,
              null,
              instant(p.path("readyAt")),
              packages(p.path("packages"), p.path("shipmentId").asString()),
              p.path("deliveryMode").asString(null),
              false,
              reshipmentCase,
              now);
      case "PackingFinished" -> {
        String shipmentId = p.path("shipmentId").asString();
        store
            .lockByShipment(shipmentId)
            .ifPresent(
                d ->
                    store.updatePackages(
                        d.deliveryId(), packages(p.path("packages"), shipmentId), now));
      }
      case "WorkStopResult" -> onStop(p, now);
      case "PackageHandedOver" -> onHandover(p, now);
      case "PackageInvalidated" -> onInvalidated(p, now);
      default -> {}
    }
  }

  private void ensure(
      String shipmentId,
      String orderId,
      String mfcId,
      @Nullable String trigger,
      @Nullable Instant estimatedReadyAt,
      @Nullable Instant readyAt,
      @Nullable String packagesJson,
      @Nullable String deliveryMode,
      boolean requested,
      @Nullable String reshipmentCase,
      Instant now) {
    store.lockOrderKey(orderId); // 작업 중지·주문 취소와 같은 잠금(주문 → 출고 순서)
    store.lockShipmentKey(shipmentId);
    var existing = store.lockByShipment(shipmentId);
    if (existing.isPresent()) {
      DeliveryRow d = existing.get();
      if (reshipmentCase != null) {
        store.markReshipment(d.deliveryId(), reshipmentCase);
      }
      if (trigger != null || estimatedReadyAt != null) {
        store.updateRequest(d.deliveryId(), trigger, estimatedReadyAt, now);
      }
      if (readyAt != null || packagesJson != null) {
        store.updateReady(
            d.deliveryId(),
            readyAt,
            packagesJson == null ? d.packagesJson() : packagesJson,
            deliveryMode,
            now);
        recheckPickup(d.deliveryId(), now); // 포장 목록을 이제 알면 인계 기록으로 다시 판정(M4)
      }
      if (requested && "CREATED".equals(d.state())) {
        if (store.orderStopped(orderId)) {
          store.setState(
              d.deliveryId(), "BLOCKED", "ORDER_CANCELLED", now); // 중지된 주문 — 배차하지 않는다(H1)
          log.info("배송 {} 배차 요청 무시 — 주문 {} 작업 중지됨", d.deliveryId(), orderId);
        } else {
          store.setState(d.deliveryId(), "DISPATCHING", null, now);
          startDispatch(d.deliveryId(), now);
        }
      }
      return;
    }
    boolean stopped = store.orderStopped(orderId);
    String deliveryId = "DLV-" + UUID.randomUUID();
    String pkgs = packagesJson == null ? "[]" : packagesJson;
    store.insertDelivery(
        deliveryId,
        orderId,
        shipmentId,
        mfcId,
        trigger,
        estimatedReadyAt,
        readyAt,
        pkgs,
        deliveryMode,
        stopped ? "BLOCKED" : requested ? "DISPATCHING" : "CREATED",
        now);
    if (reshipmentCase != null) {
      store.markReshipment(deliveryId, reshipmentCase); // DeliveryCreated 전에 — 사건에 RESHIPMENT 로 싣는다
    }
    if (stopped) {
      log.info("배송 {} 기록만 — 주문 {} 작업 중지됨(배차 안 함)", deliveryId, orderId);
    } else if (requested) {
      startDispatch(deliveryId, now);
    } else {
      log.info("배송 {} 기록 — 출고 {} 출하 대기, 점원 배차 요청 대기", deliveryId, shipmentId);
    }
  }

  /** 점원 요청 → 배차 시작: 시도 + DeliveryCreated. */
  private void startDispatch(String deliveryId, Instant now) {
    store.insertAttempt("ATT-" + UUID.randomUUID(), deliveryId, "AUTO", now);
    DeliveryRow d = store.delivery(deliveryId).orElseThrow();
    events.created(d, packageIds(d.packagesJson()));
    log.info(
        "배송 {} 배차 시작 — 출고 {} 주문 {} MFC {}", deliveryId, d.shipmentId(), d.orderId(), d.mfcId());
  }

  private void onStop(JsonNode p, Instant now) {
    Map<String, String> verdicts = new LinkedHashMap<>();
    for (JsonNode pp : p.path("perPackage")) {
      verdicts.put(pp.path("packageId").asString(), pp.path("state").asString(""));
    }
    String orderId = p.path("orderId").asString();
    store.lockOrderKey(orderId);
    if (verdicts.isEmpty()) {
      // 피킹 전 중지는 전체·부분 모두 perPackage=[] 로 온다(mfc WorkStopService) — 여기서는 주문 전체인지 모른다.
      // 주문 전체 취소는 order 의 OrderCancelled(scope FULL)로 판단한다(onOrderCancelled, PC 1 재검토 ①, 사용자 (가)).
      log.info("주문 {} 피킹 전 작업 중지 — 배송 판단은 주문 취소 사건(FULL)으로", orderId);
      return;
    }
    if (verdicts.values().stream().allMatch("STOPPED"::equals)) {
      // 배차 요청이 이 판정보다 늦게 와도 배차하지 않게 남겨 둔다(키가 달라 순서 보장 없음 — H1)
      store.recordStoppedOrder(orderId, p.path("stopRequestId").asString(""), now);
    }
    for (DeliveryRow d : store.lockByOrder(orderId)) {
      if (CLOSED.contains(d.state())) {
        continue;
      }
      List<String> known = packageIds(d.packagesJson());
      if (known.isEmpty()) {
        continue; // 이 배송의 포장을 모른다 — 주문 취소 사건으로 판단
      }
      if (known.stream().allMatch(id -> "STOPPED".equals(verdicts.get(id)))) {
        cancel(d, "모든 포장 STOPPED", now);
      } else {
        log.info("배송 {} 작업 중지 판정에 인계·인계 중 포장 있음 — 배차 유지", d.deliveryId());
      }
    }
  }

  /**
   * order 주문 취소(order.order.v1) — scope FULL 이면 주문 전체가 취소됐다: 픽업 전 배송은 취소하고, 나중에 온 배차 요청은 BLOCKED(멈춘
   * 주문으로 기록). PARTIAL 은 남은 상품이 있어 배차를 그대로 둔다. 픽업한 배송은 그대로(회수는 claim·지원센터).
   */
  public void onOrderCancelled(EventEnvelope e) {
    JsonNode p = e.payload();
    if (!"FULL".equals(p.path("scope").asString(""))) {
      return;
    }
    Instant now = clock.instant();
    String orderId = p.path("orderId").asString();
    store.lockOrderKey(orderId);
    store.recordStoppedOrder(orderId, "order-cancelled:" + e.eventId(), now);
    for (DeliveryRow d : store.lockByOrder(orderId)) {
      if (CLOSED.contains(d.state())) {
        continue;
      }
      if (d.handoverStarted()) {
        // 일부 포장이 이미 라이더 손에 — 픽업한 배송처럼 그대로(회수는 claim·지원센터)
        log.warn("배송 {} 주문 전체 취소 — 포장 일부가 이미 인계돼 배차 유지", d.deliveryId());
        continue;
      }
      cancel(d, "주문 전체 취소", now);
    }
  }

  private void cancel(DeliveryRow d, String why, Instant now) {
    transitions.cancelBeforePickup(d, "ORDER_CANCELLED", now); // CREATED(알린 적 없음)면 사건 없이
    log.info("배송 {} 픽업 전 취소 — {}", d.deliveryId(), why);
  }

  private void onHandover(JsonNode p, Instant now) {
    String deliveryId = p.path("deliveryId").asString(null);
    if (deliveryId == null) {
      return;
    }
    store
        .lockDelivery(deliveryId)
        .ifPresent(
            d -> {
              Set<String> handed = new LinkedHashSet<>(stringList(d.handedOverJson()));
              handed.add(p.path("packageId").asString());
              List<String> known = packageIds(d.packagesJson());
              // 포장 목록을 모르면 픽업 완료로 바꾸지 않는다 — 목록(ShipmentReady)이 오면 다시 판정(D-6 모든 포장 인계 뒤, M4)
              boolean all = !known.isEmpty() && handed.containsAll(known);
              boolean pickedUp = all && "ASSIGNED".equals(d.state());
              store.recordHandover(
                  deliveryId, mapper.writeValueAsString(new ArrayList<>(handed)), pickedUp, now);
            });
  }

  private void recheckPickup(String deliveryId, Instant now) {
    store
        .lockDelivery(deliveryId)
        .ifPresent(
            d -> {
              List<String> known = packageIds(d.packagesJson());
              List<String> handed = stringList(d.handedOverJson());
              if ("ASSIGNED".equals(d.state())
                  && !known.isEmpty()
                  && !handed.isEmpty()
                  && handed.containsAll(known)) {
                store.recordHandover(deliveryId, d.handedOverJson(), true, now);
              }
            });
  }

  /**
   * 재포장 무효(2026-11-20 — MFC 시뮬레이터 R-28, 주문 23631583): 옛 포장을 새 포장으로 바꾸고 픽업을 다시 판정한다. 처리하지 않던 때는 옛
   * 포장이 목록에 남아 새 포장을 인계해도 PICKED_UP 이 되지 않았다(배차 취소도 422). 무효가 목록보다 먼저 오면 기록만 — 목록이 올 때 바꾼다.
   */
  private void onInvalidated(JsonNode p, Instant now) {
    String packageId = p.path("packageId").asString("");
    if (packageId.isBlank()) {
      return;
    }
    List<String> next = new ArrayList<>();
    p.path("newPackageIds").forEach(n -> next.add(n.asString()));
    if (next.isEmpty() && !p.path("newPackageId").asString("").isBlank()) {
      next.add(p.path("newPackageId").asString());
    }
    String shipmentId = p.path("shipmentId").asString(null);
    store.invalidatePackage(packageId, shipmentId, mapper.writeValueAsString(next), now);
    if (shipmentId == null || shipmentId.isBlank()) {
      return;
    }
    store
        .lockByShipment(shipmentId)
        .filter(d -> d.packagesJson() != null)
        .ifPresent(
            d -> {
              String replaced = applyInvalidations(d.packagesJson(), shipmentId);
              store.updatePackages(d.deliveryId(), replaced, now);
              recheckPickup(d.deliveryId(), now);
              log.info("배송 {} 포장 {} 무효(재포장) → 새 포장 {}개", d.deliveryId(), packageId, next.size());
            });
  }

  /** 포장 목록에서 무효 포장을 새 포장으로(연쇄 재포장도 끝까지, 고리 막음). 새 포장은 옛 포장의 봉지 수·무게·주류 표시를 물려받는다. */
  String applyInvalidations(String packagesJson, @Nullable String shipmentId) {
    List<Map<String, Object>> items = new ArrayList<>();
    for (JsonNode n : mapper.readTree(packagesJson)) {
      Map<String, Object> m = new LinkedHashMap<>();
      n.properties().forEach(en -> m.put(en.getKey(), en.getValue()));
      m.put("packageId", n.path("packageId").asString());
      items.add(m);
    }
    Map<String, String> inv =
        store.invalidations(
            shipmentId, items.stream().map(m -> String.valueOf(m.get("packageId"))).toList());
    if (inv.isEmpty()) {
      return packagesJson;
    }
    List<Map<String, Object>> out = new ArrayList<>();
    Set<String> seen = new java.util.HashSet<>();
    java.util.ArrayDeque<Map<String, Object>> todo = new java.util.ArrayDeque<>(items);
    while (!todo.isEmpty()) {
      Map<String, Object> m = todo.pollFirst();
      String id = String.valueOf(m.get("packageId"));
      if (!seen.add(id)) {
        continue;
      }
      String next = inv.get(id);
      if (next == null) {
        out.add(m);
        continue;
      }
      List<Map<String, Object>> repl = new ArrayList<>();
      for (JsonNode n : mapper.readTree(next)) {
        Map<String, Object> copy = new LinkedHashMap<>(m);
        copy.put("packageId", n.asString());
        repl.add(copy);
      }
      for (int i = repl.size() - 1; i >= 0; i--) {
        todo.addFirst(repl.get(i));
      }
    }
    return mapper.writeValueAsString(out);
  }

  /** 포장 목록 — 라이더 표시·과적 판단에 필요한 칸만(내용물 줄은 저장하지 않는다). 이미 무효된 포장은 새 포장으로. */
  private String packages(JsonNode arr, @Nullable String shipmentId) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (JsonNode n : arr) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("packageId", n.path("packageId").asString());
      m.put("bagCount", n.path("bagCount").asInt(0));
      if (n.path("weightG").isNumber()) {
        m.put("weightG", n.path("weightG").asInt());
      }
      m.put("alcohol", n.path("alcohol").asBoolean(false));
      out.add(m);
    }
    return applyInvalidations(mapper.writeValueAsString(out), shipmentId);
  }

  List<String> packageIds(String packagesJson) {
    List<String> ids = new ArrayList<>();
    for (JsonNode n : mapper.readTree(packagesJson)) {
      ids.add(n.path("packageId").asString());
    }
    return ids;
  }

  private List<String> stringList(String json) {
    List<String> out = new ArrayList<>();
    for (JsonNode n : mapper.readTree(json)) {
      out.add(n.asString());
    }
    return out;
  }

  private static @Nullable Instant instant(JsonNode n) {
    return n.isString() ? Instant.parse(n.asString()) : null;
  }
}
