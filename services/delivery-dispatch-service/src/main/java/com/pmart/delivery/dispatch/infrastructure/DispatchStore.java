package com.pmart.delivery.dispatch.infrastructure;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** 배송·배차 시도·제안·연속 거절 저장(JDBC, V2__deliveries_dispatch.sql). */
@Repository
public class DispatchStore {

  public record DeliveryRow(
      String deliveryId,
      int version,
      String type,
      String orderId,
      String shipmentId,
      String mfcId,
      String state,
      @Nullable String deliveryMode,
      @Nullable LocalDate arrivalDate,
      @Nullable String arrivalStart,
      @Nullable String arrivalEnd,
      @Nullable String requestTrigger,
      @Nullable Instant estimatedReadyAt,
      @Nullable Instant readyAt,
      String packagesJson,
      @Nullable Double mfcLat,
      @Nullable Double mfcLng,
      @Nullable Double dropLat,
      @Nullable Double dropLng,
      @Nullable String dropAreaLabel,
      @Nullable Integer dropDistanceM,
      @Nullable Integer dropDurationS,
      @Nullable String geoError,
      @Nullable String riderId,
      @Nullable String assignmentId,
      int assignmentVersion,
      @Nullable Instant assignedAt,
      @Nullable String acceptedOfferId,
      @Nullable Instant pickedUpAt,
      String handedOverJson,
      @Nullable Instant cancelledAt,
      @Nullable String cancelReason,
      Instant createdAt,
      Instant updatedAt,
      @Nullable String bundleId,
      @Nullable Integer stopSeq,
      @Nullable Instant delayAlertedAt,
      @Nullable Instant promisedAt,
      @Nullable String purpose,
      @Nullable String caseId,
      @Nullable String originDeliveryId,
      @Nullable Double pickupLat,
      @Nullable Double pickupLng,
      @Nullable String pickupSource,
      @Nullable Instant pickupRecordedAt,
      @Nullable Instant completedAt,
      @Nullable String redeliveryDestinationId,
      @Nullable Integer straightDistanceM,
      boolean paymentRequired,
      @Nullable Instant releasedAt,
      @Nullable String claimId,
      @Nullable String returnLinesJson,
      @Nullable String pickupRequestId,
      @Nullable Integer deliverySeq) {

    /** 고객 한집배달(SINGLE, 회수·반납 아님) — 2026-11-18 "한집은 단독". */
    public boolean customerSingle() {
      return "SINGLE".equals(deliveryMode()) && !"RETURN".equals(type());
    }

    /** 오배송 고객 잘못 다시 배송(ADR-0065) — 잘못 둔 곳에서 실제 집으로. */
    public boolean redelivery() {
      return "MISDELIVERY_REDELIVERY".equals(purpose);
    }

    /** 픽업지가 MFC 가 아닌 배송(회수·다시 배송) — 따로 판단한다. */
    public boolean pickupBased() {
      return recovery() || redelivery() || claimReturn() || failedReturn();
    }

    /** 전달 실패 반납(2026-11-17) — 확정된 전달 실패 뒤 상품을 든 원래 라이더가 MFC 로. */
    public boolean failedReturn() {
      return "RETURN".equals(type) && "FAILED_DELIVERY_RETURN".equals(purpose);
    }

    /** 전달 실패 반납을 다른 라이더가(2026-11-17 — 원래 라이더가 더 갈 수 없을 때). request_trigger FAILURE_OTHER. */
    public boolean failedReturnByOther() {
      return failedReturn() && "FAILURE_OTHER".equals(requestTrigger);
    }

    /** 고객 반품 회수(CL-1, 2026-11-17) — 고객 집에서 받아 MFC 로. 픽업지가 MFC 가 아니라 따로 배차한다. */
    public boolean claimReturn() {
      return "RETURN".equals(type) && "CLAIM_RETURN".equals(purpose);
    }

    /** 9,000원 결제를 기다리는 다시 배송(200m 이상, 결제 확인 전). */
    public boolean awaitingPayment() {
      return paymentRequired && releasedAt == null;
    }

    /** 확인된 오배송 회수 배송(DD-2) — 회수지에서 받아 MFC 로. 픽업지가 MFC 가 아니라 따로 배차한다. */
    public boolean recovery() {
      return "RETURN".equals(type) && "MISDELIVERY_RECOVERY".equals(purpose);
    }

    public boolean geoReady() {
      return mfcLat != null
          && dropLat != null
          && dropDistanceM != null
          && (!pickupBased() || pickupLat != null);
    }

    /**
     * 포장을 하나라도 넘겨받았다. 모든 포장이 인계돼야 PICKED_UP 이라(D-6) 그 전에는 ASSIGNED 로 남지만, 이때부터는 픽업한 배송처럼 다룬다 — 배정을
     * 풀거나 픽업 전 취소하면 넘겨받은 묶음이 그 라이더 손에 남는다(PC 1 PR #13 검토, 사용자 2026-11-14).
     */
    public boolean handoverStarted() {
      String s = handedOverJson == null ? "" : handedOverJson.replaceAll("\\s", "");
      return !s.isEmpty() && !"[]".equals(s) && !"null".equals(s);
    }

    /** 출하 대기(실제 → 예상 → 없음). */
    public @Nullable Instant readyOrEstimate() {
      return readyAt != null ? readyAt : estimatedReadyAt;
    }
  }

  public record AttemptRow(
      String attemptId,
      String deliveryId,
      String trigger,
      String state,
      int stage,
      Instant stageStartedAt,
      int retryCount,
      int round,
      Instant nextRunAt,
      Instant startedAt,
      @Nullable Instant finishedAt,
      @Nullable String failureReason,
      boolean hadCandidate,
      boolean hadRejection,
      boolean hadExpiry,
      @Nullable String excludedRiderId) {}

  public record OfferRow(
      String offerId,
      String attemptId,
      String deliveryId,
      String riderId,
      int round,
      String source,
      double radiusKm,
      String state,
      Instant sentAt,
      Instant expiresAt,
      @Nullable Instant viewedAt,
      @Nullable Instant decidedAt,
      @Nullable String rejectReason,
      @Nullable String lateReason,
      @Nullable Integer pickupDistanceM,
      @Nullable Integer pickupDurationS,
      int fee,
      @Nullable String vehicleType,
      @Nullable String bundleId,
      int bundleSize,
      int stopSeq) {}

  private final JdbcTemplate jdbc;

  public DispatchStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  // ---------- 배송 ----------

  public Optional<DeliveryRow> delivery(String deliveryId) {
    return first(
        "SELECT * FROM delivery WHERE delivery_id = ?", DispatchStore::delivery, deliveryId);
  }

  public Optional<DeliveryRow> lockDelivery(String deliveryId) {
    return first(
        "SELECT * FROM delivery WHERE delivery_id = ? FOR UPDATE",
        DispatchStore::delivery,
        deliveryId);
  }

  /** 재포장으로 무효된 포장(PackageInvalidated, 2026-11-20). 같은 포장은 한 번. */
  public void invalidatePackage(
      String packageId, @Nullable String shipmentId, String newPackageIdsJson, Instant now) {
    jdbc.update(
        "INSERT INTO invalidated_package (package_id, shipment_id, new_package_ids, recorded_at)"
            + " VALUES (?, ?, ?::jsonb, ?) ON CONFLICT (package_id) DO NOTHING",
        packageId,
        shipmentId,
        newPackageIdsJson,
        ts(now));
  }

  /** 무효 포장 → 새 포장 ID 목록(JSON). 그 출고 것과, 주어진 포장 ID 것. */
  public java.util.Map<String, String> invalidations(
      @Nullable String shipmentId, java.util.Collection<String> packageIds) {
    java.util.Map<String, String> out = new java.util.HashMap<>();
    jdbc.query(
        "SELECT package_id, new_package_ids::text FROM invalidated_package WHERE shipment_id = ?"
            + " OR package_id = ANY (?)",
        rs -> {
          out.put(rs.getString(1), rs.getString(2));
        },
        shipmentId,
        packageIds.toArray(String[]::new));
    return out;
  }

  public Optional<DeliveryRow> lockByShipment(String shipmentId) {
    return first(
        "SELECT * FROM delivery WHERE shipment_id = ? FOR UPDATE",
        DispatchStore::delivery,
        shipmentId);
  }

  public List<DeliveryRow> lockByOrder(String orderId) {
    return jdbc.query(
        "SELECT * FROM delivery WHERE order_id = ? ORDER BY created_at FOR UPDATE",
        (rs, i) -> delivery(rs),
        orderId);
  }

  /**
   * 같은 주문의 배차 요청(배송 만들기)과 작업 중지·주문 취소를 줄 세운다(PC 1 검토 후속, 2026-11-14). 없으면 배차 요청이 "멈춘 주문 아님"을 본 뒤 아직
   * 커밋 안 된 배송을 중지 처리가 못 보고 지나가, 둘 다 커밋된 뒤 멈춘 주문이 배차된다. 잡는 순서는 늘 주문 → 출고.
   */
  public void lockOrderKey(String orderId) {
    jdbc.queryForList("SELECT pg_advisory_xact_lock(hashtext(?))", "dispatch-order:" + orderId);
  }

  /** 출고 하나에 배송 하나 — 같은 출고는 줄을 세운 뒤 다시 본다. */
  public void lockShipmentKey(String shipmentId) {
    jdbc.queryForList(
        "SELECT pg_advisory_xact_lock(hashtext(?))", "dispatch-shipment:" + shipmentId);
  }

  public void insertDelivery(
      String deliveryId,
      String orderId,
      String shipmentId,
      String mfcId,
      @Nullable String trigger,
      @Nullable Instant estimatedReadyAt,
      @Nullable Instant readyAt,
      String packagesJson,
      @Nullable String deliveryMode,
      String state,
      Instant now) {
    jdbc.update(
        "INSERT INTO delivery (delivery_id, version, type, order_id, shipment_id, mfc_id, state,"
            + " request_trigger, estimated_ready_at, ready_at, packages, delivery_mode, created_at,"
            + " updated_at) VALUES (?, 1, 'FORWARD', ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)",
        deliveryId,
        orderId,
        shipmentId,
        mfcId,
        state,
        trigger,
        ts(estimatedReadyAt),
        ts(readyAt),
        packagesJson,
        deliveryMode,
        ts(now),
        ts(now));
  }

  public void updateRequest(
      String deliveryId,
      @Nullable String trigger,
      @Nullable Instant estimatedReadyAt,
      Instant now) {
    jdbc.update(
        "UPDATE delivery SET request_trigger = COALESCE(request_trigger, ?),"
            + " estimated_ready_at = COALESCE(estimated_ready_at, ?), updated_at = ? WHERE delivery_id = ?",
        trigger,
        ts(estimatedReadyAt),
        ts(now),
        deliveryId);
  }

  public void updateReady(
      String deliveryId,
      @Nullable Instant readyAt,
      String packagesJson,
      @Nullable String deliveryMode,
      Instant now) {
    jdbc.update(
        "UPDATE delivery SET ready_at = COALESCE(?, ready_at), packages = ?::jsonb,"
            + " delivery_mode = COALESCE(?, delivery_mode), updated_at = ? WHERE delivery_id = ?",
        ts(readyAt),
        packagesJson,
        deliveryMode,
        ts(now),
        deliveryId);
  }

  public void updatePackages(String deliveryId, String packagesJson, Instant now) {
    jdbc.update(
        "UPDATE delivery SET packages = ?::jsonb, updated_at = ? WHERE delivery_id = ?",
        packagesJson,
        ts(now),
        deliveryId);
  }

  /**
   * 오배송 회수 배송(DD-2) — 바로 배차 대기(DISPATCHING). 같은 사건(shipment 'RCV-<caseId>')이면 넣지 않고 false. 출하 대기
   * 없음(실물이 이미 밖에 있다).
   */
  public boolean insertRecovery(
      String deliveryId,
      String orderId,
      String mfcId,
      String caseId,
      String originDeliveryId,
      Instant now) {
    return jdbc.update(
            "INSERT INTO delivery (delivery_id, version, type, purpose, order_id, shipment_id, mfc_id,"
                + " state, case_id, origin_delivery_id, request_trigger, created_at, updated_at) VALUES"
                + " (?, 1, 'RETURN', 'MISDELIVERY_RECOVERY', ?, ?, ?, 'DISPATCHING', ?, ?, 'MISDELIVERY',"
                + " ?, ?) ON CONFLICT DO NOTHING",
            deliveryId,
            orderId,
            "RCV-" + caseId,
            mfcId,
            caseId,
            originDeliveryId,
            ts(now),
            ts(now))
        == 1;
  }

  /**
   * 고객 반품 회수 배송(CL-1, 2026-11-17) — 바로 배차 대기. 같은 회수 요청 사건(shipment 'CLR-<eventId>')이면 넣지 않고 false.
   * 호출자가 클레임 잠금({@link #lockClaim})을 먼저 잡는다.
   */
  public boolean insertClaimReturn(
      String deliveryId,
      String orderId,
      String mfcId,
      String claimId,
      @Nullable String originDeliveryId,
      String linesJson,
      String requestKey,
      @Nullable String pickupRequestId,
      Instant now) {
    return jdbc.update(
            "INSERT INTO delivery (delivery_id, version, type, purpose, order_id, shipment_id, mfc_id,"
                + " state, claim_id, origin_delivery_id, return_lines, pickup_request_id,"
                + " request_trigger, created_at, updated_at) VALUES (?, 1, 'RETURN', 'CLAIM_RETURN', ?,"
                + " ?, ?, 'DISPATCHING', ?, ?, ?::jsonb, ?, 'CLAIM', ?, ?) ON CONFLICT DO NOTHING",
            deliveryId,
            orderId,
            "CLR-" + requestKey,
            mfcId,
            claimId,
            originDeliveryId,
            linesJson,
            pickupRequestId,
            ts(now),
            ts(now))
        == 1;
  }

  /** 전달 실패 반납 배송(2026-11-17). 같은 원래 배송은 하나(배송 ID 를 원래 배송으로 정한다). */
  public boolean insertFailedReturn(
      String deliveryId,
      String orderId,
      String mfcId,
      String originDeliveryId,
      boolean otherRider,
      Instant now) {
    return jdbc.update(
            "INSERT INTO delivery (delivery_id, version, type, purpose, order_id, shipment_id, mfc_id,"
                + " state, origin_delivery_id, request_trigger, created_at, updated_at) VALUES (?, 1,"
                + " 'RETURN', 'FAILED_DELIVERY_RETURN', ?, ?, ?, 'DISPATCHING', ?, ?, ?, ?) ON"
                + " CONFLICT DO NOTHING",
            deliveryId,
            orderId,
            "FLR-" + originDeliveryId,
            mfcId,
            originDeliveryId,
            otherRider ? "FAILURE_OTHER" : "FAILURE",
            ts(now),
            ts(now))
        == 1;
  }

  /** 같은 클레임의 회수 요청을 줄 세운다(트랜잭션 끝까지). */
  public void lockClaim(String claimId) {
    jdbc.query("SELECT pg_advisory_xact_lock(hashtext('dispatch-claim:' || ?))", rs -> {}, claimId);
  }

  /** 같은 클레임의 아직 끝나지 않은 반품 회수 배송(배차 대기·배정·회수함). */
  public Optional<String> openClaimReturn(String claimId) {
    return jdbc
        .query(
            "SELECT delivery_id FROM delivery WHERE claim_id = ? AND purpose = 'CLAIM_RETURN' AND state"
                + " IN ('CREATED', 'DISPATCHING', 'ASSIGNED', 'PICKED_UP') ORDER BY created_at DESC LIMIT 1",
            (rs, i) -> rs.getString(1),
            claimId)
        .stream()
        .findFirst();
  }

  /** 오배송 다시 배송(ADR-0065) — 바로 배차 대기. 같은 사건(shipment 'RDL-<caseId>')이면 false. */
  public boolean insertRedelivery(
      String deliveryId,
      String orderId,
      String mfcId,
      String caseId,
      String originDeliveryId,
      String destinationId,
      Instant now) {
    return jdbc.update(
            "INSERT INTO delivery (delivery_id, version, type, purpose, order_id, shipment_id, mfc_id,"
                + " state, case_id, origin_delivery_id, redelivery_destination_id, request_trigger,"
                + " created_at, updated_at) VALUES (?, 1, 'RESHIPMENT', 'MISDELIVERY_REDELIVERY', ?, ?,"
                + " ?, 'DISPATCHING', ?, ?, ?, 'MISDELIVERY', ?, ?) ON CONFLICT DO NOTHING",
            deliveryId,
            orderId,
            "RDL-" + caseId,
            mfcId,
            caseId,
            originDeliveryId,
            destinationId,
            ts(now),
            ts(now))
        == 1;
  }

  /** 다시 배송 좌표: 픽업 = 잘못 둔 곳, 도착 = 실제 집, 거리 = 픽업 → 집 도로(배달료), 직선(200m 판정)·결제 필요. */
  public void saveRedeliveryGeo(
      String deliveryId,
      double mfcLat,
      double mfcLng,
      double pickupLat,
      double pickupLng,
      String pickupSource,
      @Nullable Instant pickupRecordedAt,
      double dropLat,
      double dropLng,
      @Nullable String areaLabel,
      int distanceM,
      int durationS,
      int straightM,
      boolean paymentRequired,
      Instant now) {
    jdbc.update(
        "UPDATE delivery SET mfc_lat = ?, mfc_lng = ?, drop_lat = ?, drop_lng = ?, pickup_lat = ?,"
            + " pickup_lng = ?, pickup_source = ?, pickup_recorded_at = ?, drop_area_label = ?,"
            + " drop_distance_m = ?, drop_duration_s = ?, straight_distance_m = ?, payment_required"
            + " = ?, delivery_mode = 'SINGLE', geo_error = NULL, updated_at = ? WHERE delivery_id = ?",
        mfcLat,
        mfcLng,
        dropLat,
        dropLng,
        pickupLat,
        pickupLng,
        pickupSource,
        ts(pickupRecordedAt),
        areaLabel,
        distanceM,
        durationS,
        straightM,
        paymentRequired,
        ts(now),
        deliveryId);
  }

  /** 다시 배송 결제됨(ADR-0065 4단계) — 이미 풀렸으면 0. */
  public int markReleased(String deliveryId, Instant now) {
    return jdbc.update(
        "UPDATE delivery SET released_at = ?, updated_at = ? WHERE delivery_id = ? AND released_at"
            + " IS NULL",
        ts(now),
        ts(now),
        deliveryId);
  }

  /** 라이더용 주문 요약 사본(라이더 앱 요청 §1). */
  public void saveOrderSummary(String deliveryId, String json) {
    jdbc.update(
        "UPDATE delivery SET order_summary = ?::jsonb WHERE delivery_id = ?", json, deliveryId);
  }

  /** 주문 요약 사본(없으면 null). */
  public @Nullable String orderSummary(String deliveryId) {
    List<String> r =
        jdbc.queryForList(
            "SELECT order_summary::text FROM delivery WHERE delivery_id = ? AND order_summary IS NOT"
                + " NULL",
            String.class,
            deliveryId);
    return r.isEmpty() ? null : r.get(0);
  }

  /** 회수 배송 좌표: 픽업 = 회수지, 도착 = MFC, 거리·시간 = 회수지 → MFC(예상 배달료 기준). */
  public void saveRecoveryGeo(
      String deliveryId,
      double mfcLat,
      double mfcLng,
      double pickupLat,
      double pickupLng,
      String pickupSource,
      @Nullable Instant pickupRecordedAt,
      @Nullable String areaLabel,
      int distanceM,
      int durationS,
      Instant now) {
    jdbc.update(
        "UPDATE delivery SET mfc_lat = ?, mfc_lng = ?, drop_lat = ?, drop_lng = ?, pickup_lat = ?,"
            + " pickup_lng = ?, pickup_source = ?, pickup_recorded_at = ?, drop_area_label = ?,"
            + " drop_distance_m = ?, drop_duration_s = ?, delivery_mode = 'SINGLE', geo_error = NULL,"
            + " updated_at = ? WHERE delivery_id = ?",
        mfcLat,
        mfcLng,
        mfcLat,
        mfcLng,
        pickupLat,
        pickupLng,
        pickupSource,
        ts(pickupRecordedAt),
        areaLabel,
        distanceM,
        durationS,
        ts(now),
        deliveryId);
  }

  /** 오배송 재발송 출고의 배송(2026-11-16) — type RESHIPMENT·purpose·caseId. 이미 표시됐으면 그대로. */
  public void markReshipment(String deliveryId, String caseId) {
    jdbc.update(
        "UPDATE delivery SET type = 'RESHIPMENT', purpose = 'MISDELIVERY_RESHIPMENT', case_id ="
            + " COALESCE(case_id, ?) WHERE delivery_id = ?",
        caseId,
        deliveryId);
  }

  /** 같은 오배송 사건으로 처음 만든 배송 시각(회수 배송 — 오배송 확인 때). 없으면 null. */
  public @Nullable Instant caseFirstSeen(String caseId) {
    Timestamp t =
        jdbc.queryForObject(
            "SELECT MIN(created_at) FROM delivery WHERE case_id = ?", Timestamp.class, caseId);
    return t == null ? null : t.toInstant();
  }

  /** DD-4(2026-11-19) — 완료 방법 사본. */
  public void setCompletionMethod(String deliveryId, @Nullable String method) {
    jdbc.update(
        "UPDATE delivery SET completion_method = COALESCE(completion_method, ?) WHERE delivery_id = ?",
        method,
        deliveryId);
  }

  public void setCompletedAt(String deliveryId, Instant completedAt) {
    jdbc.update(
        "UPDATE delivery SET completed_at = COALESCE(completed_at, ?) WHERE delivery_id = ?",
        ts(completedAt),
        deliveryId);
  }

  public void saveGeo(
      String deliveryId,
      double mfcLat,
      double mfcLng,
      double dropLat,
      double dropLng,
      @Nullable String label,
      int dropDistanceM,
      int dropDurationS,
      @Nullable String deliveryMode,
      @Nullable LocalDate arrivalDate,
      @Nullable String arrivalStart,
      @Nullable String arrivalEnd,
      Instant now) {
    jdbc.update(
        "UPDATE delivery SET mfc_lat = ?, mfc_lng = ?, drop_lat = ?, drop_lng = ?, drop_area_label = ?,"
            + " drop_distance_m = ?, drop_duration_s = ?, delivery_mode = COALESCE(?, delivery_mode),"
            + " arrival_date = ?, arrival_start = ?, arrival_end = ?, geo_error = NULL, updated_at = ?"
            + " WHERE delivery_id = ?",
        mfcLat,
        mfcLng,
        dropLat,
        dropLng,
        label,
        dropDistanceM,
        dropDurationS,
        deliveryMode,
        arrivalDate == null ? null : Date.valueOf(arrivalDate),
        arrivalStart,
        arrivalEnd,
        ts(now),
        deliveryId);
  }

  public void setPromise(String deliveryId, @Nullable Instant promisedAt) {
    jdbc.update(
        "UPDATE delivery SET promised_at = ? WHERE delivery_id = ?", ts(promisedAt), deliveryId);
  }

  public void saveGeoError(String deliveryId, String error, Instant now) {
    jdbc.update(
        "UPDATE delivery SET geo_error = ?, updated_at = ? WHERE delivery_id = ?",
        error,
        ts(now),
        deliveryId);
  }

  public void assign(
      String deliveryId,
      String riderId,
      String assignmentId,
      int assignmentVersion,
      String offerId,
      Instant now) {
    jdbc.update(
        "UPDATE delivery SET state = 'ASSIGNED', rider_id = ?, assignment_id = ?, assignment_version = ?,"
            + " assigned_at = ?, accepted_offer_id = ?, version = version + 1, updated_at = ?"
            + " WHERE delivery_id = ?",
        riderId,
        assignmentId,
        assignmentVersion,
        ts(now),
        offerId,
        ts(now),
        deliveryId);
  }

  public void setState(
      String deliveryId, String state, @Nullable String cancelReason, Instant now) {
    jdbc.update(
        "UPDATE delivery SET state = ?, cancel_reason = COALESCE(?, cancel_reason),"
            + " cancelled_at = CASE WHEN ? IN ('CANCELLED_BEFORE_PICKUP', 'BLOCKED') THEN ? ELSE"
            + " cancelled_at END, version = version + 1, updated_at = ? WHERE delivery_id = ?",
        state,
        cancelReason,
        state,
        ts(now),
        ts(now),
        deliveryId);
  }

  public void recordHandover(
      String deliveryId, String handedOverJson, boolean pickedUp, Instant now) {
    jdbc.update(
        "UPDATE delivery SET handed_over_packages = ?::jsonb,"
            + " state = CASE WHEN ? THEN 'PICKED_UP' ELSE state END,"
            + " picked_up_at = CASE WHEN ? THEN ? ELSE picked_up_at END,"
            + " version = version + CASE WHEN ? THEN 1 ELSE 0 END, updated_at = ? WHERE delivery_id = ?",
        handedOverJson,
        pickedUp,
        pickedUp,
        ts(now),
        pickedUp,
        ts(now),
        deliveryId);
  }

  /** 라이더가 지금 맡고 있는 배송(배정 ~ 픽업 전). 픽업 뒤 수행 상태는 execution 이 정본이라 여기서는 픽업까지만 센다. */
  /** 일반배차모드 새 배달 알림 — 라이더마다 마지막 알린 때(없으면 없음). */
  public java.util.Map<String, Instant> lastListNotices(List<String> riderIds) {
    java.util.Map<String, Instant> out = new java.util.HashMap<>();
    if (riderIds.isEmpty()) {
      return out;
    }
    String in = String.join(",", java.util.Collections.nCopies(riderIds.size(), "?"));
    jdbc.query(
        "SELECT rider_id, notified_at FROM list_notice WHERE rider_id IN (" + in + ")",
        rs -> {
          out.put(rs.getString(1), rs.getTimestamp(2).toInstant());
        },
        riderIds.toArray());
    return out;
  }

  /** 알린 때를 남기고 몇 번째 알림인지 돌려준다(사건 버전). */
  public int markListNotice(String riderId, Instant at) {
    Integer n =
        jdbc.queryForObject(
            "INSERT INTO list_notice (rider_id, notified_at, notice_count) VALUES (?, ?, 1) ON"
                + " CONFLICT (rider_id) DO UPDATE SET notified_at = EXCLUDED.notified_at,"
                + " notice_count = list_notice.notice_count + 1 RETURNING notice_count",
            Integer.class,
            riderId,
            Timestamp.from(at));
    return n == null ? 1 : n;
  }

  /** 고객 한집배달(SINGLE — 회수·반납 RETURN 은 아님)을 지금 맡고 있는 라이더(2026-11-18 "한집은 단독"). */
  public java.util.Set<String> singleHolders(List<String> riderIds) {
    if (riderIds.isEmpty()) {
      return java.util.Set.of();
    }
    String in = String.join(",", java.util.Collections.nCopies(riderIds.size(), "?"));
    return new java.util.HashSet<>(
        jdbc.queryForList(
            "SELECT DISTINCT rider_id FROM delivery WHERE state IN ('ASSIGNED', 'PICKED_UP') AND"
                + " delivery_mode = 'SINGLE' AND type <> 'RETURN' AND rider_id IN ("
                + in
                + ")",
            String.class,
            riderIds.toArray()));
  }

  public int activeDeliveries(String riderId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT count(*) FROM delivery WHERE rider_id = ? AND state IN ('ASSIGNED', 'PICKED_UP')",
            Integer.class,
            riderId);
    return n == null ? 0 : n;
  }

  public Set<String> ridersAtOrAbove(Collection<String> riderIds, int limit) {
    if (riderIds.isEmpty()) {
      return Set.of();
    }
    return new TreeSet<>(
        jdbc.queryForList(
            "SELECT rider_id FROM delivery WHERE state IN ('ASSIGNED', 'PICKED_UP') AND rider_id IN ("
                + in(riderIds.size())
                + ") GROUP BY rider_id HAVING count(*) >= ?",
            String.class,
            concat(riderIds, limit)));
  }

  /** 배차 대기 배송과 진행 중 시도의 반경 단계(배달리스트). */
  /**
   * @param excludedRiderId 이 시도에서 빼는 라이더(방금 배차취소한 라이더, 원문 §5.4)
   */
  public record ListedDelivery(
      DeliveryRow delivery, int stage, Instant waitingSince, @Nullable String excludedRiderId) {}

  public List<ListedDelivery> dispatchingWithStage() {
    return jdbc.query(
        "SELECT d.*, a.stage AS attempt_stage, a.started_at AS attempt_started,"
            + " a.excluded_rider_id AS attempt_excluded FROM delivery d JOIN"
            + " dispatch_attempt a ON a.delivery_id ="
            + " d.delivery_id AND a.state IN ('SEARCHING', 'OFFERING') WHERE d.state = 'DISPATCHING'"
            + " AND d.drop_distance_m IS NOT NULL AND d.type <> 'RETURN' AND COALESCE(d.purpose, '') <>"
            + " 'MISDELIVERY_REDELIVERY' ORDER BY d.created_at LIMIT 500",
        (rs, i) ->
            new ListedDelivery(
                delivery(rs),
                rs.getInt("attempt_stage"),
                rs.getTimestamp("attempt_started").toInstant(),
                rs.getString("attempt_excluded")));
  }

  // ---------- 시도 ----------

  public void insertAttempt(String attemptId, String deliveryId, String trigger, Instant now) {
    jdbc.update(
        "INSERT INTO dispatch_attempt (attempt_id, delivery_id, trigger, state, stage, stage_started_at,"
            + " next_run_at, started_at) VALUES (?, ?, ?, 'SEARCHING', 1, ?, ?, ?)",
        attemptId,
        deliveryId,
        trigger,
        ts(now),
        ts(now),
        ts(now));
  }

  /** 재배차 시도 — 방금 취소한 라이더는 뺀다. */
  public void insertAttempt(
      String attemptId,
      String deliveryId,
      String trigger,
      @Nullable String excludedRiderId,
      Instant now) {
    insertAttempt(attemptId, deliveryId, trigger, now);
    jdbc.update(
        "UPDATE dispatch_attempt SET excluded_rider_id = ? WHERE attempt_id = ?",
        excludedRiderId,
        attemptId);
  }

  /** 픽업 전 배정 해제 → 다시 배차 대기(원문 §5.4). */
  public void unassign(String deliveryId, int assignmentVersion, Instant now) {
    jdbc.update(
        "UPDATE delivery SET state = 'DISPATCHING', rider_id = NULL, assignment_id = NULL,"
            + " accepted_offer_id = NULL, assigned_at = NULL, bundle_id = NULL, stop_seq = NULL,"
            + " assignment_version = ?, version = version + 1, updated_at = ? WHERE delivery_id = ?",
        assignmentVersion,
        ts(now),
        deliveryId);
  }

  // ---------- 라이더 잠금·멈춘 주문 ----------

  /** 계정 제한 사본 기록(V7). until 이 null 이면 무기한. */
  public void markRestricted(
      String riderId, String restriction, @Nullable Instant until, Instant now) {
    jdbc.update(
        "INSERT INTO restricted_rider (rider_id, restriction, until_at, updated_at) VALUES (?, ?, ?,"
            + " ?) ON CONFLICT (rider_id) DO UPDATE SET restriction = EXCLUDED.restriction, until_at"
            + " = EXCLUDED.until_at, updated_at = EXCLUDED.updated_at",
        riderId,
        restriction,
        ts(until),
        ts(now));
  }

  public void clearRestricted(String riderId) {
    jdbc.update("DELETE FROM restricted_rider WHERE rider_id = ?", riderId);
  }

  /** 지금 제한 중인가(기한 안). 수락은 라이더 잠금 아래 이것을 다시 본다. */
  public boolean restricted(String riderId, Instant now) {
    Integer n =
        jdbc.queryForObject(
            "SELECT count(*) FROM restricted_rider WHERE rider_id = ? AND (until_at IS NULL OR"
                + " until_at > ?)",
            Integer.class,
            riderId,
            ts(now));
    return n != null && n > 0;
  }

  public Set<String> restrictedAmong(Collection<String> riderIds, Instant now) {
    if (riderIds.isEmpty()) {
      return Set.of();
    }
    String in = String.join(",", Collections.nCopies(riderIds.size(), "?"));
    List<Object> args = new ArrayList<>(riderIds);
    args.add(ts(now));
    return new TreeSet<>(
        jdbc.queryForList(
            "SELECT rider_id FROM restricted_rider WHERE rider_id IN ("
                + in
                + ") AND (until_at IS NULL OR until_at > ?)",
            String.class,
            args.toArray()));
  }

  /** 라이더가 맡고 있는(ASSIGNED·PICKED_UP) 배송(배송 ID 순) — 정지 라이더 로그인 끊기 시점(rider-auth, 2026-11-15). */
  public List<String> activeDeliveryIds(String riderId) {
    return jdbc.queryForList(
        "SELECT delivery_id FROM delivery WHERE rider_id = ? AND state IN ('ASSIGNED', 'PICKED_UP')"
            + " ORDER BY delivery_id",
        String.class,
        riderId);
  }

  /** 라이더가 맡은(ASSIGNED·PICKED_UP) 배송과 그 라이더에게 간 SENT 제안의 배송 — 잠글 순서대로(배송 ID 순). RS-1. */
  public List<String> deliveryIdsTouchingRider(String riderId) {
    return jdbc.queryForList(
        "SELECT delivery_id FROM delivery WHERE rider_id = ? AND state IN ('ASSIGNED', 'PICKED_UP')"
            + " UNION SELECT delivery_id FROM dispatch_offer WHERE rider_id = ? AND state = 'SENT'"
            + " ORDER BY 1",
        String.class,
        riderId,
        riderId);
  }

  /** 같은 라이더의 수락을 줄 세운다(트랜잭션 끝까지) — 진행 중 수를 센 뒤 배정하기까지 다른 수락이 끼지 못하게(PC 1 검토 M1). */
  public void lockRider(String riderId) {
    jdbc.queryForList("SELECT pg_advisory_xact_lock(hashtext(?))", "dispatch-rider:" + riderId);
  }

  public void recordStoppedOrder(String orderId, String stopRequestId, Instant now) {
    jdbc.update(
        "INSERT INTO stopped_order (order_id, stop_request_id, stopped_at) VALUES (?, ?, ?)"
            + " ON CONFLICT (order_id) DO NOTHING",
        orderId,
        stopRequestId,
        ts(now));
  }

  public boolean orderStopped(String orderId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT count(*) FROM stopped_order WHERE order_id = ?", Integer.class, orderId);
    return n != null && n > 0;
  }

  /** 라이더별 최근 배차취소 수(라이더 본인이 한 것) — 배차 알림 감소에 쓴다(제재 아님). */
  public java.util.Map<String, Integer> recentRiderCancellations(
      Collection<String> riderIds, Instant since) {
    java.util.Map<String, Integer> out = new java.util.HashMap<>();
    if (riderIds.isEmpty()) {
      return out;
    }
    Object[] args = new Object[riderIds.size() + 1];
    int i = 0;
    for (String id : riderIds) {
      args[i++] = id;
    }
    args[i] = ts(since);
    jdbc.query(
        "SELECT actor, count(*) FROM assignment_cancellation WHERE requested_by = 'RIDER' AND"
            + " state = 'CANCELLED_BEFORE_PICKUP' AND actor IN ("
            + in(riderIds.size())
            + ") AND created_at >= ? GROUP BY actor",
        rs -> {
          out.put(rs.getString(1), rs.getInt(2));
        },
        args);
    return out;
  }

  // ---------- 배차 취소 기록 ----------

  public record CancellationRow(
      String requestId,
      String deliveryId,
      String requestedBy,
      String actor,
      String reason,
      String state,
      @Nullable String previousRiderId,
      @Nullable String reassignAttemptId,
      Instant createdAt) {}

  /**
   * @return 새로 남겼으면 true — 같은 (요청, 배송)이 이미 있으면 false(다른 X-Source-System 으로 같은 키, PC 1 검토 L2)
   */
  public boolean insertCancellation(
      String requestId,
      String deliveryId,
      String requestedBy,
      String actor,
      String reason,
      @Nullable String detail,
      boolean reassign,
      @Nullable String approvalRef,
      String state,
      @Nullable String previousRiderId,
      @Nullable String reassignAttemptId,
      Instant now) {
    return jdbc.update(
            "INSERT INTO assignment_cancellation (request_id, delivery_id, requested_by, actor, reason,"
                + " reason_detail, reassign, approval_ref, state, previous_rider_id, reassign_attempt_id,"
                + " created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (request_id, delivery_id) DO NOTHING",
            requestId,
            deliveryId,
            requestedBy,
            actor,
            reason,
            detail,
            reassign,
            approvalRef,
            state,
            previousRiderId,
            reassignAttemptId,
            ts(now))
        > 0;
  }

  public Optional<CancellationRow> cancellation(String deliveryId, String requestId) {
    return first(
        "SELECT * FROM assignment_cancellation WHERE delivery_id = ? AND request_id = ?",
        rs ->
            new CancellationRow(
                rs.getString("request_id"),
                rs.getString("delivery_id"),
                rs.getString("requested_by"),
                rs.getString("actor"),
                rs.getString("reason"),
                rs.getString("state"),
                rs.getString("previous_rider_id"),
                rs.getString("reassign_attempt_id"),
                rs.getTimestamp("created_at").toInstant()),
        deliveryId,
        requestId);
  }

  // ---------- 배차 지연 안내 ----------

  /** 지연 안내 후보 — 배차 대기·아직 안내 안 함·약속 시각 있음(잠금). 늦었는지는 호출자가 예상 전달 시각으로 판정. */
  public List<DeliveryRow> lockDelayCandidates(Instant promisedBefore, int limit) {
    return jdbc.query(
        "SELECT * FROM delivery WHERE state = 'DISPATCHING' AND delay_alerted_at IS NULL AND promised_at"
            + " IS NOT NULL AND promised_at < ? ORDER BY promised_at LIMIT ? FOR UPDATE SKIP LOCKED",
        (rs, i) -> delivery(rs),
        ts(promisedBefore),
        limit);
  }

  /**
   * 상태는 그대로 두고 배송 버전만 올린다 — 사건(DispatchOfferSent·DispatchDelayed)마다 aggregateVersion 이 달라지게(PC 1 검토
   * L3).
   *
   * @return 올린 뒤 버전
   */
  public int bumpVersion(String deliveryId, Instant now) {
    Integer v =
        jdbc.queryForObject(
            "UPDATE delivery SET version = version + 1, updated_at = ? WHERE delivery_id = ? RETURNING version",
            Integer.class,
            ts(now),
            deliveryId);
    return v == null ? 0 : v;
  }

  public void markDelayAlerted(String deliveryId, Instant now) {
    jdbc.update(
        "UPDATE delivery SET delay_alerted_at = ? WHERE delivery_id = ?", ts(now), deliveryId);
  }

  public Optional<AttemptRow> openAttempt(String deliveryId) {
    return first(
        "SELECT * FROM dispatch_attempt WHERE delivery_id = ? AND state IN ('SEARCHING', 'OFFERING')",
        DispatchStore::attempt,
        deliveryId);
  }

  public Optional<AttemptRow> lockAttempt(String attemptId) {
    return first(
        "SELECT * FROM dispatch_attempt WHERE attempt_id = ? FOR UPDATE",
        DispatchStore::attempt,
        attemptId);
  }

  public List<AttemptRow> attempts(String deliveryId) {
    return jdbc.query(
        "SELECT * FROM dispatch_attempt WHERE delivery_id = ? ORDER BY started_at",
        (rs, i) -> attempt(rs),
        deliveryId);
  }

  /** 판단할 차례가 된 시도(MFC 와 함께). */
  public record Due(String attemptId, String deliveryId, String mfcId) {}

  public List<Due> dueAttempts(Instant now, int limit) {
    return jdbc.query(
        "SELECT a.attempt_id, a.delivery_id, d.mfc_id FROM dispatch_attempt a JOIN delivery d ON"
            + " d.delivery_id = a.delivery_id WHERE a.state = 'SEARCHING' AND a.next_run_at <= ? AND"
            + " d.state = 'DISPATCHING' ORDER BY a.next_run_at LIMIT ?",
        (rs, i) -> new Due(rs.getString(1), rs.getString(2), rs.getString(3)),
        ts(now),
        limit);
  }

  public void updateAttempt(
      String attemptId,
      String state,
      int stage,
      Instant stageStartedAt,
      int retryCount,
      int round,
      Instant nextRunAt,
      @Nullable Instant finishedAt,
      @Nullable String failureReason) {
    jdbc.update(
        "UPDATE dispatch_attempt SET state = ?, stage = ?, stage_started_at = ?, retry_count = ?,"
            + " round = ?, next_run_at = ?, finished_at = ?, failure_reason = ? WHERE attempt_id = ?",
        state,
        stage,
        ts(stageStartedAt),
        retryCount,
        round,
        ts(nextRunAt),
        ts(finishedAt),
        failureReason,
        attemptId);
  }

  public void postpone(String attemptId, Instant nextRunAt) {
    jdbc.update(
        "UPDATE dispatch_attempt SET next_run_at = ? WHERE attempt_id = ? AND state = 'SEARCHING'",
        ts(nextRunAt),
        attemptId);
  }

  public void markAttempt(String attemptId, boolean candidate, boolean rejection, boolean expiry) {
    jdbc.update(
        "UPDATE dispatch_attempt SET had_candidate = had_candidate OR ?, had_rejection = had_rejection"
            + " OR ?, had_expiry = had_expiry OR ? WHERE attempt_id = ?",
        candidate,
        rejection,
        expiry,
        attemptId);
  }

  public void searchAgain(String attemptId, Instant now) {
    jdbc.update(
        "UPDATE dispatch_attempt SET state = 'SEARCHING', next_run_at = ? WHERE attempt_id = ? AND"
            + " state = 'OFFERING'",
        ts(now),
        attemptId);
  }

  public void finishAttempt(String attemptId, String state, @Nullable String reason, Instant now) {
    jdbc.update(
        "UPDATE dispatch_attempt SET state = ?, finished_at = ?, failure_reason = ? WHERE attempt_id ="
            + " ? AND state IN ('SEARCHING', 'OFFERING')",
        state,
        ts(now),
        reason,
        attemptId);
  }

  // ---------- 제안 ----------

  public void insertOffer(OfferRow o) {
    jdbc.update(
        "INSERT INTO dispatch_offer (offer_id, attempt_id, delivery_id, rider_id, round, source,"
            + " radius_km, state, sent_at, expires_at, decided_at, pickup_distance_m,"
            + " pickup_duration_s, fee, vehicle_type, bundle_id, bundle_size, stop_seq) VALUES (?, ?, ?, ?, ?,"
            + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        o.offerId(),
        o.attemptId(),
        o.deliveryId(),
        o.riderId(),
        o.round(),
        o.source(),
        BigDecimal.valueOf(o.radiusKm()),
        o.state(),
        ts(o.sentAt()),
        ts(o.expiresAt()),
        ts(o.decidedAt()),
        o.pickupDistanceM(),
        o.pickupDurationS(),
        o.fee(),
        o.vehicleType(),
        o.bundleId(),
        o.bundleSize(),
        o.stopSeq());
  }

  /** 같은 묶음의 제안(방문 순서). */
  public List<OfferRow> bundleOffers(String bundleId) {
    return jdbc.query(
        "SELECT * FROM dispatch_offer WHERE bundle_id = ? ORDER BY stop_seq",
        (rs, i) -> offer(rs),
        bundleId);
  }

  public void setBundle(String deliveryId, @Nullable String bundleId, int stopSeq) {
    jdbc.update(
        "UPDATE delivery SET bundle_id = ?, stop_seq = ? WHERE delivery_id = ?",
        bundleId,
        stopSeq,
        deliveryId);
  }

  /** 라이더별 맡고 있는 배송 수(배정 ~ 픽업). */
  public java.util.Map<String, Integer> activeCounts(Collection<String> riderIds) {
    java.util.Map<String, Integer> out = new java.util.HashMap<>();
    if (riderIds.isEmpty()) {
      return out;
    }
    jdbc.query(
        "SELECT rider_id, count(*) FROM delivery WHERE state IN ('ASSIGNED', 'PICKED_UP') AND rider_id IN ("
            + in(riderIds.size())
            + ") GROUP BY rider_id",
        rs -> {
          out.put(rs.getString(1), rs.getInt(2));
        },
        riderIds.toArray());
    return out;
  }

  public Optional<OfferRow> offer(String offerId) {
    return first("SELECT * FROM dispatch_offer WHERE offer_id = ?", DispatchStore::offer, offerId);
  }

  public Optional<OfferRow> lockOffer(String offerId) {
    return first(
        "SELECT * FROM dispatch_offer WHERE offer_id = ? FOR UPDATE",
        DispatchStore::offer,
        offerId);
  }

  public List<OfferRow> offersOfDelivery(String deliveryId) {
    return jdbc.query(
        "SELECT * FROM dispatch_offer WHERE delivery_id = ? ORDER BY sent_at",
        (rs, i) -> offer(rs),
        deliveryId);
  }

  public List<OfferRow> offersOfAttempt(String attemptId) {
    return jdbc.query(
        "SELECT * FROM dispatch_offer WHERE attempt_id = ? ORDER BY sent_at",
        (rs, i) -> offer(rs),
        attemptId);
  }

  /** 라이더 본인 제안(최근 순). state 가 null 이면 만료 전 SENT 만. */
  public List<OfferRow> riderOffers(
      String riderId, @Nullable String state, Instant now, int limit, int offset) {
    if (state == null) {
      return jdbc.query(
          "SELECT * FROM dispatch_offer WHERE rider_id = ? AND state = 'SENT' AND expires_at > ? ORDER"
              + " BY sent_at DESC LIMIT ? OFFSET ?",
          (rs, i) -> offer(rs),
          riderId,
          ts(now),
          limit,
          offset);
    }
    return jdbc.query(
        "SELECT * FROM dispatch_offer WHERE rider_id = ? AND state = ? ORDER BY sent_at DESC LIMIT ?"
            + " OFFSET ?",
        (rs, i) -> offer(rs),
        riderId,
        state,
        limit,
        offset);
  }

  public Set<String> offeredInRound(String attemptId, int round) {
    return new TreeSet<>(
        jdbc.queryForList(
            "SELECT DISTINCT rider_id FROM dispatch_offer WHERE attempt_id = ? AND round = ?",
            String.class,
            attemptId,
            round));
  }

  public Set<String> ridersWithLiveOffer(Instant now) {
    return new TreeSet<>(
        jdbc.queryForList(
            "SELECT DISTINCT rider_id FROM dispatch_offer WHERE state = 'SENT' AND expires_at > ?",
            String.class,
            ts(now)));
  }

  /** 응답 없이 만료된 제안(잠금 — 다른 인스턴스와 겹치지 않게). */
  public List<OfferRow> lockExpired(Instant now, int limit) {
    return jdbc.query(
        "SELECT * FROM dispatch_offer WHERE state = 'SENT' AND expires_at <= ? ORDER BY expires_at"
            + " LIMIT ? FOR UPDATE SKIP LOCKED",
        (rs, i) -> offer(rs),
        ts(now),
        limit);
  }

  /**
   * 응답 전(SENT) 제안만 결정한다 — 이미 만료·결정된 제안을 덮지 않는다(PC 1 PR #13 검토: 제한 처리가 만료 처리와 겹치면 EXPIRED 가 LATE 로
   * 덮였다). @return 바꿨으면 true
   */
  public boolean decideOffer(
      String offerId,
      String state,
      Instant now,
      @Nullable String rejectReason,
      @Nullable String lateReason) {
    return 1
        == jdbc.update(
            "UPDATE dispatch_offer SET state = ?, decided_at = ?, reject_reason = ?, late_reason = ?"
                + " WHERE offer_id = ? AND state = 'SENT'",
            state,
            ts(now),
            rejectReason,
            lateReason,
            offerId);
  }

  /** 배정이 정해지면 그 배송의 남은 제안은 SUPERSEDED. */
  public void supersedeOpenOffers(String deliveryId, Instant now, String lateReason) {
    jdbc.update(
        "UPDATE dispatch_offer SET state = 'SUPERSEDED', decided_at = ?, late_reason = ? WHERE"
            + " delivery_id = ? AND state = 'SENT'",
        ts(now),
        lateReason,
        deliveryId);
  }

  public int liveOffersInAttempt(String attemptId, Instant now) {
    Integer n =
        jdbc.queryForObject(
            "SELECT count(*) FROM dispatch_offer WHERE attempt_id = ? AND state = 'SENT' AND expires_at"
                + " > ?",
            Integer.class,
            attemptId,
            ts(now));
    return n == null ? 0 : n;
  }

  public void markViewed(String offerId, Instant now) {
    jdbc.update(
        "UPDATE dispatch_offer SET viewed_at = ? WHERE offer_id = ? AND viewed_at IS NULL",
        ts(now),
        offerId);
  }

  // ---------- 연속 거절 ----------

  public int addRejection(String riderId, Instant now) {
    Integer n =
        jdbc.queryForObject(
            "INSERT INTO rider_rejection (rider_id, consecutive, updated_at) VALUES (?, 1, ?) ON"
                + " CONFLICT (rider_id) DO UPDATE SET consecutive = rider_rejection.consecutive + 1,"
                + " updated_at = EXCLUDED.updated_at RETURNING consecutive",
            Integer.class,
            riderId,
            ts(now));
    return n == null ? 0 : n;
  }

  public void resetRejections(String riderId, Instant now) {
    jdbc.update(
        "INSERT INTO rider_rejection (rider_id, consecutive, updated_at) VALUES (?, 0, ?) ON CONFLICT"
            + " (rider_id) DO UPDATE SET consecutive = 0, updated_at = EXCLUDED.updated_at",
        riderId,
        ts(now));
  }

  public int rejections(String riderId) {
    List<Integer> n =
        jdbc.queryForList(
            "SELECT consecutive FROM rider_rejection WHERE rider_id = ?", Integer.class, riderId);
    return n.isEmpty() ? 0 : n.get(0);
  }

  // ---------- 매핑 ----------

  private interface Mapper<T> {
    T map(ResultSet rs) throws SQLException;
  }

  private <T> Optional<T> first(String sql, Mapper<T> m, Object... args) {
    List<T> rows = jdbc.query(sql, (rs, i) -> m.map(rs), args);
    return rows.stream().findFirst();
  }

  static DeliveryRow delivery(ResultSet rs) throws SQLException {
    Date ad = rs.getDate("arrival_date");
    return new DeliveryRow(
        rs.getString("delivery_id"),
        rs.getInt("version"),
        rs.getString("type"),
        rs.getString("order_id"),
        rs.getString("shipment_id"),
        rs.getString("mfc_id"),
        rs.getString("state"),
        rs.getString("delivery_mode"),
        ad == null ? null : ad.toLocalDate(),
        rs.getString("arrival_start"),
        rs.getString("arrival_end"),
        rs.getString("request_trigger"),
        instant(rs, "estimated_ready_at"),
        instant(rs, "ready_at"),
        rs.getString("packages"),
        rs.getObject("mfc_lat", Double.class),
        rs.getObject("mfc_lng", Double.class),
        rs.getObject("drop_lat", Double.class),
        rs.getObject("drop_lng", Double.class),
        rs.getString("drop_area_label"),
        rs.getObject("drop_distance_m", Integer.class),
        rs.getObject("drop_duration_s", Integer.class),
        rs.getString("geo_error"),
        rs.getString("rider_id"),
        rs.getString("assignment_id"),
        rs.getInt("assignment_version"),
        instant(rs, "assigned_at"),
        rs.getString("accepted_offer_id"),
        instant(rs, "picked_up_at"),
        rs.getString("handed_over_packages"),
        instant(rs, "cancelled_at"),
        rs.getString("cancel_reason"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("updated_at").toInstant(),
        rs.getString("bundle_id"),
        rs.getObject("stop_seq", Integer.class),
        instant(rs, "delay_alerted_at"),
        instant(rs, "promised_at"),
        rs.getString("purpose"),
        rs.getString("case_id"),
        rs.getString("origin_delivery_id"),
        rs.getObject("pickup_lat", Double.class),
        rs.getObject("pickup_lng", Double.class),
        rs.getString("pickup_source"),
        instant(rs, "pickup_recorded_at"),
        instant(rs, "completed_at"),
        rs.getString("redelivery_destination_id"),
        rs.getObject("straight_distance_m", Integer.class),
        rs.getBoolean("payment_required"),
        instant(rs, "released_at"),
        rs.getString("claim_id"),
        rs.getString("return_lines"),
        rs.getString("pickup_request_id"),
        rs.getObject("delivery_seq", Integer.class));
  }

  static AttemptRow attempt(ResultSet rs) throws SQLException {
    return new AttemptRow(
        rs.getString("attempt_id"),
        rs.getString("delivery_id"),
        rs.getString("trigger"),
        rs.getString("state"),
        rs.getInt("stage"),
        rs.getTimestamp("stage_started_at").toInstant(),
        rs.getInt("retry_count"),
        rs.getInt("round"),
        rs.getTimestamp("next_run_at").toInstant(),
        rs.getTimestamp("started_at").toInstant(),
        instant(rs, "finished_at"),
        rs.getString("failure_reason"),
        rs.getBoolean("had_candidate"),
        rs.getBoolean("had_rejection"),
        rs.getBoolean("had_expiry"),
        rs.getString("excluded_rider_id"));
  }

  static OfferRow offer(ResultSet rs) throws SQLException {
    return new OfferRow(
        rs.getString("offer_id"),
        rs.getString("attempt_id"),
        rs.getString("delivery_id"),
        rs.getString("rider_id"),
        rs.getInt("round"),
        rs.getString("source"),
        rs.getBigDecimal("radius_km").doubleValue(),
        rs.getString("state"),
        rs.getTimestamp("sent_at").toInstant(),
        rs.getTimestamp("expires_at").toInstant(),
        instant(rs, "viewed_at"),
        instant(rs, "decided_at"),
        rs.getString("reject_reason"),
        rs.getString("late_reason"),
        rs.getObject("pickup_distance_m", Integer.class),
        rs.getObject("pickup_duration_s", Integer.class),
        rs.getInt("fee"),
        rs.getString("vehicle_type"),
        rs.getString("bundle_id"),
        rs.getInt("bundle_size"),
        rs.getInt("stop_seq"));
  }

  private static @Nullable Instant instant(ResultSet rs, String col) throws SQLException {
    Timestamp t = rs.getTimestamp(col);
    return t == null ? null : t.toInstant();
  }

  private static @Nullable Timestamp ts(@Nullable Instant i) {
    return i == null ? null : Timestamp.from(i);
  }

  private static String in(int n) {
    return String.join(",", Collections.nCopies(n, "?"));
  }

  private static Object[] concat(Collection<String> ids, Object last) {
    Object[] a = new Object[ids.size() + 1];
    int i = 0;
    for (String id : ids) {
      a[i++] = id;
    }
    a[i] = last;
    return a;
  }
}
