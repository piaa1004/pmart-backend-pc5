package com.pmart.delivery.dispatch.application;

import com.pmart.common.web.PmartException;
import com.pmart.delivery.dispatch.domain.AcceptRules;
import com.pmart.delivery.dispatch.domain.BundleProblem;
import com.pmart.delivery.dispatch.domain.FeeTable;
import com.pmart.delivery.dispatch.domain.Geo;
import com.pmart.delivery.dispatch.domain.OperatingHours;
import com.pmart.delivery.dispatch.domain.SearchRules;
import com.pmart.delivery.dispatch.infrastructure.AiOptimizerClient;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore.AttemptRow;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore.DeliveryRow;
import com.pmart.delivery.dispatch.infrastructure.DispatchStore.OfferRow;
import com.pmart.delivery.dispatch.infrastructure.Upstreams;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * AI 추천 배차 판단 주기(원문 §2.1 ①~④, ADR-0058 — Python AI 서버 전까지 OR-Tools).
 *
 * <ol>
 *   <li>응답 없이 만료된 제안 → EXPIRED(상세를 열었으면 거절 횟수에 넣는다 — §4.4), 그 시도는 다시 탐색.
 *   <li>판단할 차례가 된 시도를 MFC 별로 모은다. 배차 가능 시간(08~24시) 밖이면 다음 시작까지 미룬다.
 *   <li>배송마다 좌표 준비(한 번): MFC 좌표(center-master), 배송지(order → serviceability 정확 일치), MFC → 고객 도로
 *       거리(tracking 카카오모빌리티). 실패하면 사유를 남기고 30초 뒤 다시(직선거리로 대신하지 않는다).
 *   <li>후보: 적격 라이더(rider) 중 AI 추천 모드·살아 있는 제안 없음·맡은 배송 한도 밑, 위치(tracking)가 최신이고 그 시도의 반경 단계 안(MFC
 *       중심 직선 — 사용자 2026-11-13), 이번 회차에 이미 제안받지 않음.
 *   <li>라이더 → MFC 도로 소요(카카오모빌리티)로 {@link AssignmentProblem} → {@link AssignmentSolver}(OR-Tools
 *       CP-SAT).
 *   <li>짝지어진 묶음은 배송마다 제안(1분, 같은 bundleId·방문 순서) + DispatchOfferSent. 못 지은 배송은 {@link
 *       SearchRules}(단계 머물기·다음 단계·재요청·실패).
 * </ol>
 *
 * 일반 배차모드 라이더는 제안을 받지 않고 배달리스트에서 고른다({@link OfferService}). 상태 변경은 짧은 트랜잭션마다 행 잠금 아래에서 다시 확인한다.
 *
 * <p>오배송 회수 배송(DD-2, 2026-11-16 — {@link RecoveryHandler})은 픽업지가 MFC 가 아니라 따로 판단한다({@link
 * #runRecovery}): 좌표 준비 = 회수지(원래 라이더가 완료를 누른 곳의 위치 점, 없으면 원래 배송지 좌표) + 회수지 → MFC 도로 거리, 후보 = 회수지 중심
 * 직선 반경 안 AI 추천 라이더(원래 라이더 제외), 회수지까지 도로 시간이 가장 짧은 한 명에게 1건 제안(묶지 않음). 일반 배송 제안을 먼저 보낸 뒤라 살아 있는 제안은
 * 다시 읽는다.
 */
@Service
public class DispatchEngine {

  private static final Logger log = LoggerFactory.getLogger(DispatchEngine.class);
  private static final Duration RETRY_LATER = Duration.ofSeconds(30);

  /** 배차 후보로 쓸 수 있는 위치 품질(AI 추천·배달리스트 같은 기준 — PC 1 검토 L5). */
  static final Set<String> USABLE_QUALITY = Set.of("GOOD", "LOW_ACCURACY");

  private final DispatchStore store;
  private final Upstreams upstreams;
  private final AssignmentSolver solver;
  private final AiOptimizerClient ai;
  private final com.pmart.delivery.dispatch.infrastructure.RouteCache routes;
  private final org.springframework.beans.factory.ObjectProvider<
          io.micrometer.core.instrument.MeterRegistry>
      meters;
  private final DeliveryTransitions transitions;
  private final DispatchEvents events;
  private final DispatchProperties props;
  private final TransactionTemplate tx;
  private final Clock clock;

  public DispatchEngine(
      DispatchStore store,
      Upstreams upstreams,
      AssignmentSolver solver,
      AiOptimizerClient ai,
      com.pmart.delivery.dispatch.infrastructure.RouteCache routes,
      org.springframework.beans.factory.ObjectProvider<io.micrometer.core.instrument.MeterRegistry>
          meters,
      DeliveryTransitions transitions,
      DispatchEvents events,
      DispatchProperties props,
      TransactionTemplate tx,
      Clock clock) {
    this.store = store;
    this.upstreams = upstreams;
    this.solver = solver;
    this.ai = ai;
    this.routes = routes;
    this.meters = meters;
    this.transitions = transitions;
    this.events = events;
    this.props = props;
    this.tx = tx;
    this.clock = clock;
  }

  @Scheduled(
      fixedDelayString = "${pmart.dispatch.tick-interval:PT5S}",
      initialDelayString = "${pmart.dispatch.tick-initial-delay:PT10S}")
  public void scheduledTick() {
    try {
      tick();
    } catch (RuntimeException e) {
      log.error("배차 판단 주기 실패 — 다음 주기에 다시", e);
    }
  }

  /** 한 주기. 시험은 직접 부른다. */
  public void tick() {
    Instant now = clock.instant();
    expireOffers(now);
    alertDelays(now);
    Map<String, List<DispatchStore.Due>> byMfc = new LinkedHashMap<>();
    for (DispatchStore.Due d : store.dueAttempts(now, 50)) {
      byMfc.computeIfAbsent(d.mfcId(), k -> new ArrayList<>()).add(d);
    }
    for (var e : byMfc.entrySet()) {
      try {
        runMfc(e.getKey(), e.getValue(), now);
      } catch (RuntimeException ex) {
        boolean quota =
            ex instanceof org.springframework.web.client.HttpClientErrorException.TooManyRequests
                || (ex instanceof PmartException pe && "QUOTA_EXCEEDED".equals(pe.code()));
        Instant later = now.plus(quota ? props.quotaBackoff() : RETRY_LATER);
        log.error(
            "MFC {} 배차 판단 실패 — {} 뒤 다시",
            e.getKey(),
            quota ? props.quotaBackoff() : RETRY_LATER,
            ex);
        e.getValue()
            .forEach(
                d ->
                    tx.executeWithoutResult(
                        s ->
                            store.postpone(
                                d.attemptId(), later))); // 429 면 quota-backoff(PC 1 재검토 ②)
      }
    }
  }

  // ---------- 1. 만료 ----------

  void expireOffers(Instant now) {
    tx.executeWithoutResult(
        s -> {
          Set<String> bundlesCounted = new HashSet<>();
          for (OfferRow o : store.lockExpired(now, 200)) {
            store.decideOffer(o.offerId(), "EXPIRED", now, null, null);
            store.markAttempt(o.attemptId(), false, false, true);
            boolean firstOfBundle = o.bundleId() == null || bundlesCounted.add(o.bundleId());
            if (firstOfBundle && AcceptRules.expiryCountsAsRejection(o.viewedAt())) {
              store.addRejection(o.riderId(), now);
            }
            if (store.liveOffersInAttempt(o.attemptId(), now) == 0) {
              store.searchAgain(o.attemptId(), now);
            }
          }
        });
  }

  /**
   * 배차 지연 안내 — 예상 전달 시각이 배송 약속(한집·알뜰 = 주문 확정 + 50분, 예약 = 도착 시간대 끝)보다 delayAlertAfter(20분) 넘게 늦으면 한
   * 번 DispatchDelayed(사용자 2026-11-13). 라이더가 아직 없으므로 예상 = 지금(또는 출하 대기) + MFC → 고객 도로 시간 — 가장 빠른 경우라
   * 실제는 더 늦을 수 있다.
   */
  void alertDelays(Instant now) {
    tx.executeWithoutResult(
        s -> {
          int threshold = (int) props.delayAlertAfter().toMinutes();
          for (DeliveryRow d :
              store.lockDelayCandidates(now.plus(props.delayAlertAfter()).plusSeconds(7200), 200)) {
            if (d.dropDurationS() == null) {
              continue;
            }
            Instant ready = d.readyOrEstimate();
            Instant depart = ready != null && ready.isAfter(now) ? ready : now;
            Instant eta = depart.plusSeconds(d.dropDurationS());
            if (eta.isAfter(d.promisedAt().plus(props.delayAlertAfter()))) {
              int minutes = (int) Duration.between(d.promisedAt(), eta).toMinutes();
              store.markDelayAlerted(d.deliveryId(), now);
              events.delayed(d, minutes, threshold, now, store.bumpVersion(d.deliveryId(), now));
              log.warn("배송 {} 배차 지연 — 약속보다 {}분 늦을 것(임계 {}분)", d.deliveryId(), minutes, threshold);
            }
          }
        });
  }

  // ---------- 2~6. 한 MFC ----------

  private record Ready(DeliveryRow delivery, AttemptRow attempt, double radiusKm) {}

  private void runMfc(String mfcId, List<DispatchStore.Due> dues, Instant now) {
    OperatingHours hours = props.hours();
    if (!hours.isOpen(now)) {
      Instant open = hours.nextOpen(now);
      dues.forEach(d -> tx.executeWithoutResult(s -> store.postpone(d.attemptId(), open)));
      return;
    }
    List<Ready> ready = new ArrayList<>();
    for (DispatchStore.Due due : dues) {
      Optional<DeliveryRow> d = store.delivery(due.deliveryId());
      Optional<AttemptRow> a = store.openAttempt(due.deliveryId());
      if (d.isEmpty() || a.isEmpty() || !"SEARCHING".equals(a.get().state())) {
        continue;
      }
      DeliveryRow row = d.get();
      if (!row.geoReady()) {
        row = prepareGeo(row, a.get(), now).orElse(null);
        if (row == null) {
          continue;
        }
      }
      ready.add(new Ready(row, a.get(), SearchRules.radiusKm(props.radiiKm(), a.get().stage())));
    }
    List<Ready> recoveries =
        ready.stream()
            .filter(r -> r.delivery().pickupBased() && !r.delivery().awaitingPayment())
            .toList();
    List<Ready> forward = ready.stream().filter(r -> !r.delivery().pickupBased()).toList();
    if (!forward.isEmpty()) {
      runForward(mfcId, forward, now);
    }
    if (!recoveries.isEmpty()) {
      runRecovery(mfcId, recoveries, now);
    }
  }

  private void runForward(String mfcId, List<Ready> ready, Instant now) {
    Geo mfc = new Geo(ready.get(0).delivery().mfcLat(), ready.get(0).delivery().mfcLng());

    // 후보 라이더(AI 추천 모드만 제안을 받는다 — 일반 모드는 배달리스트)
    List<Upstreams.Rider> eligible =
        upstreams.eligibleRiders(mfcId, props.maxRadiusKm()).stream()
            .filter(r -> r.acceptsNewDispatch() && "AI_RECOMMENDED".equals(r.dispatchMode()))
            .toList();
    Set<String> busy = new java.util.HashSet<>(store.ridersWithLiveOffer(now));
    // rider 적격 조회가 계정 제한 반영 전이어도 사본으로 뺀다(RS-1)
    busy.addAll(
        store.restrictedAmong(eligible.stream().map(Upstreams.Rider::riderId).toList(), now));
    List<String> ids =
        eligible.stream().map(Upstreams.Rider::riderId).filter(id -> !busy.contains(id)).toList();
    Map<String, Integer> active = store.activeCounts(ids);
    java.util.Set<String> holdsSingle = store.singleHolders(ids); // 한집은 단독(2026-11-18)
    Map<String, Upstreams.Rider> byId = new HashMap<>();
    eligible.forEach(r -> byId.put(r.riderId(), r));
    Map<String, Integer> room = new HashMap<>();
    for (String id : ids) {
      if (holdsSingle.contains(id)) {
        continue;
      }
      int limit = byId.get(id).oneAtATime() ? 1 : props.maxActiveDeliveries();
      int left = limit - active.getOrDefault(id, 0);
      if (left > 0) {
        room.put(id, left);
      }
    }
    List<String> free = ids.stream().filter(room::containsKey).toList();

    Map<String, Geo> located = new HashMap<>();
    Map<String, Double> straight = new HashMap<>();
    for (Upstreams.Location l : upstreams.locations(free)) {
      boolean fresh = !l.receivedAt().plus(props.locationMaxAge()).isBefore(now);
      if (fresh && USABLE_QUALITY.contains(l.quality()) && free.contains(l.riderId())) {
        located.put(l.riderId(), l.at());
        straight.put(l.riderId(), l.at().metersTo(mfc));
      }
    }

    // 시도마다 반경 안·이번 회차 미제안 라이더
    List<Set<String>> allowedSets = new ArrayList<>();
    Set<String> union = new HashSet<>();
    for (Ready r : ready) {
      Set<String> offered =
          new HashSet<>(store.offeredInRound(r.attempt().attemptId(), r.attempt().round()));
      if (r.attempt().excludedRiderId() != null) {
        offered.add(r.attempt().excludedRiderId()); // 방금 배차취소한 라이더(원문 §5.4 다른 라이더 재배차)
      }
      Set<String> ok = new HashSet<>();
      for (var e : straight.entrySet()) {
        if (e.getValue() <= r.radiusKm() * 1000.0 && !offered.contains(e.getKey())) {
          ok.add(e.getKey());
        }
      }
      allowedSets.add(ok);
      union.addAll(ok);
    }
    List<String> riders =
        union.stream()
            .sorted(Comparator.comparingDouble(straight::get))
            .limit(30) // 거리 행렬 한 번에 30개까지
            .toList();

    // 라이더 → MFC 도로 소요(카카오모빌리티)
    Upstreams.Leg[] legs = new Upstreams.Leg[0];
    if (!riders.isEmpty()) {
      legs = routes.toMfc(mfcId, riders.stream().map(located::get).toList(), mfc);
    }
    List<Integer> usable = new ArrayList<>();
    for (int i = 0; i < riders.size(); i++) {
      if (legs[i].ok()) {
        usable.add(i);
      }
    }
    int nr = usable.size();
    int nd = ready.size();
    int[] pickup = new int[nr];
    int[] capacity = new int[nr];
    boolean[][] allowed = new boolean[nr][nd];
    for (int i = 0; i < nr; i++) {
      String id = riders.get(usable.get(i));
      pickup[i] = legs[usable.get(i)].durationS();
      capacity[i] = room.get(id);
      for (int d = 0; d < nd; d++) {
        // 한집배달은 아무것도 맡지 않은 라이더에게만(2026-11-18 "한집은 단독")
        allowed[i][d] =
            allowedSets.get(d).contains(id)
                && !(ready.get(d).delivery().customerSingle() && active.getOrDefault(id, 0) > 0);
      }
    }
    int[] readyIn = new int[nd];
    int[] direct = new int[nd];
    boolean[] bundlable = new boolean[nd];
    int[] promiseIn = new int[nd];
    // 최근 반복 배차취소 → 제안 순위를 뒤로(제재 아님 — 사용자 2026-11-13)
    Map<String, Integer> cancels =
        store.recentRiderCancellations(
            usable.stream().map(riders::get).toList(), now.minus(props.cancellationWindow()));
    int[] penalty = new int[nr];
    for (int i = 0; i < nr; i++) {
      penalty[i] =
          (int)
              (cancels.getOrDefault(riders.get(usable.get(i)), 0)
                  * props.cancellationPenalty().toSeconds());
    }
    for (int d = 0; d < nd; d++) {
      DeliveryRow row = ready.get(d).delivery();
      Instant at = row.readyOrEstimate();
      readyIn[d] = at == null ? 0 : (int) Math.max(0, Duration.between(now, at).toSeconds());
      direct[d] = row.dropDurationS() == null ? 0 : row.dropDurationS();
      bundlable[d] = "BUNDLED".equals(row.deliveryMode()); // 알뜰배달만 묶는다(원문 §5.3)
      promiseIn[d] =
          row.promisedAt() == null
              ? Integer.MAX_VALUE
              : (int)
                  Math.max(Integer.MIN_VALUE, Duration.between(now, row.promisedAt()).toSeconds());
    }
    int[][] between = betweenDrops(ready, bundlable);
    int maxSize = Math.min(3, props.maxActiveDeliveries());
    int maxExtra = (int) props.bundleMaxExtra().toSeconds();
    List<BundleProblem.Bundle> bundles = null;
    BundleProblem.Solution sol = null;
    // 1) Python AI 최적화 서버(RouteFinder) — 검증한 묶음만 쓴다(ADR-0058 결정 13)
    if (ai.enabled() && nr > 0) {
      List<AiOptimizerClient.Delivery> ds = new ArrayList<>();
      Map<String, Integer> dIndex = new HashMap<>();
      for (int d = 0; d < nd; d++) {
        DeliveryRow row = ready.get(d).delivery();
        dIndex.put(row.deliveryId(), d);
        ds.add(
            new AiOptimizerClient.Delivery(
                row.deliveryId(),
                new Geo(row.dropLat(), row.dropLng()),
                direct[d],
                readyIn[d],
                bundlable[d]));
      }
      List<AiOptimizerClient.Rider> rs = new ArrayList<>();
      Map<String, Integer> rIndex = new HashMap<>();
      for (int i = 0; i < nr; i++) {
        String id = riders.get(usable.get(i));
        rIndex.put(id, i);
        List<String> ok = new ArrayList<>();
        for (int d = 0; d < nd; d++) {
          if (allowed[i][d]) {
            ok.add(ready.get(d).delivery().deliveryId());
          }
        }
        rs.add(new AiOptimizerClient.Rider(id, pickup[i], capacity[i], ok));
      }
      Optional<AiOptimizerClient.Plan> plan =
          ai.plan(mfc, ds, rs, between, maxSize, maxExtra, props.lateWeight());
      if (plan.isPresent()) {
        List<int[]> raw = new ArrayList<>();
        for (AiOptimizerClient.Assignment a : plan.get().assignments()) {
          int[] row = new int[a.deliveryIds().size() + 1];
          row[0] = rIndex.getOrDefault(a.riderId(), -1);
          for (int k = 0; k < a.deliveryIds().size(); k++) {
            row[k + 1] = dIndex.getOrDefault(a.deliveryIds().get(k), -1);
          }
          raw.add(row);
        }
        BundleProblem.Checked checked =
            BundleProblem.check(
                raw, direct, between, bundlable, maxSize, maxExtra, capacity, allowed, pickup,
                readyIn, promiseIn);
        if (checked.dropped() > 0) {
          log.warn("AI 배정안 {}건이 검증을 통과하지 못해 버림(MFC {})", checked.dropped(), mfcId);
          count("pmart.dispatch.ai.dropped", checked.dropped());
        }
        if (!checked.bundles().isEmpty()) {
          bundles = checked.bundles();
          sol = new BundleProblem.Solution(checked.riderForBundle(), "AI " + plan.get().solver());
        } else {
          count("pmart.dispatch.ai.fallback", 1);
        }
      } else {
        count("pmart.dispatch.ai.fallback", 1);
      }
    }
    // 2) OR-Tools CP-SAT(AI 서버가 없거나 응답·검증 실패)
    if (bundles == null) {
      bundles =
          BundleProblem.candidates(
              direct, between, bundlable, maxSize, maxExtra, props.bundleNeighbors());
      BundleProblem problem =
          new BundleProblem(
              pickup, allowed, capacity, readyIn, bundles, props.lateWeight(), promiseIn, penalty);
      sol = nr == 0 ? problem.greedy() : solver.solve(problem, props.solverTimeLimit());
    }

    boolean[] offered = new boolean[nd];
    for (int b = 0; b < bundles.size(); b++) {
      int ri = sol.riderForBundle()[b];
      if (ri < 0) {
        continue;
      }
      BundleProblem.Bundle bundle = bundles.get(b);
      String riderId = riders.get(usable.get(ri));
      List<Ready> members = new ArrayList<>();
      for (int d : bundle.deliveries()) {
        members.add(ready.get(d));
        offered[d] = true;
      }
      offer(members, byId.get(riderId), legs[usable.get(ri)], sol.solver(), now);
    }
    for (int d = 0; d < nd; d++) {
      if (!offered[d]) {
        noCandidate(ready.get(d).attempt().attemptId(), ready.get(d).delivery().deliveryId(), now);
      }
    }
  }

  /**
   * 오배송 회수(DD-2) — 한 건씩, 회수지 중심. 적격 AI 추천 라이더 중 살아 있는 제안 없음·계정 제한 없음·맡은 배송 한도 밑·위치 최신, 회수지 직선 반경(그
   * 시도 단계) 안, 원래 라이더·시도 제외 라이더·이번 회차 제안받은 라이더 제외 → 회수지까지 도로 시간이 가장 짧은 사람(가까운 10명 안). 비용·지연 약속이 없어
   * 묶음 계산(CP-SAT)을 쓰지 않는다.
   */
  private void runRecovery(String mfcId, List<Ready> recoveries, Instant now) {
    List<Upstreams.Rider> eligible =
        upstreams.eligibleRiders(mfcId, props.maxRadiusKm()).stream()
            .filter(r -> r.acceptsNewDispatch() && "AI_RECOMMENDED".equals(r.dispatchMode()))
            .toList();
    Map<String, Upstreams.Rider> byId = new HashMap<>();
    eligible.forEach(r -> byId.put(r.riderId(), r));
    List<String> all = eligible.stream().map(Upstreams.Rider::riderId).toList();
    Set<String> taken = new HashSet<>(); // 이번 주기에 회수 제안을 받은 라이더
    for (Ready r : recoveries) {
      DeliveryRow d = r.delivery();
      if (d.failedReturn() && !d.failedReturnByOther()) {
        assignFailedReturn(r, now);
        continue;
      }
      // 2026-11-20 사용자 "가": 다른 라이더 반납을 failedReturnFallbackAfter(5분) 동안 못 맡기면 상품을 든 원래 라이더에게 바로
      if (d.failedReturnByOther()
          && !d.createdAt().plus(props.failedReturnFallbackAfter()).isAfter(now)) {
        Boolean switched = tx.execute(s -> store.switchFailedReturnToSameRider(d.deliveryId()));
        if (Boolean.TRUE.equals(switched)) {
          log.warn(
              "반납 배송 {} — 다른 라이더를 {} 동안 못 구해 원래 라이더 반납으로 바꿈",
              d.deliveryId(),
              props.failedReturnFallbackAfter());
          assignFailedReturn(r, now);
          continue;
        }
      }
      Geo pickup = new Geo(d.pickupLat(), d.pickupLng());
      Set<String> skip = new HashSet<>(store.ridersWithLiveOffer(now)); // 일반 배송 제안 뒤 다시 읽는다
      skip.addAll(store.restrictedAmong(all, now));
      skip.addAll(taken);
      skip.addAll(store.offeredInRound(r.attempt().attemptId(), r.attempt().round()));
      if (r.attempt().excludedRiderId() != null) {
        skip.add(r.attempt().excludedRiderId());
      }
      String originRider =
          d.originDeliveryId() == null
              ? null
              : store.delivery(d.originDeliveryId()).map(DeliveryRow::riderId).orElse(null);
      if ((d.recovery() || d.failedReturnByOther()) && originRider != null) {
        skip.add(originRider); // 회수: 원래 라이더(ADR-0052 "인근 다른 라이더") — 재배차 시도에도
      }
      // 다시 배송 200m 안(ADR-0065): 원래 기사에게 먼저 — 배달료 0(추가 운행료 없음), 반경 상관없이. 거절·만료면 다음부터 근처 다른 라이더
      if (d.redelivery()
          && !d.paymentRequired()
          && originRider != null
          && !skip.contains(originRider)
          && byId.containsKey(originRider)) {
        Integer busyCount = store.activeCounts(List.of(originRider)).getOrDefault(originRider, 0);
        int limit =
            store.singleHolders(List.of(originRider)).isEmpty()
                ? (byId.get(originRider).oneAtATime() ? 1 : props.maxActiveDeliveries())
                : 0; // 한집을 맡고 있으면 다른 배송 없음(2026-11-18)
        Upstreams.Location loc =
            upstreams.locations(List.of(originRider)).stream().findFirst().orElse(null);
        if (busyCount < limit
            && loc != null
            && !loc.receivedAt().plus(props.locationMaxAge()).isBefore(now)
            && USABLE_QUALITY.contains(loc.quality())) {
          Upstreams.Leg leg = upstreams.matrix(List.of(loc.at()), List.of(pickup))[0][0];
          if (leg.ok()) {
            offer(List.of(r), byId.get(originRider), leg, "REDELIVERY original rider", now, 0);
            taken.add(originRider);
            continue;
          }
        }
      }
      if (d.redelivery() && originRider != null && !d.paymentRequired()) {
        skip.add(originRider); // 원래 기사가 지금 못 받으면 근처 다른 라이더(일반 배달료)
      }
      List<String> ids = all.stream().filter(id -> !skip.contains(id)).toList();
      Map<String, Integer> active = store.activeCounts(ids);
      java.util.Set<String> holdsSingle = store.singleHolders(ids); // 한집은 단독(2026-11-18)
      List<String> free =
          ids.stream()
              .filter(id -> !holdsSingle.contains(id))
              .filter(
                  id ->
                      (byId.get(id).oneAtATime() ? 1 : props.maxActiveDeliveries())
                          > active.getOrDefault(id, 0))
              .toList();
      Map<String, Geo> located = new HashMap<>();
      Map<String, Double> straight = new HashMap<>();
      for (Upstreams.Location l : upstreams.locations(free)) {
        boolean fresh = !l.receivedAt().plus(props.locationMaxAge()).isBefore(now);
        double m = l.at().metersTo(pickup);
        if (fresh
            && USABLE_QUALITY.contains(l.quality())
            && free.contains(l.riderId())
            && m <= r.radiusKm() * 1000.0) {
          located.put(l.riderId(), l.at());
          straight.put(l.riderId(), m);
        }
      }
      List<String> near =
          straight.keySet().stream()
              .sorted(Comparator.comparingDouble(straight::get))
              .limit(10)
              .toList();
      if (near.isEmpty()) {
        noCandidate(r.attempt().attemptId(), d.deliveryId(), now);
        continue;
      }
      Upstreams.Leg[][] legs =
          upstreams.matrix(near.stream().map(located::get).toList(), List.of(pickup));
      int best = -1;
      for (int i = 0; i < near.size(); i++) {
        if (legs[i][0].ok() && (best < 0 || legs[i][0].durationS() < legs[best][0].durationS())) {
          best = i;
        }
      }
      if (best < 0) {
        noCandidate(r.attempt().attemptId(), d.deliveryId(), now);
        continue;
      }
      String riderId = near.get(best);
      offer(
          List.of(r),
          byId.get(riderId),
          legs[best][0],
          d.redelivery()
              ? "REDELIVERY nearest"
              : d.claimReturn()
                  ? "CLAIM_RETURN nearest"
                  : d.failedReturn() ? "FAILED_RETURN other rider" : "RECOVERY nearest",
          now);
      taken.add(riderId);
    }
  }

  /** 알뜰배달 고객 사이 도로 소요(카카오모빌리티). 알뜰배달이 2건 미만이면 부르지 않는다. 실패는 묶음 없이(1건씩) 계산. */
  private int[][] betweenDrops(List<Ready> ready, boolean[] bundlable) {
    int nd = ready.size();
    int[][] between = new int[nd][nd];
    for (int[] row : between) {
      java.util.Arrays.fill(row, -1);
    }
    List<Integer> idx = new ArrayList<>();
    for (int d = 0; d < nd; d++) {
      if (bundlable[d]) {
        idx.add(d);
      }
    }
    if (idx.size() < 2) {
      return between;
    }
    List<Integer> use = idx.subList(0, Math.min(idx.size(), 10)); // 묶음 후보는 한 번에 10건까지
    List<Geo> drops =
        use.stream()
            .map(d -> new Geo(ready.get(d).delivery().dropLat(), ready.get(d).delivery().dropLng()))
            .toList();
    try {
      int[][] m =
          routes.betweenDeliveries(
              use.stream().map(d -> ready.get(d).delivery().deliveryId()).toList(), drops);
      for (int i = 0; i < use.size(); i++) {
        for (int j = 0; j < use.size(); j++) {
          if (i != j && m[i][j] >= 0) {
            between[use.get(i)][use.get(j)] = m[i][j];
          }
        }
      }
    } catch (PmartException e) {
      log.warn("알뜰배달 고객 사이 도로 거리 조회 실패({}) — 이번 판단은 묶지 않는다", e.code());
    }
    return between;
  }

  /** 묶음(1~3건) 제안 — 배송마다 제안 한 줄, 같은 bundleId·방문 순서. 모두 아직 배차 대기·탐색 중일 때만(행 잠금, 배송 ID 순). */
  private void offer(
      List<Ready> members,
      Upstreams.Rider rider,
      Upstreams.Leg leg,
      String solverName,
      Instant now) {
    offer(members, rider, leg, solverName, now, null);
  }

  /**
   * @param fee 정해진 배달료(다시 배송 원래 기사 0원 — ADR-0065). null 이면 원문 §7.1 표
   */
  private void offer(
      List<Ready> members,
      Upstreams.Rider rider,
      Upstreams.Leg leg,
      String solverName,
      Instant now,
      @org.jspecify.annotations.Nullable Integer fee) {
    tx.executeWithoutResult(
        s -> {
          // 라이더 잠금 아래 제한 사본을 다시 본다(RS-1, 사용자 2026-11-14 "제한된 계정에는 배차가 가면 안 된다") — 후보는 잠금 밖에서
          // 골라 제한 처리와 겹칠 수 있다. 잠금 순서는 수락·제한 처리와 같다(라이더 → 배송 ID 순).
          store.lockRider(rider.riderId());
          if (store.restricted(rider.riderId(), now)) {
            log.info("라이더 {} 계정 제한 — 제안 보내지 않음", rider.riderId());
            return;
          }
          List<Ready> sorted =
              members.stream()
                  .sorted(Comparator.comparing((Ready m) -> m.delivery().deliveryId()))
                  .toList();
          Map<String, DeliveryRow> d = new HashMap<>();
          Map<String, AttemptRow> a = new HashMap<>();
          for (Ready m : sorted) {
            DeliveryRow x = store.lockDelivery(m.delivery().deliveryId()).orElse(null);
            AttemptRow y = store.lockAttempt(m.attempt().attemptId()).orElse(null);
            if (x == null
                || y == null
                || !"DISPATCHING".equals(x.state())
                || !"SEARCHING".equals(y.state())) {
              return;
            }
            d.put(x.deliveryId(), x);
            a.put(x.deliveryId(), y);
          }
          String bundleId = members.size() > 1 ? "BDL-" + UUID.randomUUID() : null;
          Instant expires = now.plus(props.offerTimeout());
          int seq = 0;
          for (Ready m : members) {
            seq++;
            DeliveryRow x = d.get(m.delivery().deliveryId());
            AttemptRow y = a.get(m.delivery().deliveryId());
            OfferRow o =
                new OfferRow(
                    "OFR-" + UUID.randomUUID(),
                    y.attemptId(),
                    x.deliveryId(),
                    rider.riderId(),
                    y.round(),
                    "AI_RECOMMENDED",
                    m.radiusKm(),
                    "SENT",
                    now,
                    expires,
                    null,
                    null,
                    null,
                    null,
                    leg.distanceM(),
                    leg.durationS(),
                    fee != null ? fee : FeeTable.estimate(rider.vehicleType(), x.dropDistanceM()),
                    rider.vehicleType(),
                    bundleId,
                    members.size(),
                    seq);
            store.insertOffer(o);
            store.updateAttempt(
                y.attemptId(),
                "OFFERING",
                y.stage(),
                y.stageStartedAt(),
                y.retryCount(),
                y.round(),
                expires,
                null,
                null);
            store.markAttempt(y.attemptId(), true, false, false);
            events.offerSent(x, o, store.bumpVersion(x.deliveryId(), now));
          }
          log.info(
              "제안 배송 {}건{} → 라이더 {} ({})",
              members.size(),
              bundleId == null ? "" : " 묶음 " + bundleId,
              rider.riderId(),
              solverName);
        });
  }

  /**
   * 전달 실패 반납(2026-11-17): 상품을 든 원래 라이더에게 제안 없이 바로 배정 — 운행 상태·신규배차·계정 제한과 상관없이(상품을 돌려놓아야 한다). 배달료 =
   * 일반 표(고객 집 → MFC, 원래 배송 수락 수단 — 사용자 "원배송 수행료 + 반납 이동료 따로"). 지급 여부(고객 사유가 아니면 지원센터 조정)는 execution
   * 이 반납 때 정한다. 원래 라이더를 모르면 SYSTEM_FAILURE(사건함).
   */
  private void assignFailedReturn(Ready r, Instant now) {
    DeliveryRow origin =
        r.delivery().originDeliveryId() == null
            ? null
            : store.delivery(r.delivery().originDeliveryId()).orElse(null);
    if (origin == null || origin.riderId() == null) {
      failSystem(r.delivery(), r.attempt(), "NO_ORIGIN_RIDER", now);
      return;
    }
    String riderId = origin.riderId();
    String vehicle =
        origin.acceptedOfferId() == null
            ? null
            : store.offer(origin.acceptedOfferId()).map(OfferRow::vehicleType).orElse(null);
    tx.executeWithoutResult(
        s -> {
          store.lockRider(riderId);
          DeliveryRow x = store.lockDelivery(r.delivery().deliveryId()).orElse(null);
          AttemptRow y = store.lockAttempt(r.attempt().attemptId()).orElse(null);
          if (x == null
              || y == null
              || !"DISPATCHING".equals(x.state())
              || !"SEARCHING".equals(y.state())) {
            return;
          }
          OfferRow o =
              new OfferRow(
                  "OFR-" + UUID.randomUUID(),
                  y.attemptId(),
                  x.deliveryId(),
                  riderId,
                  y.round(),
                  "FAILED_RETURN",
                  0,
                  "SENT",
                  now,
                  now,
                  now,
                  null,
                  null,
                  null,
                  null,
                  null,
                  vehicle == null || x.dropDistanceM() == null
                      ? 0
                      : FeeTable.estimate(vehicle, x.dropDistanceM()),
                  vehicle,
                  null,
                  1,
                  1);
          store.insertOffer(o);
          transitions.assign(x, riderId, o.offerId(), null, 1, now);
        });
    log.info("반납 배송 {} → 원래 라이더 {} 바로 배정", r.delivery().deliveryId(), riderId);
  }

  private void noCandidate(String attemptId, String deliveryId, Instant now) {
    tx.executeWithoutResult(
        s -> {
          DeliveryRow d = store.lockDelivery(deliveryId).orElse(null);
          AttemptRow a = store.lockAttempt(attemptId).orElse(null);
          if (d == null || a == null || !"SEARCHING".equals(a.state())) {
            return;
          }
          switch (SearchRules.whenNoCandidate(
              a.stage(),
              a.stageStartedAt(),
              a.retryCount(),
              now,
              props.radiiKm(),
              props.offerTimeout(),
              props.maxRetries())) {
            case STAY -> store.postpone(a.attemptId(), now.plus(props.searchInterval()));
            case NEXT_STAGE ->
                store.updateAttempt(
                    a.attemptId(),
                    "SEARCHING",
                    a.stage() + 1,
                    now,
                    a.retryCount(),
                    a.round(),
                    now,
                    null,
                    null);
            case RETRY -> {
              Instant at = now.plus(props.retryInterval());
              store.updateAttempt(
                  a.attemptId(),
                  "SEARCHING",
                  1,
                  at,
                  a.retryCount() + 1,
                  a.round() + 1,
                  at,
                  null,
                  null);
              log.info(
                  "배송 {} 재요청 {}회 — {} 뒤", deliveryId, a.retryCount() + 1, props.retryInterval());
            }
            case FAIL -> {
              String reason = SearchRules.failureReason(a.hadRejection(), a.hadExpiry());
              String next =
                  transitions.failAndRedispatch(d, a, reason, now.plus(props.retryInterval()), now);
              log.warn(
                  "배송 {} 배차 실패 {} — {} 뒤 다시 배차 요청({})",
                  deliveryId,
                  reason,
                  props.retryInterval(),
                  next);
            }
          }
        });
  }

  private void count(String name, int n) {
    io.micrometer.core.instrument.MeterRegistry r = meters.getIfAvailable();
    if (r != null) {
      r.counter(name).increment(n);
    }
  }

  // ---------- 좌표 준비 ----------

  private Optional<DeliveryRow> prepareGeo(DeliveryRow d, AttemptRow a, Instant now) {
    if (d.recovery()) {
      return prepareRecoveryGeo(d, a, now);
    }
    if (d.failedReturnByOther()) {
      return prepareFailedReturnOtherGeo(d, a, now);
    }
    if (d.claimReturn() || d.failedReturn()) {
      // 전달 실패 반납도 출발 = 고객 집(라이더가 실패를 등록한 곳), 도착 = MFC
      return prepareClaimReturnGeo(d, a, now);
    }
    if (d.redelivery()) {
      return prepareRedeliveryGeo(d, a, now);
    }
    try {
      Upstreams.Destination dest = upstreams.destination(d.orderId());
      if ("CANCELLED".equals(dest.status())) {
        tx.executeWithoutResult(
            s ->
                store
                    .lockDelivery(d.deliveryId())
                    .filter(x -> "DISPATCHING".equals(x.state()))
                    .ifPresent(x -> transitions.block(x, now)));
        log.info("배송 {} 주문 취소됨 — 신규 배차 차단", d.deliveryId());
        return Optional.empty();
      }
      if (dest.roadAddress() == null) {
        return failSystem(d, a, "NO_ADDRESS", now);
      }
      Optional<Geo> mfc = upstreams.centerCoordinates(d.mfcId());
      if (mfc.isEmpty()) {
        return later(d, a, "NO_MFC_COORDINATES", now);
      }
      Optional<Upstreams.Place> place = upstreams.geocode(dest.roadAddress());
      if (place.isEmpty()) {
        return failSystem(d, a, "ADDRESS_NOT_GEOCODED", now);
      }
      Upstreams.Leg leg = upstreams.matrix(List.of(mfc.get()), List.of(place.get().at()))[0][0];
      if (!leg.ok()) {
        return failSystem(d, a, "NO_ROUTE_TO_CUSTOMER", now);
      }
      tx.executeWithoutResult(
          s ->
              store.saveGeo(
                  d.deliveryId(),
                  mfc.get().lat(),
                  mfc.get().lng(),
                  place.get().at().lat(),
                  place.get().at().lng(),
                  place.get().areaLabel(),
                  leg.distanceM(),
                  leg.durationS(),
                  dest.deliveryMode(),
                  dest.arrivalDate(),
                  dest.arrivalStart(),
                  dest.arrivalEnd(),
                  now));
      tx.executeWithoutResult(
          s ->
              store.setPromise(
                  d.deliveryId(),
                  "MISDELIVERY_RESHIPMENT".equals(d.purpose())
                      ? reshipmentPromise(d)
                      : promise(dest)));
      // 라이더용 주문 요약 사본(라이더 앱 요청 §1 — 사용자 "수락 전부터 전부"). 못 받으면 없이 배차한다
      upstreams
          .riderSummaryJson(d.orderId(), d.mfcId())
          .ifPresent(j -> store.saveOrderSummary(d.deliveryId(), j));
      return store.delivery(d.deliveryId());
    } catch (PmartException e) {
      log.warn("배송 {} 좌표·도로 거리 준비 실패({}) — 30초 뒤 다시", d.deliveryId(), e.code());
      return later(d, a, e.code(), now);
    }
  }

  /**
   * 회수 배송 좌표(DD-2, 사용자 2026-11-16 "추천대로"): 회수지 = 원래 라이더가 전달 완료를 누른 시각(execution DeliveryCompleted
   * deliveredAt — 이 열 전 완료분은 배송 갱신 시각)의 위치 점(tracking 앞뒤 3분), 없으면 원래 배송지 좌표. 도착 = MFC, 거리 = 회수지 →
   * MFC 도로 거리(예상 배달료 — 일반 표). 원래 배송 완료가 아직 배차 쪽에 안 왔으면(PICKED_UP) 30초 뒤 다시.
   */
  private Optional<DeliveryRow> prepareRecoveryGeo(DeliveryRow d, AttemptRow a, Instant now) {
    try {
      Optional<Geo> mfc = upstreams.centerCoordinates(d.mfcId());
      if (mfc.isEmpty()) {
        return later(d, a, "NO_MFC_COORDINATES", now);
      }
      DeliveryRow origin =
          d.originDeliveryId() == null ? null : store.delivery(d.originDeliveryId()).orElse(null);
      if (origin == null) {
        return failSystem(d, a, "NO_ORIGIN_DELIVERY", now);
      }
      if (origin.completedAt() == null && !"CLOSED".equals(origin.state())) {
        return later(d, a, "ORIGIN_NOT_COMPLETED", now);
      }
      Pickup pk = pickupOf(origin);
      if (pk == null) {
        return failSystem(d, a, "NO_PICKUP_POINT", now);
      }
      Geo pickup = pk.at();
      String source = pk.source();
      Instant recordedAt = pk.recordedAt();
      Upstreams.Leg leg = upstreams.matrix(List.of(pickup), List.of(mfc.get()))[0][0];
      if (!leg.ok()) {
        return failSystem(d, a, "NO_ROUTE_TO_MFC", now);
      }
      Geo p = pickup;
      String src = source;
      Instant rec = recordedAt;
      tx.executeWithoutResult(
          s ->
              store.saveRecoveryGeo(
                  d.deliveryId(),
                  mfc.get().lat(),
                  mfc.get().lng(),
                  p.lat(),
                  p.lng(),
                  src,
                  rec,
                  origin.dropAreaLabel(),
                  leg.distanceM(),
                  leg.durationS(),
                  now));
      log.info("회수 배송 {} 회수지 준비 — {}, 회수지 → MFC {}m", d.deliveryId(), src, leg.distanceM());
      return store.delivery(d.deliveryId());
    } catch (PmartException e) {
      log.warn("회수 배송 {} 좌표·도로 거리 준비 실패({}) — 30초 뒤 다시", d.deliveryId(), e.code());
      return later(d, a, e.code(), now);
    }
  }

  /**
   * 고객 반품 회수 좌표(CL-1, 2026-11-17 — 사용자 "일반 표·고객 집 → MFC"): 회수지 = 고객 집 — 원래 배송의 배송지 좌표, 배차 쪽에 없으면 주문
   * 배송지(order O-4) → serviceability 좌표. 도착 = MFC, 거리 = 고객 집 → MFC 도로 거리(예상 배달료 — 일반 표).
   */
  private Optional<DeliveryRow> prepareClaimReturnGeo(DeliveryRow d, AttemptRow a, Instant now) {
    try {
      Optional<Geo> mfc = upstreams.centerCoordinates(d.mfcId());
      if (mfc.isEmpty()) {
        return later(d, a, "NO_MFC_COORDINATES", now);
      }
      DeliveryRow origin =
          d.originDeliveryId() == null ? null : store.delivery(d.originDeliveryId()).orElse(null);
      Geo pickup;
      String area;
      if (origin != null && origin.dropLat() != null && origin.dropLng() != null) {
        pickup = new Geo(origin.dropLat(), origin.dropLng());
        area = origin.dropAreaLabel();
      } else {
        Upstreams.Destination dest = upstreams.destination(d.orderId());
        if (dest.roadAddress() == null) {
          return failSystem(d, a, "NO_ADDRESS", now);
        }
        Optional<Upstreams.Place> place = upstreams.geocode(dest.roadAddress());
        if (place.isEmpty()) {
          return failSystem(d, a, "ADDRESS_NOT_GEOCODED", now);
        }
        pickup = place.get().at();
        area = place.get().areaLabel();
      }
      Upstreams.Leg leg = upstreams.matrix(List.of(pickup), List.of(mfc.get()))[0][0];
      if (!leg.ok()) {
        return failSystem(d, a, "NO_ROUTE_TO_MFC", now);
      }
      Geo p = pickup;
      String areaLabel = area;
      tx.executeWithoutResult(
          s ->
              store.saveRecoveryGeo(
                  d.deliveryId(),
                  mfc.get().lat(),
                  mfc.get().lng(),
                  p.lat(),
                  p.lng(),
                  "DROP_ADDRESS",
                  null,
                  areaLabel,
                  leg.distanceM(),
                  leg.durationS(),
                  now));
      log.info("반품 회수 배송 {} 회수지(고객 집) 준비 — 고객 집 → MFC {}m", d.deliveryId(), leg.distanceM());
      return store.delivery(d.deliveryId());
    } catch (PmartException e) {
      log.warn("반품 회수 배송 {} 좌표·도로 거리 준비 실패({}) — 30초 뒤 다시", d.deliveryId(), e.code());
      return later(d, a, e.code(), now);
    }
  }

  /**
   * 전달 실패 반납을 다른 라이더가(2026-11-17): 회수지 = 원래 라이더의 마지막 위치(상품이 그 라이더에게 있다 — 오래됐어도), 없으면 원래 배송지. 회수지 →
   * MFC 도로 거리로 일반 표.
   */
  private Optional<DeliveryRow> prepareFailedReturnOtherGeo(
      DeliveryRow d, AttemptRow a, Instant now) {
    try {
      Optional<Geo> mfc = upstreams.centerCoordinates(d.mfcId());
      if (mfc.isEmpty()) {
        return later(d, a, "NO_MFC_COORDINATES", now);
      }
      DeliveryRow origin =
          d.originDeliveryId() == null ? null : store.delivery(d.originDeliveryId()).orElse(null);
      Geo pickup = null;
      String source = "RIDER_LOCATION";
      if (origin != null && origin.riderId() != null) {
        pickup =
            upstreams.locations(List.of(origin.riderId())).stream()
                .findFirst()
                .map(Upstreams.Location::at)
                .orElse(null);
      }
      if (pickup == null
          && origin != null
          && origin.dropLat() != null
          && origin.dropLng() != null) {
        pickup = new Geo(origin.dropLat(), origin.dropLng());
        source = "DROP_ADDRESS";
      }
      if (pickup == null) {
        return failSystem(d, a, "NO_PICKUP_LOCATION", now);
      }
      Upstreams.Leg leg = upstreams.matrix(List.of(pickup), List.of(mfc.get()))[0][0];
      if (!leg.ok()) {
        return failSystem(d, a, "NO_ROUTE_TO_MFC", now);
      }
      Geo p = pickup;
      String src = source;
      String area = origin == null ? null : origin.dropAreaLabel();
      tx.executeWithoutResult(
          s ->
              store.saveRecoveryGeo(
                  d.deliveryId(),
                  mfc.get().lat(),
                  mfc.get().lng(),
                  p.lat(),
                  p.lng(),
                  src,
                  null,
                  area,
                  leg.distanceM(),
                  leg.durationS(),
                  now));
      log.info("전달 실패 반납(다른 라이더) {} 회수지 준비({}) — MFC 까지 {}m", d.deliveryId(), src, leg.distanceM());
      return store.delivery(d.deliveryId());
    } catch (PmartException e) {
      log.warn("전달 실패 반납 {} 좌표 준비 실패({}) — 30초 뒤 다시", d.deliveryId(), e.code());
      return later(d, a, e.code(), now);
    }
  }

  /** 잘못 둔 곳(회수·다시 배송 픽업지). */
  private record Pickup(
      Geo at, String source, @org.jspecify.annotations.Nullable Instant recordedAt) {}

  /** 원래 라이더가 완료를 누른 시각의 위치 점(앞뒤 3분), 없으면 원래 배송지 좌표. 둘 다 없으면 null. */
  private @org.jspecify.annotations.Nullable Pickup pickupOf(DeliveryRow origin) {
    Instant done = origin.completedAt() != null ? origin.completedAt() : origin.updatedAt();
    if (origin.riderId() != null) {
      Optional<Upstreams.PointAt> at = upstreams.locationAt(origin.riderId(), done);
      if (at.isPresent()) {
        return new Pickup(at.get().at(), "RIDER_COMPLETION_LOCATION", at.get().recordedAt());
      }
    }
    if (origin.dropLat() != null && origin.dropLng() != null) {
      return new Pickup(new Geo(origin.dropLat(), origin.dropLng()), "DROP_ADDRESS", null);
    }
    return null;
  }

  /**
   * 오배송 다시 배송 좌표(ADR-0065): 픽업 = 잘못 둔 곳, 도착 = 상담원이 기록한 실제 집(order → serviceability 정확 일치 좌표), 잘못 둔
   * 곳 ↔ 실제 집 직선 200m 미만이면 원래 기사에게 먼저, 200m 이상이면 고객 앱 9,000원 결제 뒤에만(RedeliveryPaymentRequired — 시도는
   * 결제 확인까지 미룬다). 배달료 거리 = 픽업 → 집 도로 거리.
   */
  private Optional<DeliveryRow> prepareRedeliveryGeo(DeliveryRow d, AttemptRow a, Instant now) {
    try {
      DeliveryRow origin =
          d.originDeliveryId() == null ? null : store.delivery(d.originDeliveryId()).orElse(null);
      if (origin == null) {
        return failSystem(d, a, "NO_ORIGIN_DELIVERY", now);
      }
      if (origin.completedAt() == null && !"CLOSED".equals(origin.state())) {
        return later(d, a, "ORIGIN_NOT_COMPLETED", now);
      }
      Pickup pk = pickupOf(origin);
      if (pk == null) {
        return failSystem(d, a, "NO_PICKUP_POINT", now);
      }
      Optional<String> road =
          d.redeliveryDestinationId() == null
              ? Optional.empty()
              : upstreams.redeliveryAddress(d.orderId(), d.redeliveryDestinationId());
      if (road.isEmpty()) {
        return failSystem(d, a, "NO_REDELIVERY_ADDRESS", now);
      }
      Optional<Upstreams.Place> home = upstreams.geocode(road.get());
      if (home.isEmpty()) {
        return failSystem(d, a, "ADDRESS_NOT_GEOCODED", now);
      }
      Upstreams.Leg leg = upstreams.matrix(List.of(pk.at()), List.of(home.get().at()))[0][0];
      if (!leg.ok()) {
        return failSystem(d, a, "NO_ROUTE_TO_CUSTOMER", now);
      }
      int straight = (int) Math.round(pk.at().metersTo(home.get().at()));
      boolean pay = straight >= REDELIVERY_FREE_WITHIN_M;
      tx.executeWithoutResult(
          s -> {
            store.saveRedeliveryGeo(
                d.deliveryId(),
                origin.mfcLat() == null ? pk.at().lat() : origin.mfcLat(),
                origin.mfcLng() == null ? pk.at().lng() : origin.mfcLng(),
                pk.at().lat(),
                pk.at().lng(),
                pk.source(),
                pk.recordedAt(),
                home.get().at().lat(),
                home.get().at().lng(),
                home.get().areaLabel(),
                leg.distanceM(),
                leg.durationS(),
                straight,
                pay,
                now);
            store.setPromise(d.deliveryId(), reshipmentPromise(d));
            if (pay) {
              // 결제 확인까지 배차하지 않는다(orchestration 이 결제를 받으면 풀어 준다 — 4단계)
              store.postpone(a.attemptId(), now.plus(java.time.Duration.ofDays(1)));
              DeliveryRow x = store.lockDelivery(d.deliveryId()).orElseThrow();
              events.redeliveryPaymentRequired(
                  x, CUSTOMER_FAULT_FEE, store.bumpVersion(d.deliveryId(), now));
            }
          });
      log.info(
          "다시 배송 {} 준비 — {}, 직선 {}m{}",
          d.deliveryId(),
          pk.source(),
          straight,
          pay ? " → 9,000원 결제 대기" : " → 원래 기사 먼저");
      return pay ? Optional.empty() : store.delivery(d.deliveryId());
    } catch (PmartException e) {
      log.warn("다시 배송 {} 좌표·도로 거리 준비 실패({}) — 30초 뒤 다시", d.deliveryId(), e.code());
      return later(d, a, e.code(), now);
    }
  }

  /** 이 거리 미만이면 원래 기사가 무료로 다시 배송(ADR-0065 — 사용자 "200m이내는 기사가 다시 배송"). */
  static final int REDELIVERY_FREE_WITHIN_M = 200;

  /** 200m 이상 다시 배송 고객 부담(원문 §9.2 회수재배송 9000원, ADR-0065). */
  static final long CUSTOMER_FAULT_FEE = 9_000;

  /**
   * 오배송 재발송 배송 약속(2026-11-16 — PC 5 추천, 문서 규칙 아님): 오배송 확인(같은 사건 회수 배송이 생긴 때 — 사건 소비 시각, 없으면 이 배송이
   * 생긴 때) + deliveryPromise(50분). 원래 주문의 약속은 이미 지났다.
   */
  private Instant reshipmentPromise(DeliveryRow d) {
    Instant from = d.caseId() == null ? null : store.caseFirstSeen(d.caseId());
    return (from == null ? d.createdAt() : from).plus(props.deliveryPromise());
  }

  /**
   * 배송 약속 — 예약은 도착 시간대 끝(Asia/Seoul, 사용자 확인 2026-11-13), 한집·알뜰은 주문 확정 + deliveryPromise(50분). 모르면
   * null.
   */
  private @org.jspecify.annotations.Nullable Instant promise(Upstreams.Destination dest) {
    if ("SCHEDULED".equals(dest.deliveryMode())
        && dest.arrivalDate() != null
        && dest.arrivalEnd() != null) {
      java.time.LocalDate day = dest.arrivalDate();
      String end = dest.arrivalEnd();
      if ("24:00".equals(end)) {
        return day.plusDays(1).atStartOfDay(OperatingHours.SEOUL).toInstant();
      }
      return day.atTime(java.time.LocalTime.parse(end)).atZone(OperatingHours.SEOUL).toInstant();
    }
    return dest.confirmedAt() == null ? null : dest.confirmedAt().plus(props.deliveryPromise());
  }

  private Optional<DeliveryRow> later(DeliveryRow d, AttemptRow a, String reason, Instant now) {
    tx.executeWithoutResult(
        s -> {
          store.saveGeoError(d.deliveryId(), reason, now);
          store.postpone(a.attemptId(), now.plus(RETRY_LATER));
        });
    return Optional.empty();
  }

  private Optional<DeliveryRow> failSystem(
      DeliveryRow d, AttemptRow a, String reason, Instant now) {
    tx.executeWithoutResult(
        s -> {
          DeliveryRow x = store.lockDelivery(d.deliveryId()).orElse(null);
          AttemptRow y = store.lockAttempt(a.attemptId()).orElse(null);
          if (x == null || y == null || !"SEARCHING".equals(y.state())) {
            return;
          }
          store.saveGeoError(d.deliveryId(), reason, now);
          transitions.fail(x, y, "SYSTEM_FAILURE", now);
        });
    log.error("배송 {} 배차 불가({}) — SYSTEM_FAILURE", d.deliveryId(), reason);
    return Optional.empty();
  }
}
