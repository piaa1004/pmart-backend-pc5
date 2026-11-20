package com.pmart.delivery.dispatch.application;

import com.pmart.delivery.dispatch.domain.OperatingHours;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code pmart.dispatch.*} — 기본값은 업무 원문 값(§3.2·§3.5·§2.4·§4.4)과 사용자 결정(배차 시간 08~24시, 2026-11-13).
 *
 * @param radiiKm 반경 단계(직선, MFC 중심) — 원문 1·2·3km
 * @param offerTimeout 응답 대기 — 원문 1분(단계에 머무는 시간도 같다)
 * @param retryInterval 재요청 간격 — 원문 10분
 * @param maxRetries 재요청 횟수 — 원문 최대 2회
 * @param maxActiveDeliveries 동시에 맡는 배송 — 원문 최대 3
 * @param rejectionNotice 연속 거절 안내 — 원문 5회
 * @param operatingStart 배차 가능 시작(Asia/Seoul) — 사용자 08:00
 * @param operatingEnd 배차 가능 끝 — 24:00
 * @param locationMaxAge 이보다 오래된 라이더 위치는 후보에서 뺀다(설계값 — 원문 "위치 최신성")
 * @param tickInterval 배차 판단 주기(설계값)
 * @param lateWeight 출하 대기보다 늦게 도착하는 초에 주는 가중(설계값 — 원문 §3.1 "포장이 끝날 즈음 도착")
 * @param solverTimeLimit 한 번 계산 상한
 * @param pollAfterSec 라이더 앱 폴링 권장(초) — 계약 잠정 5초
 * @param idlePollAfterSec 지금 제안을 못 받는 상태일 때 폴링 권장(초, DD-5 2026-11-19 — null 로 끊지 않는다) — 기본 30초
 * @param delayAlertAfter 예상 전달이 약속 시각보다 이만큼 넘게 늦으면 DispatchDelayed(사용자 2026-11-13 "안내기준보다 20분 더
 *     늦춰지면")
 * @param cancellationWindow 최근 배차취소를 세는 기간(7일 — 사용자 확인 2026-11-13)
 * @param cancellationPenalty AI 추천 제안에서 최근 배차취소 1건마다 비용에 더하는 시간(3분 — 다른 라이더가 먼저, 사용자 확인 2026-11-13)
 * @param listDelayPerCancellation 일반 모드 배달리스트에 새 배송이 보이기까지 배차취소 1건마다 늦추는 시간(1분 — 사용자 확인 2026-11-13)
 * @param searchInterval 제안할 사람이 없을 때 그 시도를 다시 볼 간격(설계값 30초 — 카카오 호출 줄이기 M3)
 * @param quotaBackoff 카카오 사용량 초과(429) 때 그 MFC 판단을 미루는 시간(설계값 5분)
 * @param routeCacheTtl 라이더 → MFC 도로 시간을 다시 쓰는 시간(설계값 5분, 약 100m 칸)
 * @param deliveryPromise 배송 약속 — 한집·알뜰은 주문 확정 + 이 값(사용자 2026-11-13 "주문한지 50분 이내 전달"), 예약은 도착 시간대 끝
 * @param bundleMaxExtra 묶음 보조 상한(바로 갈 때보다 이만큼 넘게 늦으면 안 묶음) — 주 조건은 약속 시각(BundleProblem), 기본 50분
 * @param bundleNeighbors 알뜰배달 묶음 후보를 고객마다 가까운 이만큼으로 줄인다(0 = 모든 조합). 기본 4 — 대기 20~40건에서 모든 조합은 후보가
 *     수천 개라 CP-SAT 가 2초 안에 80~90% 못 풀고 규칙 기반으로 떨어졌다. 4 면 0건, 배정 +7~10%(2026-11-13 측정,
 *     LargeMfcBenchmarkTest)
 * @param failedReturnFallbackAfter 다른 라이더 반납(FAILURE_OTHER)을 이만큼 못 맡기면 원래 라이더에게 바로(2026-11-20 사용자
 *     "가" — 기본 30분)
 */
@ConfigurationProperties(prefix = "pmart.dispatch")
public record DispatchProperties(
    @Nullable List<Double> radiiKm,
    @Nullable Duration offerTimeout,
    @Nullable Duration retryInterval,
    @Nullable Integer maxRetries,
    @Nullable Integer maxActiveDeliveries,
    @Nullable Integer rejectionNotice,
    @Nullable String operatingStart,
    @Nullable String operatingEnd,
    @Nullable Duration locationMaxAge,
    @Nullable Duration tickInterval,
    @Nullable Integer lateWeight,
    @Nullable Duration solverTimeLimit,
    @Nullable Integer pollAfterSec,
    @Nullable Integer idlePollAfterSec,
    @Nullable Duration bundleMaxExtra,
    @Nullable Duration delayAlertAfter,
    @Nullable Duration deliveryPromise,
    @Nullable Duration searchInterval,
    @Nullable Duration quotaBackoff,
    @Nullable Duration routeCacheTtl,
    @Nullable Duration cancellationWindow,
    @Nullable Duration cancellationPenalty,
    @Nullable Duration listDelayPerCancellation,
    @Nullable Integer bundleNeighbors,
    @Nullable Duration failedReturnFallbackAfter,
    @Nullable Ai ai,
    @Nullable Urls urls) {

  public DispatchProperties {
    radiiKm = radiiKm == null || radiiKm.isEmpty() ? List.of(1.0, 2.0, 3.0) : List.copyOf(radiiKm);
    offerTimeout = offerTimeout == null ? Duration.ofMinutes(1) : offerTimeout;
    retryInterval = retryInterval == null ? Duration.ofMinutes(10) : retryInterval;
    maxRetries = maxRetries == null ? 2 : maxRetries;
    maxActiveDeliveries = maxActiveDeliveries == null ? 3 : maxActiveDeliveries;
    rejectionNotice = rejectionNotice == null ? 5 : rejectionNotice;
    operatingStart = operatingStart == null ? "08:00" : operatingStart;
    operatingEnd = operatingEnd == null ? "24:00" : operatingEnd;
    locationMaxAge = locationMaxAge == null ? Duration.ofMinutes(2) : locationMaxAge;
    tickInterval = tickInterval == null ? Duration.ofSeconds(5) : tickInterval;
    lateWeight = lateWeight == null ? 2 : lateWeight;
    solverTimeLimit = solverTimeLimit == null ? Duration.ofSeconds(2) : solverTimeLimit;
    pollAfterSec = pollAfterSec == null ? 5 : pollAfterSec;
    idlePollAfterSec = idlePollAfterSec == null ? 30 : idlePollAfterSec;
    bundleMaxExtra = bundleMaxExtra == null ? Duration.ofMinutes(50) : bundleMaxExtra;
    ai = ai == null ? new Ai(null, null) : ai;
    delayAlertAfter = delayAlertAfter == null ? Duration.ofMinutes(20) : delayAlertAfter;
    deliveryPromise = deliveryPromise == null ? Duration.ofMinutes(50) : deliveryPromise;
    searchInterval = searchInterval == null ? Duration.ofSeconds(30) : searchInterval;
    quotaBackoff = quotaBackoff == null ? Duration.ofMinutes(5) : quotaBackoff;
    routeCacheTtl = routeCacheTtl == null ? Duration.ofMinutes(5) : routeCacheTtl;
    cancellationWindow = cancellationWindow == null ? Duration.ofDays(7) : cancellationWindow;
    cancellationPenalty = cancellationPenalty == null ? Duration.ofMinutes(3) : cancellationPenalty;
    listDelayPerCancellation =
        listDelayPerCancellation == null ? Duration.ofMinutes(1) : listDelayPerCancellation;
    bundleNeighbors = bundleNeighbors == null ? 4 : bundleNeighbors;
    failedReturnFallbackAfter =
        failedReturnFallbackAfter == null ? Duration.ofMinutes(5) : failedReturnFallbackAfter;
    urls = urls == null ? new Urls(null, null, null, null, null) : urls;
  }

  public OperatingHours hours() {
    return OperatingHours.parse(operatingStart, operatingEnd);
  }

  public double maxRadiusKm() {
    return radiiKm.get(radiiKm.size() - 1);
  }

  /**
   * Python AI 최적화 서버(ADR-0058 결정 9~13).
   *
   * @param baseUrl 예 http://delivery-dispatch-optimizer:8407 — 비우면 부르지 않고 OR-Tools 만
   * @param timeout 응답 기다림(사용자 결정 3초) — 넘으면 OR-Tools 로 대신
   */
  public record Ai(@Nullable String baseUrl, @Nullable Duration timeout) {
    public Ai {
      baseUrl = baseUrl == null || baseUrl.isBlank() ? null : baseUrl;
      timeout = timeout == null ? Duration.ofSeconds(3) : timeout;
    }
  }

  /** 서비스 간 호출 주소(LAN 은 Eureka 이름 그대로). */
  public record Urls(
      @Nullable String rider,
      @Nullable String tracking,
      @Nullable String order,
      @Nullable String centerMaster,
      @Nullable String serviceability) {
    public Urls {
      rider = or(rider, "http://rider-service:8403");
      tracking = or(tracking, "http://delivery-tracking-service:8406");
      order = or(order, "http://order-service:8201");
      centerMaster = or(centerMaster, "http://center-master-service:8507");
      serviceability = or(serviceability, "http://delivery-serviceability-service:8401");
    }

    private static String or(@Nullable String v, String d) {
      return v == null || v.isBlank() ? d : v;
    }
  }
}
