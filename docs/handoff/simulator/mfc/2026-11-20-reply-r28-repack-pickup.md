# [pmart-backend → pmart-mfc-simulation] 회신: R-28 재포장 뒤 픽업 완료 안 됨 (2026-11-20)

- 보낸 영역: delivery(PC 5)
- 답하는 것: `pmart-mfc-simulation/docs/backend-requests.md` R-28(재포장 뒤 배차·배송 수행이 무효 포장을 기다림 — 주문 23631583, DLV-eb09780b)
- 상태: **PR 올림(PC 1 검토·병합·LAN 배포 전)** — https://github.com/code-and-pick/pmart-backend/pull/293 (execution V25, dispatch V16). 계약 변경 없음.

## 원인 — 추정 그대로 맞음
- mfc-fulfillment 은 재포장·재구성 때 `PackageInvalidated` 만 낸다(새 목록 사건 없음). delivery-dispatch·delivery-execution 모두 이 사건을 구독하지 않아(events.md 에는 구독자로 적혀 있었음) 포장 목록에 옛 포장이 남고, "모든 포장 인계" 판정이 끝내 거짓 → 배송 ARRIVED_AT_STORE·배차 ASSIGNED, 배차 취소 422 PICKED_UP_CASE_REQUIRED.

## 1. 바뀐 목록 알리기 — 배송 쪽이 PackageInvalidated 를 구독(fulfillment 변경 없음)
- 두 서비스 모두 `invalidated_package`(옛 포장 → 새 포장들)에 남기고, 포장 목록을 새 포장으로 바꿔 픽업을 다시 판정한다. 연쇄 재포장(A→B→C)도 끝까지, 무효 사건이 ShipmentReady 보다 먼저 와도 된다.
- 결과: 배송 조회 `packageIds` 에서 무효 포장이 빠지고 새 포장이 들어간다(라이더 앱 QR 검사도 새 포장 기준). 배차 상세 `packages` 도 새 포장(봉지 수·무게·주류는 옛 포장 값을 물려받음).
- fulfillment 가 새 목록 사건을 따로 낼 필요는 없다.

## 2. 이미 멈춘 배송
- 배포 전에 지나간 무효 사건은 다시 오지 않아 저절로 풀리지 않는다. 운영자 API·백필은 만들지 않았다.
- DLV-eb09780b 는 사용자 승인으로 PC 1 에 요청: 배포 뒤 이 출고의 무효 3건을 새 eventId 로 다시 발행 → 두 서비스가 다시 판정해 PICKED_UP + PickupCompleted. 실물·재고 인계는 그대로(다시 피킹·재인계 없음).
- 다른 멈춘 배송이 있으면 같은 방법(무효 사건 재발행)으로 — 주문·배송 ID 를 알려 주면 PC 1 에 같이 요청한다.
