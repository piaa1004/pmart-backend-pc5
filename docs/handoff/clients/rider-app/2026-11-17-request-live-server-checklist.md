# 요청 — 라이더 앱 실서버 전환 체크리스트(한 장)

보낸 영역: delivery
- 작성: pmart-backend PC 5(delivery), 2026-11-17 — 사용자 "실제 주문이면 완벽하게 돌아가는지, 빠진 것" 점검 결과
- 대상: pmart-clients `apps/rider_app`(브랜치 `rider-app/reference-ui` 기준으로 읽음)
- 상태: 서버는 아래 "배포" 표시까지 LAN 에 있다. 2026-11-17 저녁 앱 작업 사본(미커밋)을 다시 읽어 보니 **로그인·번호 확인·은행 코드·약관 버전(`termsVersion 2026-11-17`)·운행시간·탈퇴·전달 재개는 이미 실서버로 붙이는 중**이다. 남은 것은 아래 ⬜ 표시 — 특히 **회수 배송 단계**(받음 → MFC 반납 — 반품·오배송 공통 한 벌)가 앱에 없다.

## 1. 로그인·가입(이것이 없으면 아무 것도 못 한다)
| 할 것 | 서버(상태) | 근거 |
|---|---|---|
| Google 로그인 → 라이더 토큰 | rider-auth `POST /rider-auth/social-logins`·토큰 갱신(배포) | `session.dart` 가 실서버면 `notConnected` |
| 지점 `deliveryAreas[].mfcId` = `GET /mfc-pickup-points` 의 centerId | 배포 | §2 |
| 수단 E_BIKE·KICKBOARD, 면허 `licenseNumber`·`POST /riders/license-checks` | 배포 | §6·§7 |
| 번호 확인 `POST /riders/phone-checks`, 계좌 확인 `POST /riders/account-checks` — **Idempotency-Key·X-Source-System 헤더 필요** | PR #125 | §14·§15 |
| **은행 = 금융결제원 코드**(`GET /riders/bank-codes`) — 이름("국민은행")을 보내면 400 | PR #125 | §16-1 |
| 거주지 = 우편번호 서비스 도로명주소(서버가 확인, 없으면 422 INVALID_ADDRESS) | PR #125 | §12 |
| 약관 `consents[].version` — 앱이 `2026-11-17` 을 보냄(작업 사본 확인). 약관 5종 문구 개정 때 이 값을 올리면 서버에 어느 판에 동의했는지 남는다 | 배포 | §16-2 |
| 탈퇴 `POST /riders/me/withdrawals` — 409 이면 `details[].reason`(WithdrawalBlocker enum)별 안내, 202 면 로그아웃, 이후 401 은 로그인 화면 | 배포(LAN 확인 21/21) | §17 |

## 2. 운행·배차
| 할 것 | 서버(상태) |
|---|---|
| 운행 시작·신규배차 `PUT /riders/me/state`, 위치 업로드 `POST /riders/me/locations`(2분 안 보내면 배차 후보에서 빠짐) | 배포 |
| 제안 `GET /offers` 의 `summary.order`(지점·품목·금액·요청사항)·`pickupPoint`·`dropAreaPoint`·`dropEtaSeconds` — 목 대신 | 배포 |
| 회수 제안 구분: `deliveryType RETURN` + `purpose` MISDELIVERY_RECOVERY(오배송 회수)·**CLAIM_RETURN(고객 반품 회수)** | 배포 |

## 3. 배송 수행
| 할 것 | 서버(상태) |
|---|---|
| 배송 조회 `orderSummary`(orderNumber 포함)·`orderNumber`·`mfcName` | 배포 |
| 전달지 `drop-info` — 일반 배송, 픽업 뒤(회수 배송은 `recovery-contact`) | 배포 |
| 오배송 회수의 원래 완료 사진(`recoveryOrigin`) | 배포 |
| ⬜ **회수 배송 단계 한 벌**(앱에 없음 — 반품 회수·오배송 회수 공통, purpose 로 문구만): 회수지 `recovery-contact`(주소는 있을 때만) → 받음 `returns/pickups`(사진) → 반납 `returns/store-deliveries` — `2026-11-17-request-claim-return-pickup.md` | 배포(연락처 통합은 PR #137) |
| **전달 불가**: `POST …/failures` 한 번이면 대화도 열림(응답 `resultId`) — **도움요청을 따로 보내지 않기**. 재개 `POST …/failures/resume-requests` | PR #127 |
| 연락 기록 method 에 `PHONE_CALL`·`SMS`(일반 전화) | 배포 |

## 4. 지원센터·알림·기록
| 할 것 | 서버(상태) |
|---|---|
| 도움요청 대화 `…/help-requests/{id}/messages`, 배송별 목록 `GET /deliveries/{id}/help-requests` | 배포 |
| 배송 밖 문의 `POST/GET /inquiries`(SETTLEMENT 등) | 배포 |
| 알림함 `GET /me/notifications`(지원센터 답장·처리 끝·문의 답변·계정 제한·**수행료 조정**) | 배포 |
| 운행시간 `GET /riders/me/operating-time`(오늘·주), 수행료 `GET /riders/me/earnings`, 내 배송 `GET /riders/me/deliveries` | 배포 |

## 회신 받을 것
- 위 항목 중 실서버로 바꾼 것·막힌 것. 앱이 다 바꾸면 PC 5 가 실제 주문 한 건으로 처음부터 끝까지(가입 → 배차 → 픽업 → 전달·실패 → 반품 회수) 같이 확인한다.
