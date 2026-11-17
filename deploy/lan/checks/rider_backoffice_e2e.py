"""LAN 확인 — 라이더 백오피스(web/rider-backoffice)가 부르는 직원용 API(2026-11-17~18, 실제 API·게이트웨이). 토큰·주소·전화는 출력하지 않는다.

배포된 것만 본다 — 아직 배포 전인 기능은 HTTP 404·501 로 "배포 전"이라 적고 넘어간다(실패로 세지 않는다).
직원: claude-dev(OPERATOR_ADMIN — 허용), claude-mfc-sup(MFC_SUPERVISOR 자기 센터 — 라이더 운영 화면은 403). 계정 역할은 바꾸지 않는다.

  A. 대시보드(#155)          GET /riders/stats·/delivery-stats(관리자 200, 센터 관리자 라이더 수 403·자기 센터 배송 수 200)
  B. 시험 라이더             가입(자전거·실제 주소·은행 코드) → 운행 시작(신규배차 켬) → 위치 올리기
  C. 관제(#149)              GET /rider-locations?mfcId= — 시험 라이더가 운행 중·위치 있음·끊김 아님, 센터 관리자 403
  D. 라이더 찾기·상세(#155·#156) GET /riders?riderId=(가입 승인 APPROVED), 상세 signupDecidedBy AUTO·정산계좌 등록 여부·은행 이름·동의
  E. 라이더 기록(#149·#157)   직원용 수행료·운행시간(새 라이더 0), 배정 이력(새 라이더 빈 목록)
  F. 계정 제한(RS-1)         관리자 신규배차 제한(행 버전) → 관제에 제한 표시 → 풀기
  G. 배송·수행료(#149·#152)   주문번호 찾기(없는 번호 빈 목록), 추가 배달료 대기·조정 대기 200, 직원 근거 사진(#157)·상태 정정(#150) 없는 배송 404
  H. 정리                    운행 종료 → 탈퇴(배송이 없어 바로 COMPLETED) — LAN 에 시험 라이더를 남기지 않는다

실행: uv run --no-project --with cryptography python -I deploy/lan/checks/rider_backoffice_e2e.py [--sup]
  --sup: 센터 관리자(claude-mfc-sup) 403 확인까지 — PC 1 이 준 최신 비밀번호 파일이 있을 때만(옛 값이면 계정이 잠겨 다른 PC 도 막힘)
필요: rider_e2e.py 와 같다(라이더 서명키·claude-dev 비밀번호) + keycloak-claude-mfc-sup-password.
2026-11-18 처음 작성(PC 5 — 라이더 백오피스 백엔드 #149~#158 확인용).
"""
import base64
import os
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from rider_e2e import MFC, MFC_LAT, MFC_LNG, check, http, phone, rider_token, summary  # noqa: E402
from staff_auth import staff_token  # noqa: E402

MFC_ADDRESS = os.environ.get("PMART_MFC_ADDRESS", "부산광역시 부산진구 동천로 109")  # 시험 MFC 실제 주소(2026-11-16)
PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")


def not_deployed(name, status):
    if status in (404, 501):
        print(f"  --   {name} — 배포 전(HTTP {status})")
        return True
    return False


def first(items, key, value):
    return next((i for i in items if i.get(key) == value), None)


def main():
    admin = staff_token("claude-dev")
    admin = admin[0] if isinstance(admin, tuple) else admin
    # 센터 관리자 시험 계정은 --sup 일 때만(2026-11-18 PC 1 요청): 옛 비밀번호로 5번 실패하면 Keycloak 이 계정을 잠가
    # 다른 PC 도 막히고, 비밀번호 파일이 없으면 staff_auth 가 비밀번호를 새로 바꿔 모든 PC 파일이 무효가 된다.
    sup = None
    if "--sup" in sys.argv:
        if not os.path.exists(os.path.join(HERE, "..", ".secrets", "keycloak-claude-mfc-sup-password")):
            sys.exit("--sup: keycloak-claude-mfc-sup-password 파일이 없음 — PC 1 에서 받은 뒤 다시(파일 없이 돌리면 비밀번호가 바뀜)")
        sup = staff_token("claude-mfc-sup", ["MFC_SUPERVISOR"], [MFC])
        sup = sup[0] if isinstance(sup, tuple) else sup
    else:
        print("  --   센터 관리자 403 확인 건너뜀(--sup 로 켬 — PC 1 에서 받은 비밀번호 파일이 있을 때만)")

    # A. 대시보드
    s, st = http("GET", "/riders/stats", admin)
    check("A 라이더 수(관리자)", s == 200 and {"total", "onDuty", "byRestriction", "joinedToday"} <= set(st), f"HTTP {s}")
    before = st.get("joinedToday", 0) if s == 200 else 0
    s, ds = http("GET", "/delivery-stats", admin)
    check("A 배송 수(관리자)", s == 200 and "UNASSIGNED" in ds.get("inProgress", {}), f"HTTP {s}")
    if sup:
        s, _ = http("GET", "/riders/stats", sup)
        check("A 센터 관리자 — 라이더 수 403", s == 403, f"HTTP {s}")
    if sup:
        s, _ = http("GET", f"/delivery-stats?mfcId={MFC}", sup)
        check("A 센터 관리자 — 자기 센터 배송 수 200", s == 200, f"HTTP {s}")

    # B. 시험 라이더
    rider = "RD-claude-bo-" + uuid.uuid4().hex[:10]
    token = rider_token(rider)
    acct = str(10_000_000_000 + uuid.uuid4().int % 89_999_999_999)
    s, b = http("POST", "/riders/signups", token, {
        "name": "시험라이더", "phone": phone(), "vehicleType": "BICYCLE", "deliveryAreas": [{"mfcId": MFC}],
        "residence": {"roadAddress": MFC_ADDRESS, "detail": "101호"},
        "consents": [{"consentType": "TERMS", "version": "2026-11-17"}, {"consentType": "PRIVACY", "version": "2026-11-17"}],
        "bankAccount": {"bankCode": "004", "accountNumber": acct, "holderName": "시험라이더"}})
    check("B 시험 라이더 가입", s == 201, f"HTTP {s} {b.get('code', '')}")
    if s != 201:
        summary()
        return
    s, me = http("GET", "/riders/me/state", token)
    s, b = http("PUT", "/riders/me/state", token, {"status": "ON_DUTY", "newDispatchEnabled": True, "mfcId": MFC},
                version=me.get("version"))
    check("B 운행 시작(신규배차 켬)", s == 200, f"HTTP {s} {b.get('code', '')}")
    s, _ = http("POST", "/riders/me/locations", token, {"deviceId": "claude-bo", "points": [
        {"lat": MFC_LAT + 0.001, "lng": MFC_LNG + 0.001, "accuracyM": 10,
         "recordedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}]})
    check("B 위치 올리기", s in (200, 202), f"HTTP {s}")

    # C. 관제
    s, board = http("GET", f"/rider-locations?mfcId={MFC}", admin)
    row = first(board.get("items", []), "riderId", rider) if s == 200 else None
    check("C 관제 — 운행 중 시험 라이더·위치 있음·끊김 아님",
          row is not None and row.get("location") is not None and row.get("stale") is False,
          f"HTTP {s} {len(board.get('items', []))}명")
    if row:
        check("C 관제 — 가린 이름만", "*" in (row.get("nameMasked") or ""), "")
    if sup:
        s, _ = http("GET", f"/rider-locations?mfcId={MFC}", sup)
        check("C 센터 관리자 — 관제 403", s == 403, f"HTTP {s}")

    # D. 라이더 찾기·상세
    s, found = http("GET", f"/riders?riderId={rider}", admin)
    item = (found.get("items") or [{}])[0] if s == 200 else {}
    check("D 라이더 찾기 — 가입 승인 APPROVED", item.get("signupStatus") == "APPROVED", f"HTTP {s}")
    s, d = http("GET", f"/riders/{rider}", admin)
    check("D 상세 — 자동 승인(AUTO)", s == 200 and d.get("signupDecidedBy") == "AUTO", f"HTTP {s}")
    if "bankAccountRegistered" in d:
        check("D 상세 — 정산계좌 등록됨·은행 이름(계좌번호 없음)",
              d.get("bankAccountRegistered") is True and bool(d.get("bankName")) and "bankAccountNumber" not in d, "")
        check("D 상세 — 동의 2건(항목별 마지막)", len(d.get("consents") or []) == 2, f"{len(d.get('consents') or [])}건")
    else:
        print("  --   D 정산계좌·동의 — 배포 전(#156)")
    s, st = http("GET", "/riders/stats", admin)
    check("D 오늘 가입 늘어남", s == 200 and st.get("joinedToday", 0) >= before + 1, f"{before} → {st.get('joinedToday')}")

    # E. 라이더 기록
    s, e = http("GET", f"/rider-earnings?riderId={rider}", admin)
    check("E 직원용 수행료 — 새 라이더 빈 목록", s == 200 and e.get("items") == [], f"HTTP {s}")
    s, t = http("GET", f"/rider-operating-time?riderId={rider}", admin)
    check("E 직원용 운행시간 — 0초", s == 200 and t.get("totalSeconds") == 0, f"HTTP {s}")
    s, h = http("GET", f"/rider-assignments?riderId={rider}", admin)
    if not not_deployed("E 배정 이력(#157)", s):
        check("E 배정 이력 — 새 라이더 빈 목록", s == 200 and h.get("items") == [], f"HTTP {s}")
        if sup:
            s, _ = http("GET", f"/rider-assignments?riderId={rider}", sup)
            check("E 센터 관리자 — 배정 이력 403", s == 403, f"HTTP {s}")
    if sup:
        s, _ = http("GET", f"/rider-earnings?riderId={rider}", sup)
        check("E 센터 관리자 — 직원용 수행료 403", s == 403, f"HTTP {s}")

    # F. 계정 제한 → 관제 표시 → 풀기
    s, hist = http("GET", f"/riders/{rider}/restrictions", admin)
    s, b = http("POST", f"/riders/{rider}/restrictions", admin,
                {"restriction": "DISPATCH_RESTRICTED", "reason": "LAN 시험(자동)"}, version=hist.get("version"))
    check("F 신규배차 제한(행 버전)", s == 200, f"HTTP {s} {b.get('code', '')}")
    s, board = http("GET", f"/rider-locations?mfcId={MFC}", admin)
    row = first(board.get("items", []), "riderId", rider) if s == 200 else None
    check("F 관제에 제한 표시", row is not None and row.get("restriction") == "DISPATCH_RESTRICTED", f"{row and row.get('restriction')}")
    s, hist = http("GET", f"/riders/{rider}/restrictions", admin)
    s, b = http("POST", f"/riders/{rider}/restrictions", admin,
                {"restriction": "NORMAL", "reason": "LAN 시험 풀기"}, version=hist.get("version"))
    check("F 제한 풀기", s == 200, f"HTTP {s} {b.get('code', '')}")

    # G. 배송·수행료
    s, r = http("GET", "/deliveries?orderNumber=00000000", admin)
    check("G 주문번호 찾기 — 없는 번호 빈 목록", s == 200 and r.get("items") == [], f"HTTP {s}")
    s, _ = http("GET", "/surcharge-requests", admin)
    check("G 추가 배달료 대기 200", s == 200, f"HTTP {s}")
    s, _ = http("GET", "/rider-earnings/adjustment-queue", admin)
    check("G 수행료 조정 대기 200", s == 200, f"HTTP {s}")
    boundary = "----claude" + uuid.uuid4().hex
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"e.png\"\r\n"
            f"Content-Type: image/png\r\n\r\n").encode() + PNG + f"\r\n--{boundary}--\r\n".encode()
    s, b = http("POST", "/deliveries/DLV-none-claude/staff-evidence-photos", admin, raw=body,
                content_type="multipart/form-data; boundary=" + boundary)
    if s == 404 and b.get("code") in ("DELIVERY_NOT_FOUND", "NOT_FOUND") and b.get("code") != "ROUTE_NOT_FOUND":
        check("G 직원 근거 사진 — 없는 배송 404", True, b.get("code", ""))
    else:
        not_deployed("G 직원 근거 사진(#157)", s) or check("G 직원 근거 사진 — 없는 배송 404", s == 404, f"HTTP {s} {b.get('code', '')}")
    s, b = http("POST", "/deliveries/DLV-none-claude/status-corrections", admin,
                {"targetState": "PICKED_UP", "reason": "LAN 시험"})
    check("G 상태 정정 — 없는 배송 404", s == 404, f"HTTP {s} {b.get('code', '')}")

    # H. 정리
    s, me = http("GET", "/riders/me/state", token)
    s, b = http("PUT", "/riders/me/state", token, {"status": "OFF_DUTY", "newDispatchEnabled": False}, version=me.get("version"))
    check("H 운행 종료", s == 200, f"HTTP {s}")
    s, b = http("POST", "/riders/me/withdrawals", token, {"reason": "LAN 시험 라이더 정리(자동)"})
    check("H 탈퇴(배송 없음 — 바로 정리)", s == 202, f"HTTP {s} {b.get('code', '')}")
    summary()


if __name__ == "__main__":
    main()
