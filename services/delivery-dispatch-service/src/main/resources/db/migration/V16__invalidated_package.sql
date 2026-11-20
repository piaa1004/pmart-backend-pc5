-- 2026-11-20 MFC 시뮬레이터 R-28·사용자 보고(주문 23631583): 재포장으로 무효된 포장(PackageInvalidated)을 처리하지 않아 배송 포장 목록에
-- 옛 포장이 남아 새 포장을 인계해도 PICKED_UP 이 되지 않았다. 무효 기록을 두고 포장 목록을 새 포장으로 바꾼다(사건이 목록보다 먼저 와도 된다).
CREATE TABLE invalidated_package (
    package_id      VARCHAR(128) PRIMARY KEY,
    shipment_id     VARCHAR(128),
    new_package_ids jsonb        NOT NULL DEFAULT '[]'::jsonb,
    recorded_at     TIMESTAMPTZ  NOT NULL
);
