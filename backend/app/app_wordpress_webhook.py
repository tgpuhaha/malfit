# app_wordpress_webhook.py (기존 FastAPI에 추가)
import os, hmac, hashlib, json
from fastapi import Request, Header, HTTPException
from typing import Dict, Any, Optional

WP_WEBHOOK_SECRET = os.getenv("WP_WEBHOOK_SECRET", "REPLACE_ME")

def verify_wp_signature(body: bytes, signature: str) -> bool:
    mac = hmac.new(WP_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature or "")

@app.post("/api/wordpress_payment_webhook")
async def wordpress_payment_webhook(
    request: Request,
    x_wp_signature: Optional[str] = Header(None),
):
    raw = await request.body()
    if not verify_wp_signature(raw, x_wp_signature or ""):
        raise HTTPException(401, "Invalid WP signature")

    payload: Dict[str, Any] = json.loads(raw.decode("utf-8"))

    # 필수 필드
    user_email   = payload.get("user_email")      # WP 계정 이메일
    user_id      = payload.get("user_id")         # (선택) WP user ID
    total_amount = int(payload.get("total_amount", 0))
    items        = payload.get("items", [])       # [{"sku":"MALFIT-PT-10K","qty":1}, ...]

    if not user_email and not user_id:
        raise HTTPException(400, "user identifier missing")

    # 당신의 사용자 매핑 로직 (이메일=Malfit 유저ID로 쓰거나, 별도 매핑테이블)
    # 여기선 이메일을 Malfit의 user_id로 간주
    malfit_user_id = user_email

    # 우선순위: SKU 매핑 → 없으면 금액 매핑
    added_total = 0
    for it in items:
        sku = it.get("sku")
        qty = int(it.get("qty", 1))
        if sku in PRODUCT_CREDITS:
            added_total += PRODUCT_CREDITS[sku] * qty

    if added_total == 0 and total_amount in AMOUNT_CREDITS:
        added_total = AMOUNT_CREDITS[total_amount]

    if added_total <= 0:
        raise HTTPException(400, "Unknown product/amount mapping")

    # 중복 방지 (order_id를 이벤트 키로 기록)
    event_id = f"wp-{payload.get('order_id')}"
    new = await sb_record_event("wordpress", event_id, "completed", payload)
    if not new:
        return {"ok": True, "skipped": "duplicate"}

    await sb_add_credits(malfit_user_id, added_total)
    return {"ok": True, "user": malfit_user_id, "added": added_total}
