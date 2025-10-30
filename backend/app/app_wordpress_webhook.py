# app_wordpress_webhook.py
# WordPress(퍼널모아) → 커스텀 Webhook 수신
# 결제 완료 페이로드를 받아 "충전코드(1회용 토큰)"을 발급하고 이메일 안내까지 수행

import os, hmac, hashlib, json, smtplib, ssl, secrets, string
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

import httpx
from fastapi import APIRouter, Request, Header, HTTPException
from email.mime.text import MIMEText

router = APIRouter()

# ─────────────────────────────
# ENV
# ─────────────────────────────
WP_WEBHOOK_SECRET = os.getenv("WP_WEBHOOK_SECRET", "REPLACE_ME")  # WP에서 HMAC-SHA256으로 서명
SUPABASE_URL      = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_ROLE", "")        # Service Role 키

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@example.com")
REDEEM_PORTAL_URL = os.getenv("REDEEM_PORTAL_URL", "https://malfit.site/redeem")

# SKU/금액 → 포인트 매핑 (필요 시 여기만 조정)
PRODUCT_CREDITS = {
    "MALFIT-PT-5K": 50,
    "MALFIT-PT-10K": 110,
    "MALFIT-PT-50K": 600,
}
AMOUNT_CREDITS = {5000: 50, 10000: 110, 50000: 600}

# ─────────────────────────────
# Utils
# ─────────────────────────────
def verify_wp_signature(body: bytes, signature: Optional[str]) -> bool:
    """WP 측에서 HMAC-SHA256(hex)으로 보낸 X-WP-Signature 검증"""
    if not signature:
        return False
    mac = hmac.new(WP_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)

def new_claim_token() -> str:
    """ABCDE-12345-FGHIJ-67890 형식 토큰"""
    alphabet = string.ascii_uppercase + string.digits
    parts = ["".join(secrets.choice(alphabet) for _ in range(5)) for _ in range(4)]
    return "-".join(parts)

def send_email(to_email: str, subject: str, body: str):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS):
        print("[MAIL] SMTP not configured; skip sending.")
        return
    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = to_email
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls(context=ctx)
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(MAIL_FROM, [to_email], msg.as_string())

# ─────────────────────────────
# Supabase helpers
# ─────────────────────────────
async def sb_select_one(table: str, eq: Dict[str, Any]):
    params = {"select": "*"}
    for k, v in eq.items():
        params[k] = f"eq.{v}"
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            params=params,
        )
    if r.status_code != 200:
        raise HTTPException(500, f"Supabase select error: {r.text}")
    rows = r.json()
    return rows[0] if rows else None

async def sb_insert(table: str, row: Dict[str, Any]):
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
            json=row,
        )
    if r.status_code not in (200, 201):
        raise HTTPException(500, f"Supabase insert error: {r.text}")
    return r.json()[0] if r.json() else None

# ─────────────────────────────
# Endpoint: WP 커스텀 웹훅 → 충전코드 발급
# ─────────────────────────────
@router.post("/api/wordpress_payment_webhook")
async def wordpress_payment_webhook(
    request: Request,
    x_wp_signature: Optional[str] = Header(None),  # HMAC hex (우리가 안내한 헤더명)
):
    raw = await request.body()
    if not verify_wp_signature(raw, x_wp_signature):
        raise HTTPException(401, "Invalid WP signature")

    payload: Dict[str, Any] = json.loads(raw.decode("utf-8"))

    # 기대하는 페이로드 필드
    order_id     = str(payload.get("order_id") or "")
    user_email   = (payload.get("user_email") or "").strip()
    total_amount = int(payload.get("total_amount", 0))
    items        = payload.get("items", [])  # [{"sku":"MALFIT-PT-10K","qty":1}, ...]

    if not order_id:
        raise HTTPException(400, "order_id missing")

    # 포인트 계산 (SKU 우선 → 없으면 금액 매핑)
    added_total = 0
    for it in items or []:
        sku = (it.get("sku") or "").strip()
        qty = int(it.get("qty", 1))
        if sku in PRODUCT_CREDITS:
            added_total += PRODUCT_CREDITS[sku] * qty
    if added_total == 0 and total_amount in AMOUNT_CREDITS:
        added_total = AMOUNT_CREDITS[total_amount]
    if added_total <= 0:
        raise HTTPException(400, "Unknown product/amount mapping")

    # 멱등성: 같은 order_id면 기존 토큰 재사용
    exist = await sb_select_one("credit_claims", {"order_id": order_id})
    if exist:
        # 이미 발급됨 → 그대로 반환
        return {
            "ok": True,
            "order_id": order_id,
            "token": exist["token"],
            "credits": exist["credits"],
            "dup": True,
        }

    # 새 토큰 발급 + 저장
    token = new_claim_token()
    await sb_insert(
        "credit_claims",
        {
            "token": token,
            "order_id": order_id,
            "email": user_email,
            "credits": int(added_total),
            "redeemed": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    # 이메일 안내(설정되어 있으면)
    if user_email:
        subject = "[말핏] 포인트 충전 코드 안내"
        body = (
            f"안녕하세요!\n\n"
            f"아래 '충전 코드'를 말핏에서 입력하면 {added_total}P가 적립됩니다.\n\n"
            f"충전 코드: {token}\n"
            f"입력 페이지: {REDEEM_PORTAL_URL}\n"
            f"(유효기간: 90일)\n\n주문번호: {order_id}\n"
            f"감사합니다."
        )
        try:
            send_email(user_email, subject, body)
        except Exception as e:
            print(f"[MAIL] send fail: {e}")

    return {"ok": True, "order_id": order_id, "token": token, "credits": added_total}
