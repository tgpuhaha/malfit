# payments_claim.py
# WooCommerce webhook -> 충전코드 발급, /redeem_token -> 포인트 적립

import os, json, hmac, hashlib, base64, smtplib, ssl, secrets, string
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

import httpx
from fastapi import APIRouter, Request, Header, HTTPException
from email.mime.text import MIMEText

router = APIRouter()

# ── ENV ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE")  # Service Role 키
WC_WEBHOOK_SECRET = os.getenv("WC_WEBHOOK_SECRET", "REPLACE_ME")

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
MAIL_FROM  = os.getenv("MAIL_FROM", "no-reply@example.com")
REDEEM_PORTAL_URL = os.getenv("REDEEM_PORTAL_URL", "https://malfit.site/redeem")

# SKU/금액 → 포인트 매핑(필요시 조정)
PRODUCT_CREDITS = {"MALFIT-PT-5K":50, "MALFIT-PT-10K":110, "MALFIT-PT-50K":600}
AMOUNT_CREDITS  = {5000:50, 10000:110, 50000:600}

# ── Supabase helpers ─────────────────────────────────────────────────────
async def sb_select_one(table: str, eq: Dict[str, Any]):
    params = {"select": "*"}
    for k, v in eq.items():
        params[k] = f"eq.{v}"
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{table}",
                        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
                        params=params)
    if r.status_code != 200:
        raise HTTPException(500, f"Supabase select error: {r.text}")
    rows = r.json()
    return rows[0] if rows else None

async def sb_insert(table: str, row: Dict[str, Any]):
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{table}",
                         headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                                  "Content-Type": "application/json", "Prefer": "return=representation"},
                         json=row)
    if r.status_code not in (200, 201):
        raise HTTPException(500, f"Supabase insert error: {r.text}")
    return r.json()[0] if r.json() else None

async def sb_update(table: str, eq: Dict[str, Any], patch: Dict[str, Any]):
    params = {k: f"eq.{v}" for k, v in eq.items()}
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{table}",
                          headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                                   "Content-Type": "application/json"},
                          params=params, json=patch)
    if r.status_code not in (200, 204):
        raise HTTPException(500, f"Supabase update error: {r.text}")

async def sb_add_credits(user_id: str, add: int):
    """users 테이블에 credits 필드가 있다고 가정한 read-modify-write 증가."""
    user = await sb_select_one("users", {"id": user_id})
    if not user:
        raise HTTPException(404, "User not found in users table")
    new_credits = int(user.get("credits", 0)) + int(add)
    await sb_update("users", {"id": user_id}, {"credits": new_credits})
    return new_credits

# ── utils ────────────────────────────────────────────────────────────────
def verify_wc_signature(raw: bytes, sig: Optional[str]) -> bool:
    if not sig:
        return False
    mac = hmac.new(WC_WEBHOOK_SECRET.encode(), raw, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode()
    return expected.strip() == sig.strip()

def new_claim_token() -> str:
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

# ── 1) WooCommerce Webhook → 충전코드 발급 ───────────────────────────────
@router.post("/api/woocommerce_webhook")
async def woocommerce_webhook(request: Request,
                              x_wc_webhook_signature: Optional[str] = Header(None)):
    raw = await request.body()
    if not verify_wc_signature(raw, x_wc_webhook_signature):
        raise HTTPException(401, "Invalid WC signature")

    payload = json.loads(raw.decode("utf-8"))
    order   = payload.get("order") or payload
    status  = (order.get("status") or "").lower()
    if status != "completed":
        return {"ok": True, "ignored_status": status}

    order_id     = str(order.get("id") or order.get("number") or "unknown")
    total_amount = int(float(order.get("total") or 0))
    billing      = order.get("billing") or {}
    user_email   = billing.get("email") or order.get("billing_email")

    # SKU/수량으로 포인트 계산
    added = 0
    for li in order.get("line_items", []):
        sku = li.get("sku") or (li.get("product") or {}).get("sku")
        qty = int(li.get("quantity") or 1)
        if sku in PRODUCT_CREDITS:
            added += PRODUCT_CREDITS[sku] * qty
    if added == 0 and total_amount in AMOUNT_CREDITS:
        added = AMOUNT_CREDITS[total_amount]
    if added <= 0:
        raise HTTPException(400, "Unknown product/amount mapping")

    # 멱등성: 주문번호로 기존 토큰 재사용
    exists = await sb_select_one("credit_claims", {"order_id": order_id})
    if exists:
        return {"ok": True, "token": exists["token"], "credits": exists["credits"], "dup": True}

    token = new_claim_token()
    await sb_insert("credit_claims", {
        "token": token,
        "order_id": order_id,
        "email": user_email,
        "credits": added,
        "redeemed": False,
        "created_at": datetime.now(timezone.utc).isoformat()
    })

    if user_email:
        send_email(
            user_email,
            "[말핏] 포인트 충전 코드",
            (
                f"안녕하세요!\n\n"
                f"아래 '충전 코드'를 말핏에서 입력하면 {added}P가 적립됩니다.\n\n"
                f"충전 코드: {token}\n"
                f"입력 페이지: {REDEEM_PORTAL_URL}\n"
                f"(유효기간: 90일)\n\n주문번호: {order_id}"
            ),
        )

    return {"ok": True, "order_id": order_id, "token": token, "credits": added}

# ── 2) 리딤: 로그인 후 코드 입력 → 포인트 적립 ───────────────────────────
@router.post("/api/redeem_token")
async def redeem_token(request: Request, body: Dict[str, Any]):
    # 프로젝트의 인증 방식에 맞게 현재 로그인 user_id/email을 꺼내세요.
    # 예: request.state.user_id 또는 body["user_id"] 등
    current_user_id = getattr(request.state, "user_id", None) or body.get("user_id")
    if not current_user_id:
        raise HTTPException(401, "로그인이 필요합니다")

    token = (body.get("token") or "").strip().upper()
    if not token:
        raise HTTPException(400, "토큰이 필요합니다")

    claim = await sb_select_one("credit_claims", {"token": token})
    if not claim:
        raise HTTPException(404, "존재하지 않는 코드입니다")
    if claim.get("redeemed"):
        raise HTTPException(400, "이미 사용된 코드입니다")

    created = datetime.fromisoformat(claim["created_at"].replace("Z","")).replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - created > timedelta(days=90):
        raise HTTPException(400, "코드 유효기간이 지났습니다")

    add = int(claim["credits"])
    new_total = await sb_add_credits(str(current_user_id), add)

    await sb_update("credit_claims", {"token": token}, {
        "redeemed": True,
        "redeemed_at": datetime.now(timezone.utc).isoformat(),
        "redeemed_user_id": str(current_user_id)
    })

    return {"ok": True, "added": add, "new_total": new_total}
