# payments_claim.py
# WooCommerce webhook -> 충전코드 발급
# /redeem_token -> 개별 적립
# /claim_token  -> 주문번호로 토큰 조회(WP 완료페이지/메일용)
# /my_claims    -> 말핏 마이페이지: 내 미사용 코드 목록
# /redeem_my_claims -> 말핏 마이페이지: 내 미사용 코드 일괄 적립

import os, json, hmac, hashlib, base64, smtplib, ssl, secrets, string
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

import httpx
from fastapi import APIRouter, Request, Header, HTTPException, Depends
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

async def sb_select(table: str, where: Dict[str, Any], select: str = "*", order: Optional[str] = None):
    params = {"select": select}
    for k, v in where.items():
        params[k] = f"eq.{v}"
    if order:
        params["order"] = order
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{table}",
                        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
                        params=params)
    if r.status_code != 200:
        raise HTTPException(500, f"Supabase select error: {r.text}")
    return r.json()

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

def mask_token(t: str) -> str:
    return f"{t[:5]}-*****-*****-{t[-5:]}" if t and len(t) > 10 else t

# ── 인증 헬퍼 (프로젝트 로그인 방식에 맞게 조정 가능) ─────────────────────
def get_current_user(request: Request):
    # 우선 request.state.* (미들웨어에서 세팅했다고 가정)
    uid = getattr(request.state, "user_id", None)
    email = getattr(request.state, "user_email", None)
    # 테스트/임시: 헤더로도 허용
    if not uid:
        uid = request.headers.get("X-User-Id")
    if not email:
        email = request.headers.get("X-User-Email")
    if not uid or not email:
        raise HTTPException(401, "로그인이 필요합니다")
    return {"id": str(uid), "email": email}

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
    current_user_id = getattr(request.state, "user_id", None) or body.get("user_id") \
                      or request.headers.get("X-User-Id")
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

# ── 3) 주문번호로 토큰 조회 (WP 완료페이지/메일에서 사용) ────────────────
@router.get("/api/claim_token")
async def claim_token(order_id: str):
    claim = await sb_select_one("credit_claims", {"order_id": order_id})
    if not claim:
        raise HTTPException(404, "not_ready")
    return {"token": claim["token"], "credits": int(claim["credits"])}

# ── 4) 말핏 마이페이지: 내 미사용 코드 목록 ───────────────────────────────
@router.get("/api/my_claims")
async def my_claims(user=Depends(get_current_user)):
    rows = await sb_select(
        "credit_claims",
        {"email": user["email"], "redeemed": False},
        select="order_id,token,credits,created_at,redeemed",
        order="created_at.desc"
    )
    return [{
        "order_id": r["order_id"],
        "token_masked": mask_token(r["token"]),
        "credits": int(r["credits"]),
        "created_at": r["created_at"],
    } for r in rows]

# ── 5) 말핏 마이페이지: 내 미사용 코드 일괄 적립 ──────────────────────────
@router.post("/api/redeem_my_claims")
async def redeem_my_claims(user=Depends(get_current_user)):
    rows = await sb_select(
        "credit_claims",
        {"email": user["email"], "redeemed": False},
        select="token,credits"
    )
    if not rows:
        return {"ok": True, "added": 0, "count": 0}

    total_add = sum(int(r["credits"]) for r in rows)
    new_total = await sb_add_credits(user["id"], total_add)

    for r in rows:
        await sb_update("credit_claims", {"token": r["token"]}, {
            "redeemed": True,
            "redeemed_at": datetime.now(timezone.utc).isoformat(),
            "redeemed_user_id": user["id"]
        })

    return {"ok": True, "added": total_add, "count": len(rows), "new_total": new_total}
