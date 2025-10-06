# app/main.py
import os, time, uuid
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic_settings import BaseSettings
import redis

# ───────────── 설정
class Settings(BaseSettings):
    OPENAI_API_KEY: str
    REDIS_URL: str = "redis://localhost:6379/0"
    ROOT_WORKDIR: str = "/tmp/malfit"
    MAX_MB_FREE: int = 200
    ALLOW_ORIGIN: str = "*"

S = Settings()
ROOT = Path(S.ROOT_WORKDIR)
UP   = ROOT / "uploads"
OUT  = ROOT / "out"
UP.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

r = redis.from_url(S.REDIS_URL, decode_responses=True)

app = FastAPI(title="malfit-api", version="1.0.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if S.ALLOW_ORIGIN == "*" else [S.ALLOW_ORIGIN],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

def k(jid: str) -> str: return f"job:{jid}"

# ───────────── 라우트
@app.get("/")
def root(): return {"ok": True, "msg": "malfit api"}

@app.post("/upload")
async def upload(file: UploadFile = File(...), language: str = Form("ko")):
    if not file.filename:
        raise HTTPException(400, "파일명이 없습니다.")
    if not (file.filename.lower().endswith((".mp4",".mov",".mkv",".m4v",".webm"))):
        raise HTTPException(400, "영상 파일만 허용합니다.")

    jid = str(uuid.uuid4())
    dest = UP / f"{jid}.mp4"

    # 스트리밍 저장 + 용량 가드
    size = 0
    with dest.open("wb") as f:
        while True:
            chunk = await file.read(1024*1024)
            if not chunk: break
            size += len(chunk)
            if size > S.MAX_MB_FREE * 1024 * 1024:
                f.close(); dest.unlink(missing_ok=True)
                raise HTTPException(413, f"업로드 제한 {S.MAX_MB_FREE}MB 초과")
            f.write(chunk)

    r.hmset(k(jid), {
        "status":"queued", "progress":"0", "video": str(dest),
        "language": language, "createdAt": str(int(time.time()))
    })
    r.lpush("queue:shorts", jid)
    return {"jobId": jid}

@app.get("/jobs/{jid}")
def job_status(jid: str):
    d = r.hgetall(k(jid))
    if not d: raise HTTPException(404, "not found")
    return d

@app.get("/download/{jid}/{kind}")
def download(jid: str, kind: str):
    base = OUT / jid
    path = {
        "srt": base.with_suffix(".srt"),
        "rewritten": base.with_name(f"{jid}_rewritten").with_suffix(".srt"),
        "vo": base.with_name(f"{jid}_vo").with_suffix(".txt")
    }.get(kind)
    if not path or not path.exists():
        raise HTTPException(404, "file not ready")
    media = "text/plain" if path.suffix==".txt" else "application/x-subrip"
    return FileResponse(str(path), media_type=media, filename=path.name)
