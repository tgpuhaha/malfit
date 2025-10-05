# app/main.py
import os, uuid, time
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic_settings import BaseSettings
import redis

# ── 설정 ─────────────────────────────────
class Settings(BaseSettings):
    OPENAI_API_KEY: str
    REDIS_URL: str = "redis://localhost:6379/0"
    ROOT_WORKDIR: str = "/tmp/malfit"
    MAX_MB_FREE: int = 200      # 업로드 제한(MVP)
    ALLOW_ORIGIN: str = "*"     # 프론트 도메인 설정(초기엔 *)
settings = Settings()

WORK = Path(settings.ROOT_WORKDIR)
UP = WORK / "uploads"
OUT = WORK / "out"
for p in (UP, OUT): p.mkdir(parents=True, exist_ok=True)

r = redis.from_url(settings.REDIS_URL, decode_responses=True)

app = FastAPI(title="malfit-api")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.ALLOW_ORIGIN] if settings.ALLOW_ORIGIN!="*" else ["*"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

def job_key(jid:str) -> str: return f"job:{jid}"

@app.post("/upload")
async def upload(file: UploadFile = File(...), language: str = Form("ko")):
    # 용량 가드 (헤더에 사이즈가 없을 수도 있어 보조 가드임)
    fname = file.filename or "video.mp4"
    if not fname.lower().endswith((".mp4",".mov",".mkv",".m4v",".webm")):
        raise HTTPException(400, "영상 파일만 허용합니다.")

    jid = str(uuid.uuid4())
    local = UP / f"{jid}.mp4"

    # 스트리밍 저장
    size = 0
    with local.open("wb") as f:
        while True:
            chunk = await file.read(1024*1024)
            if not chunk: break
            size += len(chunk)
            if size > settings.MAX_MB_FREE * 1024 * 1024:
                f.close(); local.unlink(missing_ok=True)
                raise HTTPException(413, f"업로드 제한 {settings.MAX_MB_FREE}MB 초과")
            f.write(chunk)

    # Job 큐 적재
    r.hmset(job_key(jid), {
        "status":"queued","progress":"0","language":language,"video":str(local),
        "createdAt": str(int(time.time()))
    })
    r.lpush("queue:shorts", jid)
    return {"jobId": jid}

@app.get("/jobs/{jid}")
def job_status(jid: str):
    d = r.hgetall(job_key(jid))
    if not d: raise HTTPException(404, "not found")
    return d

@app.get("/download/{jid}/{kind}")
def download(jid: str, kind: str):
    # kind: srt | rewritten | vo
    base = OUT / jid
    path = {
        "srt": base.with_suffix(".srt"),
        "rewritten": base.with_name(f"{jid}_rewritten.srt"),
        "vo": base.with_name(f"{jid}_vo.txt"),
    }.get(kind)
    if not path or not path.exists():
        raise HTTPException(404, "file not ready")
    media = "text/plain" if path.suffix==".txt" else "application/x-subrip"
    return FileResponse(str(path), media_type=media, filename=path.name)

@app.get("/")
def root(): return {"ok":True, "msg":"malfit api"}
