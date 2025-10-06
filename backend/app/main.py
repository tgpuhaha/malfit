# app/main.py
import os, time, uuid, json, shutil, subprocess, random
from datetime import datetime
from pathlib import Path
from typing import Dict
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic_settings import BaseSettings
import redis
from openai import OpenAI

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
client = OpenAI(api_key=S.OPENAI_API_KEY)

app = FastAPI(title="malfit-api", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if S.ALLOW_ORIGIN == "*" else [S.ALLOW_ORIGIN],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

def k(jid: str) -> str: return f"job:{jid}"
def set_status(jid: str, **kv):
    K = k(jid)
    for kk, vv in kv.items():
        r.hset(K, kk, json.dumps(vv, ensure_ascii=False) if isinstance(vv,(dict,list)) else str(vv))
def append_log(jid: str, line: str):
    K = k(jid)
    cur = r.hget(K, "log") or ""
    cur = (cur + ("\n" if cur else "") + line)[-4000:]
    r.hset(K, "log", cur)

# ───────────── ffprobe/ffmpeg 유틸 (폴백 포함)
def run_ffmpeg(args: list) -> tuple[bool, str]:
    if "ffmpeg" in args[0] and "-hide_banner" not in args:
        args = args[:1] + ["-hide_banner"] + args[1:]
    p = subprocess.run(args, capture_output=True)
    err = (p.stderr or b"").decode("utf-8", errors="ignore")
    tail = err[-1200:] if len(err) > 1200 else err
    return p.returncode == 0, tail

def ffprobe_duration(p: Path) -> float:
    try:
        cmd = ["ffprobe","-v","error","-show_entries","format=duration",
               "-of","default=noprint_wrappers=1:nokey=1",str(p)]
        run = subprocess.run(cmd, capture_output=True)
        if run.returncode==0:
            s=(run.stdout or b"").decode("utf-8","ignore").strip()
            return float(s) if s else 0.0
    except: pass
    return 0.0

def ffprobe_has_audio(src: Path) -> bool:
    try:
        cmd = ["ffprobe","-v","error","-select_streams","a:0",
               "-show_entries","stream=codec_name","-of","default=nw=1:nk=1",str(src)]
        p = subprocess.run(cmd, capture_output=True)
        out = (p.stdout or b"").decode("utf-8","ignore").strip()
        return bool(out)
    except: return False

def extract_audio_with_fallback(src: Path) -> Path:
    if not ffprobe_has_audio(src):
        raise RuntimeError("입력 영상에 오디오 트랙이 없습니다. (무음 영상)")
    base     = src.with_suffix("")
    m4a_copy = base.with_suffix(".m4a")
    m4a_aac  = base.with_name(base.name + "_aac").with_suffix(".m4a")
    wav_path = base.with_suffix(".wav")

    ok1, e1 = run_ffmpeg(["ffmpeg","-y","-i",str(src),"-vn","-c:a","copy",str(m4a_copy)])
    if ok1 and m4a_copy.exists(): return m4a_copy

    ok2, e2 = run_ffmpeg(["ffmpeg","-y","-i",str(src),"-vn","-c:a","aac","-b:a","192k","-ar","44100","-ac","2",str(m4a_aac)])
    if ok2 and m4a_aac.exists(): return m4a_aac

    ok3, e3 = run_ffmpeg(["ffmpeg","-y","-i",str(src),"-vn","-acodec","pcm_s16le","-ar","16000","-ac","1",str(wav_path)])
    if ok3 and wav_path.exists(): return wav_path

    raise RuntimeError(f"ffmpeg 추출 실패\n[copy]{e1}\n[aac ]{e2}\n[wav ]{e3}")

# ───────────── Whisper/GPT
def whisper_srt(audio: Path, language: str) -> str:
    with audio.open("rb") as f:
        resp = client.audio.transcriptions.create(
            model="whisper-1", file=f, response_format="srt", language=language or "ko"
        )
    return str(resp)

def rewrite_srt(srt_text: str) -> str:
    SYS = ("너는 한국어 영상 대본 리라이터다. 의미는 유지하되 표현은 크게 바꿔라. "
           "인덱스/타임스탬프는 절대 변경 금지. 각 블록 길이 ±30% 변동 허용.")
    USR = f"[SRT]\n{srt_text}\n[/SRT]\n위 규칙대로 SRT만 출력."
    c = client.chat.completions.create(
        model="gpt-4o-mini", temperature=0.7,
        messages=[{"role":"system","content":SYS},{"role":"user","content":USR}]
    )
    return c.choices[0].message.content or srt_text

def make_vo_text(srt_text: str) -> str:
    SYS = "넌 한국어 보이스오버 작가. 타임코드 제거, 자연스러운 구어체 문단으로."
    USR = f"[SRT]\n{srt_text}\n[/SRT]\n타임코드 제거하고 음성 대본만 출력."
    c = client.chat.completions.create(
        model="gpt-4o-mini", temperature=0.6,
        messages=[{"role":"system","content":SYS},{"role":"user","content":USR}]
    )
    return (c.choices[0].message.content or "").strip()

# ───────────── 백그라운드 작업
def process_job(jid: str, video_path: str, language: str):
    vid = Path(video_path)
    try:
        set_status(jid, status="downloading", progress=5)
        if not vid.exists():
            raise RuntimeError(f"업로드된 파일이 존재하지 않습니다: {vid}")

        dur = ffprobe_duration(vid)
        set_status(jid, durationSec=dur)

        set_status(jid, status="audio_extract", progress=15)
        append_log(jid, f"ffprobe audio? {ffprobe_has_audio(vid)}")
        audio = extract_audio_with_fallback(vid)
        append_log(jid, f"audio => {audio.suffix}")

        set_status(jid, status="whisper", progress=40)
        srt = whisper_srt(audio, language)
        srt_path = (OUT/jid).with_suffix(".srt"); srt_path.write_text(srt, encoding="utf-8")

        set_status(jid, status="rewrite", progress=70)
        rew = rewrite_srt(srt)
        rew_path = (OUT/f"{jid}_rewritten").with_suffix(".srt"); rew_path.write_text(rew, encoding="utf-8")

        set_status(jid, status="vo", progress=85)
        vo = make_vo_text(rew)
        vo_path = (OUT/f"{jid}_vo").with_suffix(".txt"); vo_path.write_text(vo, encoding="utf-8")

        set_status(jid, status="done", progress=100,
                   result=f"/download/{jid}/srt|/download/{jid}/rewritten|/download/{jid}/vo")
        append_log(jid, "완료 ✅")

    except Exception as e:
        set_status(jid, status="error", error=str(e))
        append_log(jid, f"에러 ❌ {e}")
    finally:
        try:
            if 'audio' in locals() and Path(audio).exists():
                Path(audio).unlink(missing_ok=True)
            if vid.exists(): vid.unlink(missing_ok=True)
        except: pass

# ───────────── 라우트
@app.get("/")
def root(): return {"ok": True, "msg": "malfit api (single-service background)"}

@app.post("/upload")
async def upload(background_tasks: BackgroundTasks,
                 file: UploadFile = File(...), language: str = Form("ko")):
    if not file.filename:
        raise HTTPException(400, "파일명이 없습니다.")
    if not file.filename.lower().endswith((".mp4",".mov",".mkv",".m4v",".webm")):
        raise HTTPException(400, "영상 파일만 허용합니다.")

    jid = str(uuid.uuid4())
    dest = UP / f"{jid}.mp4"

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

    set_status(jid, status="queued", progress=0,
               video=str(dest), language=language, createdAt=int(time.time()))
    # 같은 인스턴스에서 즉시 백그라운드 처리
    background_tasks.add_task(process_job, jid, str(dest), language)
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
