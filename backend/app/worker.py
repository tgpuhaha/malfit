# app/worker.py
import os, time, subprocess, json
from pathlib import Path
import redis
from openai import OpenAI

# ─────────────────────────────────────
# 환경변수
# ─────────────────────────────────────
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
REDIS_URL      = os.environ["REDIS_URL"]
ROOT           = Path(os.environ.get("ROOT_WORKDIR", "/tmp/malfit"))

# 디렉터리 준비
UP  = ROOT / "uploads"
OUT = ROOT / "out"
UP.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

# 클라이언트
r = redis.from_url(REDIS_URL, decode_responses=True)
client = OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────
# 유틸
# ─────────────────────────────────────
def key(jid: str) -> str:
    return f"job:{jid}"

def set_status(jid: str, **kv):
    """상태/진행률/결과 등을 저장"""
    k = key(jid)
    for kk, vv in kv.items():
        # dict도 들어올 수 있으니 JSON 직렬화
        if isinstance(vv, (dict, list)):
            r.hset(k, kk, json.dumps(vv, ensure_ascii=False))
        else:
            r.hset(k, kk, str(vv))

def append_log(jid: str, line: str):
    """간단 로그 축적 (오류 파악용)"""
    k = key(jid)
    cur = r.hget(k, "log") or ""
    cur = (cur + ("\n" if cur else "") + line)[:4000]  # 길이 제한
    r.hset(k, "log", cur)

def check_ffmpeg() -> bool:
    try:
        p = subprocess.run(["ffmpeg", "-version"], capture_output=True)
        return p.returncode == 0
    except Exception:
        return False

def ffprobe_duration(p: Path) -> float:
    try:
        cmd = ["ffprobe","-v","error","-show_entries","format=duration",
               "-of","default=noprint_wrappers=1:nokey=1",str(p)]
        run = subprocess.run(cmd, capture_output=True)
        if run.returncode == 0:
            s = (run.stdout or b"").decode("utf-8", errors="ignore").strip()
            return float(s) if s else 0.0
    except Exception:
        pass
    return 0.0

def run_ffmpeg(args: list) -> tuple[bool, str]:
    """ffmpeg 실행 -> (성공여부, stderr 텍스트)"""
    p = subprocess.run(args, capture_output=True)
    ok  = (p.returncode == 0)
    err = (p.stderr or b"").decode("utf-8", errors="ignore")
    return ok, err

def extract_audio_with_fallback(src: Path) -> Path:
    """
    3단계 폴백:
      1) 스트림 복사(copy) → .m4a
      2) aac 인코딩 → .m4a
      3) wav(pcm_s16le, 16kHz mono) → .wav
    성공 시 추출 파일 경로 반환. 전부 실패하면 예외 발생.
    """
    base     = src.with_suffix("")
    m4a_copy = base.with_suffix(".m4a")
    m4a_aac  = base.with_name(base.name + "_aac").with_suffix(".m4a")
    wav_path = base.with_suffix(".wav")

    # 1) copy (가장 빠르고 무손실)
    ok1, err1 = run_ffmpeg(["ffmpeg","-y","-i",str(src),"-vn","-c:a","copy",str(m4a_copy)])
    if ok1 and m4a_copy.exists():
        return m4a_copy

    # 2) aac 인코딩 (일반적인 성공 경로)
    ok2, err2 = run_ffmpeg(["ffmpeg","-y","-i",str(src),"-vn","-c:a","aac","-b:a","192k","-ar","44100","-ac","2",str(m4a_aac)])
    if ok2 and m4a_aac.exists():
        return m4a_aac

    # 3) wav (Whisper 100% 호환)
    ok3, err3 = run_ffmpeg(["ffmpeg","-y","-i",str(src),"-vn","-acodec","pcm_s16le","-ar","16000","-ac","1",str(wav_path)])
    if ok3 and wav_path.exists():
        return wav_path

    # 모두 실패 → 합쳐서 보고
    raise RuntimeError(
        "ffmpeg 추출 실패\n"
        f"[copy] {err1[:800]}\n"
        f"[aac ] {err2[:800]}\n"
        f"[wav ] {err3[:800]}"
    )

def whisper_srt(audio: Path, language: str) -> str:
    with audio.open("rb") as f:
        resp = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="srt",
            language=language or "ko",
        )
    return str(resp)

def rewrite_srt(srt_text: str) -> str:
    SYS = ("너는 한국어 영상 대본 리라이터다. 의미는 유지하되 표현은 크게 바꿔라. "
           "인덱스/타임스탬프는 절대 변경 금지. 각 블록 길이 ±30% 변동 허용.")
    USR = f"[SRT]\n{srt_text}\n[/SRT]\n위 규칙대로 SRT만 출력."
    c = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.7,
        messages=[{"role":"system","content":SYS},
                  {"role":"user","content":USR}]
    )
    return c.choices[0].message.content or srt_text

def make_vo_text(srt_text: str) -> str:
    SYS = "넌 한국어 보이스오버 작가. 타임코드 제거, 자연스러운 구어체 문단으로."
    USR = f"[SRT]\n{srt_text}\n[/SRT]\n타임코드 제거하고 음성 대본만 출력."
    c = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.6,
        messages=[{"role":"system","content":SYS},
                  {"role":"user","content":USR}]
    )
    return (c.choices[0].message.content or "").strip()

# ─────────────────────────────────────
# 메인 루프
# ─────────────────────────────────────
def run():
    if not check_ffmpeg():
        # Render의 apt.txt 설치 누락 등
        raise RuntimeError("ffmpeg 미설치/미인식: backend/apt.txt에 'ffmpeg'가 있고 Root Directory가 backend인지 확인하세요.")

    while True:
        item = r.brpop("queue:shorts", timeout=5)
        if not item:
            continue

        jid = item[1]              # (queue, job_id)
        d   = r.hgetall(key(jid))
        vid = Path(d.get("video", ""))
        lang = d.get("language", "ko")

        audio_path = None
        srt_path   = None
        rew_path   = None
        vo_path    = None

        try:
            set_status(jid, status="downloading", progress=5)
            append_log(jid, f"job {jid} 시작: {vid.name}")

            # 길이
            dur = ffprobe_duration(vid)
            set_status(jid, durationSec=dur)

            # 1) 오디오 추출 (폴백 로직)
            set_status(jid, status="audio_extract", progress=15)
            audio_path = extract_audio_with_fallback(vid)
            append_log(jid, f"오디오 추출 OK → {audio_path.suffix}")

            # 2) Whisper 전사
            set_status(jid, status="whisper", progress=40)
            srt_text = whisper_srt(audio_path, lang)
            srt_path = (OUT / jid).with_suffix(".srt")
            srt_path.write_text(srt_text, encoding="utf-8")

            # 3) (강한) 재작성
            set_status(jid, status="rewrite", progress=70)
            rew = rewrite_srt(srt_text)
            rew_path = (OUT / f"{jid}_rewritten").with_suffix(".srt")
            rew_path.write_text(rew, encoding="utf-8")

            # 4) 보이스오버 대본
            set_status(jid, status="vo", progress=85)
            vo = make_vo_text(rew)
            vo_path = (OUT / f"{jid}_vo").with_suffix(".txt")
            vo_path.write_text(vo, encoding="utf-8")

            # 완료
            set_status(
                jid,
                status="done",
                progress=100,
                result=f"/download/{jid}/srt|/download/{jid}/rewritten|/download/{jid}/vo"
            )
            append_log(jid, "완료 ✅")

        except Exception as e:
            # 에러 메시지 저장
            set_status(jid, status="error", error=str(e))
            append_log(jid, f"에러 ❌: {e}")

        finally:
            # 임시파일 정리 (업로드 원본/중간 산출물은 서버 공간 보호 위해 삭제)
            try:
                if audio_path and audio_path.exists(): audio_path.unlink(missing_ok=True)
                # 업로드 원본은 백엔드가 둔 파일 경로(vid) → 여기서도 정리 시도(백엔드에서 삭제하지 않는 MVP이므로)
                if vid and vid.exists(): vid.unlink(missing_ok=True)
            except Exception:
                pass

if __name__ == "__main__":
    run()
