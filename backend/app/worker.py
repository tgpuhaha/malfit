# app/worker.py
import os, time, random, json, shutil, subprocess
from datetime import datetime
from pathlib import Path
import redis
from openai import OpenAI

# =============== 공통 설정 ===============
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
REDIS_URL      = os.environ["REDIS_URL"]
ROOT           = Path(os.environ.get("ROOT_WORKDIR","/tmp/malfit"))
UP, OUT = ROOT/"uploads", ROOT/"out"
UP.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

r = redis.from_url(REDIS_URL, decode_responses=True)
client = OpenAI(api_key=OPENAI_API_KEY)

# =============== 로깅 유틸 ===============
_T0 = time.time()
def ts() -> str: return datetime.now().strftime("%H:%M:%S")
def since() -> str: return f"+{time.time()-_T0:5.1f}s"
def clog(msg: str, lv="info"):
    C = {"info":"\033[36m","ok":"\033[32m","warn":"\033[33m","err":"\033[31m","dbg":"\033[35m"}
    e = "\033[0m"
    print(f"{C.get(lv,'')}[{ts()} {since()}] {msg}{e}")

def backoff(i: int): time.sleep(0.8*(2**i) + random.random()*0.3)

def key(jid: str) -> str: return f"job:{jid}"
def set_status(jid: str, **kv):
    k = key(jid)
    for kk, vv in kv.items():
        r.hset(k, kk, json.dumps(vv, ensure_ascii=False) if isinstance(vv,(dict,list)) else str(vv))

def append_log(jid: str, line: str):
    k = key(jid)
    cur = r.hget(k, "log") or ""
    cur = (cur + ("\n" if cur else "") + line)[-4000:]
    r.hset(k, "log", cur)

# =============== ffprobe/ffmpeg ===============
def check_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None

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
    """오디오 스트림 존재 확인"""
    try:
        cmd = ["ffprobe","-v","error","-select_streams","a:0",
               "-show_entries","stream=codec_name","-of","default=nw=1:nk=1",str(src)]
        p = subprocess.run(cmd, capture_output=True)
        out = (p.stdout or b"").decode("utf-8","ignore").strip()
        return bool(out)
    except: return False

def run_ffmpeg(args: list) -> tuple[bool,str]:
    if "ffmpeg" in args[0] and "-hide_banner" not in args:
        args = args[:1] + ["-hide_banner"] + args[1:]
    p = subprocess.run(args, capture_output=True)
    err = (p.stderr or b"").decode("utf-8","ignore")
    tail = err[-1200:] if len(err)>1200 else err
    return p.returncode == 0, tail

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

# =============== Whisper / GPT ===============
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

# =============== 메인 루프 ===============
def run():
    if not check_ffmpeg():
        raise RuntimeError("ffmpeg/ffprobe 미설치: backend/apt.txt에 'ffmpeg'가 있고 Root Directory=backend 인지 확인.")

    clog("worker started", "ok")
    while True:
        item = r.brpop("queue:shorts", timeout=5)
        if not item: 
            continue

        jid = item[1]
        d   = r.hgetall(key(jid))
        vid = Path(d.get("video",""))
        lang = d.get("language","ko")

        audio_path = None; srt_path=None; rew_path=None; vo_path=None
        try:
            set_status(jid, status="downloading", progress=5)
            if not vid.exists():
                raise RuntimeError(f"업로드된 파일 없음: {vid}")

            dur = ffprobe_duration(vid)
            set_status(jid, durationSec=dur)

            set_status(jid, status="audio_extract", progress=15)
            append_log(jid, f"ffprobe audio? {ffprobe_has_audio(vid)}")
            audio_path = extract_audio_with_fallback(vid)
            append_log(jid, f"audio: {audio_path.suffix}")

            set_status(jid, status="whisper", progress=40)
            srt_text = whisper_srt(audio_path, lang)
            srt_path = (OUT/jid).with_suffix(".srt"); srt_path.write_text(srt_text, encoding="utf-8")

            set_status(jid, status="rewrite", progress=70)
            rew = rewrite_srt(srt_text)
            rew_path = (OUT/f"{jid}_rewritten").with_suffix(".srt"); rew_path.write_text(rew, encoding="utf-8")

            set_status(jid, status="vo", progress=85)
            vo = make_vo_text(rew)
            vo_path = (OUT/f"{jid}_vo").with_suffix(".txt"); vo_path.write_text(vo, encoding="utf-8")

            set_status(jid, status="done", progress=100,
                       result=f"/download/{jid}/srt|/download/{jid}/rewritten|/download/{jid}/vo")
            append_log(jid, "완료 ✅")
            clog(f"done {jid}", "ok")

        except Exception as e:
            set_status(jid, status="error", error=str(e))
            append_log(jid, f"에러 ❌ {e}")
            clog(f"error {jid}: {e}", "err")

        finally:
            # 공간 보호: 업로드 원본/중간 산출물 정리
            try:
                if audio_path and audio_path.exists(): audio_path.unlink(missing_ok=True)
                if vid.exists(): vid.unlink(missing_ok=True)
            except: pass

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt:
        print(); clog("worker stopped by user","warn")
