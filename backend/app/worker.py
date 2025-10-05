# app/worker.py
import os, time, subprocess, re
from pathlib import Path
import redis
from openai import OpenAI

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
REDIS_URL = os.environ["REDIS_URL"]
ROOT = Path(os.environ.get("ROOT_WORKDIR","/tmp/malfit"))

UP = ROOT/"uploads"; OUT = ROOT/"out"
UP.mkdir(parents=True, exist_ok=True); OUT.mkdir(parents=True, exist_ok=True)

r = redis.from_url(REDIS_URL, decode_responses=True)
client = OpenAI(api_key=OPENAI_API_KEY)

def key(jid): return f"job:{jid}"
def set_status(jid, **kv):
    for k,v in kv.items(): r.hset(key(jid), k, str(v))

def ffprobe_duration(p: Path) -> float:
    try:
        cmd = ["ffprobe","-v","error","-show_entries","format=duration",
               "-of","default=noprint_wrappers=1:nokey=1",str(p)]
        run = subprocess.run(cmd, capture_output=True)
        if run.returncode==0:
            s=(run.stdout or b"").decode().strip()
            return float(s) if s else 0.0
    except: pass
    return 0.0

def whisper_srt(audio: Path, language: str) -> str:
    with audio.open("rb") as f:
        resp = client.audio.transcriptions.create(
            model="whisper-1", file=f, response_format="srt", language=language
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

def run():
    while True:
        item = r.brpop("queue:shorts", timeout=5)
        if not item: 
            continue
        jid = item[1]
        d = r.hgetall(key(jid))
        vid = Path(d.get("video",""))
        lang = d.get("language","ko")
        try:
            set_status(jid, status="audio_extract", progress=10)
            dur = ffprobe_duration(vid)
            m4a = vid.with_suffix(".m4a")
            subprocess.run(["ffmpeg","-y","-i",str(vid),"-vn","-acodec","aac","-ar","44100","-ac","2",str(m4a)], check=True)

            set_status(jid, status="whisper", progress=40)
            srt_text = whisper_srt(m4a, lang)
            srt_path = (OUT/jid).with_suffix(".srt")
            srt_path.write_text(srt_text, encoding="utf-8")

            set_status(jid, status="rewrite", progress=70)
            rew = rewrite_srt(srt_text)
            rew_path = (OUT/f"{jid}_rewritten").with_suffix(".srt")
            rew_path.write_text(rew, encoding="utf-8")

            set_status(jid, status="vo", progress=85)
            vo = make_vo_text(rew)
            vo_path = (OUT/f"{jid}_vo").with_suffix(".txt")
            vo_path.write_text(vo, encoding="utf-8")

            set_status(jid, status="done", progress=100,
                       result=f"/download/{jid}/srt|/download/{jid}/rewritten|/download/{jid}/vo",
                       durationSec=dur)
        except Exception as e:
            set_status(jid, status="error", error=str(e))

if __name__ == "__main__":
    run()
