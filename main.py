"""
============================================================
🎬 FULLY AUTOMATED MULTI-MOVIE INSTAGRAM REEL UPLOADER
============================================================
Drive folder → detect → download → split (ffmpeg, fast!)
→ thumbnail (Gemini AI) → upload → track → repeat

Author: Auto Reel Bot
============================================================
"""

import os
import sys
import json
import time
import random
import shutil
import subprocess
import requests
import traceback
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta

# Image / Thumbnail only — moviepy NOT used for cutting (too slow)
from PIL import Image, ImageDraw, ImageFont

# Instagram
from instagrapi import Client
from instagrapi.exceptions import (
    LoginRequired,
    ChallengeRequired,
    FeedbackRequired,
    PleaseWaitFewMinutes,
    ClientThrottledError,
)

# Google Drive download
import gdown

# Gemini AI for thumbnails
GEMINI_AVAILABLE = False
try:
    from google import genai
    from google.genai import types as genai_types
    GEMINI_AVAILABLE = True
except ImportError:
    print("⚠️ google-genai not installed → using video-frame thumbnails")


# ============================================================
#                    CONFIGURATION
# ============================================================
class Config:
    # --- Credentials (from GitHub Secrets) ---
    IG_USERNAME      = os.environ.get("IG_USERNAME", "")
    IG_PASSWORD      = os.environ.get("IG_PASSWORD", "")
    IG_SESSION       = os.environ.get("IG_SESSION", "")   # full contents of session.json
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "")
    GDRIVE_API_KEY   = os.environ.get("GDRIVE_API_KEY", "")
    GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")

    # --- Video ---
    CLIP_LENGTH = 60            # seconds per reel

    # --- Upload timing ---
    # 3 uploads per run × 4 runs per day = 12 reels/day
    MAX_UPLOADS_PER_RUN = 3
    DELAY_MIN = 7080            # 1h 58m in seconds
    DELAY_MAX = 7380            # 2h 03m in seconds

    # --- File paths ---
    REELS_DIR      = "reels"
    THUMBS_DIR     = "thumbnails"
    MOVIE_FILE     = "current_movie.mp4"
    SESSION_FILE   = "session.json"
    LOG_FILE       = "movies_log.json"
    PROGRESS_FILE  = "progress.json"
    DETAIL_LOG     = "detailed_log.txt"
    THUMB_BG_FILE  = "thumb_background.jpg"   # cached thumbnail background

    VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".webm")

    CAPTIONS = [
        "🎬 {name} | Part {p}/{t}\n\n#movie #reels #viral #trending #fyp #cinema",
        "🔥 {name} — Part {p}/{t}\n\nFollow for next part! 🍿\n\n#movie #viral #reels",
        "🎥 {name} [{p}/{t}]\n\n⬇️ Follow for more parts!\n\n#movies #cinema #viral #fyp",
        "🍿 {name} | Part {p} of {t}\n\nLike & Follow for more ❤️\n\n#movie #trending #reels",
        "📽️ {name} • Part {p}/{t}\n\nStay tuned! 🔔\n\n#film #reels #viral #trending #fyp",
    ]

    # Gemini vision models — pick best thumbnail frame
    GEMINI_VISION_MODELS = [
        "gemini-2.0-flash",
        "gemini-1.5-flash",
        "gemini-1.5-pro",
    ]

    # Gemini image generation models — AI poster background (optional)
    GEMINI_IMAGE_MODELS = [
        "gemini-2.0-flash-exp-image-generation",
        "imagen-3.0-generate-002",
    ]


# ============================================================
#                      LOGGER
# ============================================================
class Logger:
    def __init__(self, filepath):
        self.filepath = filepath

    def _ts(self):    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def _short(self): return datetime.now().strftime("%H:%M:%S")

    def _write(self, line):
        try:
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def info(self, msg):
        print(f"[{self._short()}] ✅ {msg}")
        self._write(f"[{self._ts()}] INFO  | {msg}")

    def warn(self, msg):
        print(f"[{self._short()}] ⚠️  {msg}")
        self._write(f"[{self._ts()}] WARN  | {msg}")

    def error(self, msg):
        print(f"[{self._short()}] ❌ {msg}")
        self._write(f"[{self._ts()}] ERROR | {msg}")

    def upload(self, movie, part, total, status):
        line = f"[{self._ts()}] UPLOAD | {movie} | Part {part}/{total} | {status}"
        print(f"  📤 {movie} Part {part}/{total} → {status}")
        self._write(line)

    def separator(self, char="=", length=60):
        sep = char * length
        print(sep)
        self._write(sep)


log = Logger(Config.DETAIL_LOG)


# ============================================================
#                   HELPERS
# ============================================================
def load_json(filepath, default=None):
    if default is None:
        default = {}
    try:
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log.error(f"Failed to load {filepath}: {e}")
    return default


def save_json(filepath, data):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except IOError as e:
        log.error(f"Failed to save {filepath}: {e}")


def movie_display_name(filename):
    return Path(filename).stem


def cleanup_temp():
    for path in [Config.MOVIE_FILE, Config.THUMB_BG_FILE]:
        if os.path.exists(path):
            os.remove(path)
    for folder in [Config.REELS_DIR, Config.THUMBS_DIR]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
    log.info("🧹 Temp files cleaned up")


def git_push():
    try:
        os.system('git config user.name "Reel Bot"')
        os.system('git config user.email "bot@reelbot.com"')
        for f in [Config.LOG_FILE, Config.PROGRESS_FILE,
                  Config.SESSION_FILE, Config.DETAIL_LOG]:
            if os.path.exists(f):
                os.system(f'git add "{f}"')
        os.system('git diff --staged --quiet || git commit -m "🤖 Auto: progress update"')
        os.system('git push')
        log.info("📁 Progress pushed to GitHub")
    except Exception as e:
        log.error(f"Git push failed: {e}")


# ============================================================
#              SETUP  (session + verify)
# ============================================================
def write_session_from_secret():
    """
    Write IG_SESSION GitHub Secret → session.json on disk.
    Paste the FULL contents of session.json into the secret,
    INCLUDING the opening { and closing }.
    Secret name: IG_SESSION
    """
    session_json = Config.IG_SESSION.strip()
    if not session_json:
        return

    try:
        parsed = json.loads(session_json)
    except json.JSONDecodeError as e:
        log.error(f"IG_SESSION secret is not valid JSON: {e}")
        log.error("Paste the FULL session.json contents including {{ and }}")
        return

    try:
        with open(Config.SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(parsed, f, indent=4, ensure_ascii=False)
        log.info("🔑 session.json written from IG_SESSION secret")
    except IOError as e:
        log.error(f"Failed to write session.json: {e}")


def verify_setup():
    critical_missing = []
    if not Config.IG_USERNAME:      critical_missing.append("IG_USERNAME")
    if not Config.IG_PASSWORD:      critical_missing.append("IG_PASSWORD")
    if not Config.GDRIVE_FOLDER_ID: critical_missing.append("GDRIVE_FOLDER_ID")
    if not Config.GDRIVE_API_KEY:   critical_missing.append("GDRIVE_API_KEY")

    if not Config.GEMINI_API_KEY:
        log.warn("GEMINI_API_KEY not set → video-frame thumbnails only")
    if not Config.IG_SESSION and not os.path.exists(Config.SESSION_FILE):
        log.warn("IG_SESSION secret empty and no session.json found — login will fail")

    if critical_missing:
        for m in critical_missing:
            log.error(f"Missing required secret: {m}")
        return False

    log.info("✅ All required credentials verified")
    return True


# ============================================================
#                 GOOGLE DRIVE
# ============================================================
def list_drive_movies():
    folder_id  = Config.GDRIVE_FOLDER_ID
    api_key    = Config.GDRIVE_API_KEY
    url        = "https://www.googleapis.com/drive/v3/files"
    all_files  = []
    page_token = None

    log.info(f"📂 Scanning Google Drive folder: {folder_id}")

    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed=false",
            "key": api_key,
            "fields": "nextPageToken,files(id,name,size,mimeType,createdTime)",
            "pageSize": 100,
            "orderBy": "name",
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 403:
                log.error("Drive API: Access denied. Check key + folder sharing.")
                return []
            if r.status_code == 404:
                log.error("Drive API: Folder not found. Check GDRIVE_FOLDER_ID.")
                return []
            if r.status_code != 200:
                log.error(f"Drive API error {r.status_code}: {r.text[:200]}")
                return []

            data = r.json()
            for f in data.get("files", []):
                if any(f["name"].lower().endswith(ext) for ext in Config.VIDEO_EXTS):
                    all_files.append({
                        "id":      f["id"],
                        "name":    f["name"],
                        "size":    int(f.get("size", 0)),
                        "created": f.get("createdTime", ""),
                    })
            page_token = data.get("nextPageToken")
            if not page_token:
                break

        except requests.exceptions.RequestException as e:
            log.error(f"Drive API request failed: {e}")
            return []

    log.info(f"📂 Found {len(all_files)} video files in Drive")
    return all_files


def download_movie(file_id, output_path):
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
        log.info("📥 Downloading movie from Google Drive...")
        gdown.download(f"https://drive.google.com/uc?id={file_id}",
                       output_path, quiet=False)
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            log.info(f"📥 Download complete! Size: {size_mb:.1f} MB")
            return True
        log.error("Download produced empty or missing file")
        return False
    except Exception as e:
        log.error(f"Download failed: {e}")
        log.error(traceback.format_exc())
        return False


# ============================================================
#          VIDEO — ffmpeg-based (FAST, no re-encoding)
# ============================================================
def ffprobe_duration(video_path):
    """
    Get video duration in seconds using ffprobe.
    Returns float or 0 on failure.
    """
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return float(result.stdout.strip())
    except Exception as e:
        log.error(f"ffprobe failed: {e}")
        return 0.0


def get_video_info(video_path):
    """Get duration + calculate total 60s parts."""
    duration = ffprobe_duration(video_path)
    if duration <= 0:
        return 0, 0

    total_parts = 0
    start = 0
    while start < duration:
        end = min(start + Config.CLIP_LENGTH, duration)
        if end - start >= 5:
            total_parts += 1
        start += Config.CLIP_LENGTH

    return duration, total_parts


def extract_clip(video_path, part_num, output_path):
    """
    Cut a 60-second clip using ffmpeg stream-copy (NO re-encoding).
    This is 20-50x faster than moviepy because it just copies bytes.
    GitHub runner: ~3-5 seconds per clip instead of 3-5 minutes.
    """
    start = (part_num - 1) * Config.CLIP_LENGTH
    duration_sec = Config.CLIP_LENGTH

    log.info(f"✂️ Cutting Part {part_num} with ffmpeg "
             f"({start}s → {start + duration_sec}s)...")

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),          # seek BEFORE input = fast
        "-i", video_path,
        "-t", str(duration_sec),
        "-c", "copy",               # stream copy = no re-encoding = FAST
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",
        output_path,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,            # 2 min max per clip
        )
        if result.returncode != 0:
            log.error(f"ffmpeg error: {result.stderr[-500:]}")
            return False

        if os.path.exists(output_path) and os.path.getsize(output_path) > 10_000:
            log.info(f"✂️ Part {part_num} ready "
                     f"({os.path.getsize(output_path) / 1024 / 1024:.1f} MB)")
            return True
        else:
            log.error(f"ffmpeg produced empty output for part {part_num}")
            return False

    except subprocess.TimeoutExpired:
        log.error(f"ffmpeg timed out on part {part_num}")
        return False
    except Exception as e:
        log.error(f"extract_clip failed: {e}")
        return False


def extract_frame_ffmpeg(video_path, time_sec, output_jpg):
    """
    Extract a single frame as JPEG using ffmpeg.
    Fast — typically under 1 second.
    """
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(time_sec),
        "-i", video_path,
        "-frames:v", "1",
        "-q:v", "2",
        output_jpg,
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=30)
        if os.path.exists(output_jpg) and os.path.getsize(output_jpg) > 0:
            return Image.open(output_jpg).copy()
    except Exception as e:
        log.error(f"Frame extract failed: {e}")
    return Image.new("RGB", (1280, 720), (20, 20, 40))


def extract_frames_for_grid(video_path, duration, frame_count=9):
    """
    Extract frame_count evenly-spaced frames via ffmpeg.
    Returns list of PIL Images.
    """
    log.info(f"🎞️ Extracting {frame_count} frames for thumbnail selection...")
    frames = []
    tmp_dir = "tmp_frames"
    os.makedirs(tmp_dir, exist_ok=True)

    for i in range(frame_count):
        t   = duration * (0.15 + i * 0.07)
        t   = min(t, duration - 1.0)
        out = os.path.join(tmp_dir, f"frame_{i}.jpg")
        img = extract_frame_ffmpeg(video_path, t, out)
        frames.append(img)
        log.info(f"  🖼️ Frame {i+1}/{frame_count} extracted (t={t:.1f}s)")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return frames


def create_frame_grid(frames, tile_size=320):
    grid = Image.new("RGB", (tile_size * 3, tile_size * 3))
    for idx, img in enumerate(frames):
        x = (idx % 3) * tile_size
        y = (idx // 3) * tile_size
        grid.paste(img.resize((tile_size, tile_size)), (x, y))
    return grid


# ============================================================
#         GEMINI VISION — pick best thumbnail frame
# ============================================================
def choose_best_frame_with_gemini(grid_image, frames):
    """
    Ask Gemini vision to pick the best frame number (1-9).
    Falls back to middle frame on any error.
    Uses correct SDK format: Part.from_bytes + Part.from_text
    """
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        log.warn("Gemini not available → using middle frame")
        return frames[4]

    buf = BytesIO()
    grid_image.save(buf, format="JPEG", quality=85)
    image_bytes = buf.getvalue()

    prompt_text = (
        "You are selecting the best movie thumbnail frame.\n"
        "The image shows a 3x3 grid numbered:\n"
        "  1 2 3\n  4 5 6\n  7 8 9\n\n"
        "Pick the frame that is brightest, most interesting, "
        "has characters/action, and would attract viewers.\n"
        "Reply with ONLY a single digit 1-9. Nothing else."
    )

    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini init failed: {e} → middle frame")
        return frames[4]

    for model_name in Config.GEMINI_VISION_MODELS:
        try:
            log.info(f"🤖 Asking Gemini ({model_name}) to pick best frame...")
            image_part = genai_types.Part.from_bytes(
                data=image_bytes, mime_type="image/jpeg")
            text_part  = genai_types.Part.from_text(text=prompt_text)
            response   = client.models.generate_content(
                model=model_name,
                contents=[image_part, text_part],
            )
            raw   = response.text.strip()
            digit = next((c for c in raw if c.isdigit() and c != "0"), None)
            if digit and 1 <= int(digit) <= 9:
                log.info(f"🤖 Gemini picked frame #{digit}")
                return frames[int(digit) - 1]
            log.warn(f"Gemini returned '{raw}' → next model")
        except Exception as e:
            log.warn(f"Gemini vision {model_name} failed: {e}")

    log.warn("All Gemini vision models failed → middle frame")
    return frames[4]


# ============================================================
#       GEMINI IMAGE GEN — optional AI poster background
# ============================================================
def generate_gemini_background(movie_name):
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        return None

    prompt = (
        f"Cinematic movie poster background for '{movie_name}'. "
        "Dark moody atmosphere, dramatic lighting, professional quality. "
        "NO text, NO letters, NO words."
    )

    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini init failed: {e}")
        return None

    for model_name in Config.GEMINI_IMAGE_MODELS:
        try:
            log.info(f"🎨 Trying Gemini image gen ({model_name})...")
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    response_modalities=["IMAGE", "TEXT"]
                ),
            )
            if response.candidates:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, "inline_data") and part.inline_data is not None:
                        image = Image.open(BytesIO(part.inline_data.data))
                        log.info(f"🎨 AI background generated ({model_name})!")
                        return image
            log.warn(f"{model_name}: no image → next")
        except Exception as e:
            log.warn(f"Gemini image {model_name} failed: {e}")

    log.warn("All Gemini image models failed → using video frame")
    return None


# ============================================================
#                  THUMBNAIL  (Pillow)
# ============================================================
def get_font(size):
    for fp in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                continue
    return ImageFont.load_default()


def create_thumbnail(bg_image, movie_name, part_num, total_parts,
                     movie_num, total_movies, output_path):
    try:
        thumb = bg_image.copy().resize((1080, 1920), Image.LANCZOS).convert("RGBA")

        overlay = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
        odraw   = ImageDraw.Draw(overlay)
        for y in range(500):
            odraw.rectangle([(0, y), (1080, y+1)],
                            fill=(0, 0, 0, int(220 * (1 - y / 500))))
        for y in range(1420, 1920):
            odraw.rectangle([(0, y), (1080, y+1)],
                            fill=(0, 0, 0, int(220 * ((y - 1420) / 500))))

        thumb = Image.alpha_composite(thumb, overlay).convert("RGB")
        draw  = ImageDraw.Draw(thumb)

        font_title = get_font(68)
        font_part  = get_font(56)
        font_info  = get_font(36)

        # Word-wrapped title
        title = movie_name.upper()
        if len(title) > 18:
            words, lines, line = title.split(), [], ""
            for w in words:
                test = (line + " " + w).strip()
                if len(test) > 18 and line:
                    lines.append(line)
                    line = w
                else:
                    line = test
            if line:
                lines.append(line)
        else:
            lines = [title]

        y_cur = 100
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font_title)
            tw   = bbox[2] - bbox[0]
            x    = (1080 - tw) // 2
            for dx in range(-3, 4):
                for dy in range(-3, 4):
                    draw.text((x+dx, y_cur+dy), line, font=font_title, fill="black")
            draw.text((x, y_cur), line, font=font_title, fill="white")
            y_cur += bbox[3] - bbox[1] + 15

        # Gold part number
        part_text = f"PART {part_num} / {total_parts}"
        bbox = draw.textbbox((0, 0), part_text, font=font_part)
        tw   = bbox[2] - bbox[0]
        x    = (1080 - tw) // 2
        for dx in range(-2, 3):
            for dy in range(-2, 3):
                draw.text((x+dx, 1740+dy), part_text, font=font_part, fill="black")
        draw.text((x, 1740), part_text, font=font_part, fill=(255, 215, 0))

        # Grey counter
        ct   = f"Movie {movie_num} of {total_movies}"
        bbox = draw.textbbox((0, 0), ct, font=font_info)
        draw.text(((1080 - (bbox[2]-bbox[0])) // 2, 1810),
                  ct, font=font_info, fill=(180, 180, 180))

        # Gold line
        draw.rectangle([(200, 1720), (880, 1723)], fill=(255, 215, 0))

        thumb.save(output_path, "JPEG", quality=95)
        log.info(f"🖼️ Thumbnail saved: {output_path}")
        return True

    except Exception as e:
        log.error(f"Thumbnail failed: {e}")
        try:
            fb = Image.new("RGB", (1080, 1920), (20, 20, 40))
            d  = ImageDraw.Draw(fb)
            f2 = get_font(60)
            d.text((100, 800), movie_name,                       font=f2, fill="white")
            d.text((100, 900), f"Part {part_num}/{total_parts}", font=f2, fill=(255,215,0))
            fb.save(output_path, "JPEG")
            return True
        except Exception:
            return False


# ============================================================
#                 INSTAGRAM
# ============================================================
def instagram_login():
    """
    Login using session.json only (written from IG_SESSION secret).
    Never does a raw fresh login from GitHub runners → no OTP.
    """
    if not os.path.exists(Config.SESSION_FILE):
        log.error("❌ session.json not found!")
        log.error("Add IG_SESSION secret (full JSON contents) to GitHub Secrets.")
        return None

    for attempt in range(1, 4):
        try:
            log.info(f"🔐 Login attempt {attempt}/3 (session)...")
            cl = Client()
            cl.delay_range = [2, 5]
            cl.load_settings(Config.SESSION_FILE)
            cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
            cl.get_timeline_feed()
            cl.dump_settings(Config.SESSION_FILE)
            log.info("🔐 Logged in via session ✅")
            return cl

        except ChallengeRequired:
            log.error("⛔ Instagram challenge required.")
            log.error("Run generate_session.py locally, complete challenge, "
                      "update IG_SESSION secret.")
            return None

        except Exception as e:
            log.warn(f"Session login attempt {attempt} failed: {e}")
            if attempt < 3:
                time.sleep(30 * attempt)

    log.error("All session login attempts failed. "
              "Regenerate session.json and update IG_SESSION secret.")
    return None


def upload_reel(cl, video_path, thumbnail_path, caption):
    """Upload one reel. Returns True / False / 'STOP'."""
    for retry in range(1, 4):
        try:
            log.info(f"  📤 Upload attempt {retry}/3...")
            kwargs = {"path": video_path, "caption": caption}
            if thumbnail_path and os.path.exists(thumbnail_path):
                kwargs["thumbnail"] = Path(thumbnail_path)
            cl.clip_upload(**kwargs)
            return True

        except PleaseWaitFewMinutes:
            wait = 600 * retry
            log.warn(f"Rate limited → waiting {wait//60}m...")
            time.sleep(wait)

        except ClientThrottledError:
            wait = 900 * retry
            log.warn(f"Throttled → waiting {wait//60}m...")
            time.sleep(wait)

        except FeedbackRequired as e:
            log.error(f"Feedback required: {e}")
            return "STOP"

        except ChallengeRequired:
            log.error("⛔ Challenge required. Update IG_SESSION secret.")
            return "STOP"

        except LoginRequired:
            log.warn("Session expired mid-upload → re-login...")
            try:
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                cl.dump_settings(Config.SESSION_FILE)
            except Exception:
                log.error("Re-login failed")
                return "STOP"

        except ConnectionError:
            wait = 180 * retry
            log.warn(f"Connection error → waiting {wait//60}m...")
            time.sleep(wait)

        except Exception as e:
            log.error(f"Upload error attempt {retry}: {e}")
            if retry < 3:
                time.sleep(300 * retry)

    return False


# ============================================================
#                   SMART DELAY
# ============================================================
def smart_delay(upload_number):
    base   = random.randint(Config.DELAY_MIN, Config.DELAY_MAX)
    jitter = random.randint(-45, 45)
    total  = max(60, base + jitter)

    next_t = datetime.now() + timedelta(seconds=total)
    log.info(f"⏳ Waiting {total//60}m {total%60}s "
             f"(next upload ≈ {next_t.strftime('%H:%M:%S')})")

    elapsed = 0
    while elapsed < total:
        chunk    = min(60, total - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        remaining = total - elapsed
        if remaining > 0 and elapsed % 600 < 60:
            log.info(f"  ⏳ {remaining//60}m {remaining%60}s remaining...")


# ============================================================
#                  MOVIE TRACKER
# ============================================================
def load_movies_log():
    default = {
        "movies": {},
        "current_movie": "",
        "total_movies_found": 0,
        "total_completed": 0,
        "total_reels_uploaded": 0,
        "last_run": "",
    }
    data = load_json(Config.LOG_FILE, default)
    for k, v in default.items():
        if k not in data:
            data[k] = v
    return data


def save_movies_log(data):
    data["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["total_completed"] = sum(
        1 for m in data["movies"].values() if m["status"] == "completed")
    data["total_movies_found"]   = len(data["movies"])
    data["total_reels_uploaded"] = sum(
        m.get("uploaded_parts", 0) for m in data["movies"].values())
    save_json(Config.LOG_FILE, data)


def sync_with_drive(movies_log, drive_files):
    added = 0
    for f in drive_files:
        name = f["name"]
        if name not in movies_log["movies"]:
            movies_log["movies"][name] = {
                "drive_id":         f["id"],
                "status":           "pending",
                "total_parts":      0,
                "uploaded_parts":   0,
                "size_mb":          round(f["size"] / (1024*1024), 1),
                "started_at":       "",
                "completed_at":     "",
                "last_uploaded_at": "",
                "errors":           0,
            }
            log.info(f"🆕 New movie: {name}")
            added += 1
    if added:
        log.info(f"🆕 {added} new movie(s) added to tracking")
    return movies_log


def get_next_movie(movies_log):
    for name, info in movies_log["movies"].items():
        if info["status"] == "in_progress":
            log.info(f"▶️ Resuming: {name}")
            return name, info
    for name, info in movies_log["movies"].items():
        if info["status"] == "pending":
            log.info(f"🆕 Starting: {name}")
            return name, info
    return None, None


def load_progress():
    return load_json(Config.PROGRESS_FILE,
                     {"movie_name": "", "last_uploaded": 0, "total_parts": 0})


def save_progress(data):
    save_json(Config.PROGRESS_FILE, data)


# ============================================================
#                    SUMMARY
# ============================================================
def print_summary(movies_log):
    log.separator("=")
    print("📊 MOVIES STATUS REPORT")
    log.separator("-")
    emoji_map = {"pending":"⏳","in_progress":"🔄","completed":"✅","error":"❌"}
    for idx, (name, info) in enumerate(movies_log["movies"].items(), 1):
        emoji = emoji_map.get(info["status"], "❓")
        display = movie_display_name(name)
        parts   = f"{info.get('uploaded_parts',0)}/{info.get('total_parts','?')}"
        print(f"  {emoji} #{idx} {display}")
        print(f"      Status: {info['status']} | Parts: {parts} | "
              f"Size: {info.get('size_mb','?')} MB")
        if info.get("started_at"):   print(f"      Started:   {info['started_at']}")
        if info.get("completed_at"): print(f"      Completed: {info['completed_at']}")
        if info.get("errors", 0) > 0: print(f"      Errors:    {info['errors']}")
        print()
    log.separator("-")
    total = len(movies_log["movies"])
    done  = movies_log.get("total_completed", 0)
    reels = movies_log.get("total_reels_uploaded", 0)
    print(f"  📈 Movies: {done}/{total}   📤 Reels: {reels}")
    print(f"  🕐 Last run: {movies_log.get('last_run','N/A')}")
    log.separator("=")


# ============================================================
#                        MAIN
# ============================================================
def main():
    log.separator("=")
    print("🎬 FULLY AUTOMATED INSTAGRAM REEL UPLOADER")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.separator("=")

    # Step 1 — write session from secret
    write_session_from_secret()

    # Step 2 — verify
    if not verify_setup():
        log.error("Setup failed. Add missing secrets!")
        return

    # Step 3 — scan Drive
    drive_files = list_drive_movies()
    if not drive_files:
        log.error("No video files in Google Drive folder!")
        return

    # Step 4 — sync tracker
    movies_log = load_movies_log()
    movies_log = sync_with_drive(movies_log, drive_files)
    save_movies_log(movies_log)

    # Step 5 — next movie
    movie_name, movie_info = get_next_movie(movies_log)
    if not movie_name:
        log.info("🎉 All movies uploaded!")
        print_summary(movies_log)
        return

    display_name = movie_display_name(movie_name)
    log.info(f"🎬 Movie: {display_name}  |  Status: {movie_info['status']}")

    # Step 6 — download
    if not download_movie(movie_info["drive_id"], Config.MOVIE_FILE):
        log.error(f"Download failed. Retry next run.")
        movie_info["errors"] = movie_info.get("errors", 0) + 1
        save_movies_log(movies_log)
        git_push()
        return

    # Step 7 — video info (fast ffprobe)
    duration, total_parts = get_video_info(Config.MOVIE_FILE)
    if total_parts == 0:
        log.error(f"Can't read video. Marking as error.")
        movie_info["status"] = "error"
        save_movies_log(movies_log)
        git_push()
        return

    movie_info["total_parts"] = total_parts
    log.info(f"📏 Duration: {int(duration)//60}m {int(duration)%60}s "
             f"→ {total_parts} parts of {Config.CLIP_LENGTH}s each")

    # Step 8 — mark in_progress
    if movie_info["status"] == "pending":
        movie_info["status"]     = "in_progress"
        movie_info["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    movies_log["current_movie"] = movie_name
    save_movies_log(movies_log)

    # Step 9 — load progress
    progress = load_progress()
    if progress.get("movie_name") != movie_name:
        progress = {"movie_name": movie_name,
                    "last_uploaded": 0, "total_parts": total_parts}
    last_uploaded = progress["last_uploaded"]
    log.info(f"📊 Progress: {last_uploaded}/{total_parts} parts already uploaded")

    # Step 10 — pick thumbnail background (once per movie, cached)
    thumb_bg = None

    if os.path.exists(Config.THUMB_BG_FILE):
        try:
            thumb_bg = Image.open(Config.THUMB_BG_FILE)
            log.info("🎨 Loaded cached thumbnail background")
        except Exception:
            thumb_bg = None

    if thumb_bg is None:
        # Try AI image generation
        thumb_bg = generate_gemini_background(display_name)

        if thumb_bg is None:
            # Use Gemini vision to pick the best video frame
            frames   = extract_frames_for_grid(Config.MOVIE_FILE, duration)
            grid     = create_frame_grid(frames)
            thumb_bg = choose_best_frame_with_gemini(grid, frames)

        try:
            thumb_bg.save(Config.THUMB_BG_FILE, "JPEG", quality=95)
            log.info("💾 Thumbnail background cached")
        except Exception as e:
            log.warn(f"Could not cache thumbnail bg: {e}")

    # Step 11 — Instagram login
    cl = instagram_login()
    if cl is None:
        log.error("Login failed. Check IG_SESSION secret.")
        save_progress(progress)
        save_movies_log(movies_log)
        git_push()
        return

    os.makedirs(Config.REELS_DIR,  exist_ok=True)
    os.makedirs(Config.THUMBS_DIR, exist_ok=True)

    movie_names  = list(movies_log["movies"].keys())
    movie_num    = movie_names.index(movie_name) + 1
    total_movies = len(movie_names)

    # Step 12 — UPLOAD LOOP
    uploaded_this_run = 0
    stop_uploading    = False

    for part_num in range(last_uploaded + 1, total_parts + 1):

        if stop_uploading:
            break
        if uploaded_this_run >= Config.MAX_UPLOADS_PER_RUN:
            log.info(f"🛑 Run limit ({Config.MAX_UPLOADS_PER_RUN}) reached. "
                     "Continuing next scheduled run.")
            break

        log.separator("-")
        log.info(f"📦 Part {part_num}/{total_parts} — {display_name}")

        # Cut clip (fast ffmpeg)
        clip_path = os.path.join(Config.REELS_DIR, f"part_{part_num}.mp4")
        if not extract_clip(Config.MOVIE_FILE, part_num, clip_path):
            log.warn(f"Skipping part {part_num}")
            progress["last_uploaded"] = part_num
            save_progress(progress)
            continue

        # Make thumbnail
        thumb_path = os.path.join(Config.THUMBS_DIR, f"thumb_{part_num}.jpg")
        if thumb_bg:
            bg_image = thumb_bg.copy()
        else:
            mid_t    = ((part_num-1) * Config.CLIP_LENGTH) + (Config.CLIP_LENGTH//2)
            mid_t    = min(mid_t, duration - 1)
            tmp_frame = os.path.join(Config.THUMBS_DIR, f"tmp_frame_{part_num}.jpg")
            bg_image = extract_frame_ffmpeg(Config.MOVIE_FILE, mid_t, tmp_frame)

        create_thumbnail(bg_image, display_name, part_num, total_parts,
                         movie_num, total_movies, thumb_path)

        # Caption
        caption = random.choice(Config.CAPTIONS).format(
            name=display_name, p=part_num, t=total_parts)

        # Upload
        log.info(f"📤 Uploading Part {part_num}/{total_parts}...")
        result = upload_reel(cl, clip_path, thumb_path, caption)

        if result == "STOP":
            log.error("⛔ Fatal Instagram error → stopping")
            log.upload(display_name, part_num, total_parts, "FATAL_STOP")
            stop_uploading = True

        elif result is True:
            log.info(f"✅ Part {part_num}/{total_parts} uploaded!")
            log.upload(display_name, part_num, total_parts, "SUCCESS")
            uploaded_this_run             += 1
            progress["last_uploaded"]      = part_num
            movie_info["uploaded_parts"]   = part_num
            movie_info["last_uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()

        else:
            log.error(f"❌ Part {part_num} failed")
            log.upload(display_name, part_num, total_parts, "FAILED")
            movie_info["errors"] = movie_info.get("errors", 0) + 1
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()
            log.info("⏳ Waiting 10m before continuing...")
            time.sleep(600)
            continue

        # Cleanup
        for f in [clip_path, thumb_path]:
            if os.path.exists(f):
                try: os.remove(f)
                except: pass

        # Wait before next upload
        if (uploaded_this_run < Config.MAX_UPLOADS_PER_RUN
                and part_num < total_parts
                and not stop_uploading):
            smart_delay(uploaded_this_run)

    # Step 13 — movie complete?
    if progress["last_uploaded"] >= total_parts:
        log.separator("*")
        log.info(f"🎉🎉🎉 {display_name} FULLY UPLOADED! 🎉🎉🎉")
        log.separator("*")
        movie_info["status"]         = "completed"
        movie_info["uploaded_parts"] = total_parts
        movie_info["completed_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        movies_log["current_movie"]  = ""
        progress = {"movie_name": "", "last_uploaded": 0, "total_parts": 0}
        cleanup_temp()

    # Step 14 — final save
    save_progress(progress)
    save_movies_log(movies_log)
    git_push()
    print_summary(movies_log)
    log.separator("=")
    log.info(f"✅ Run complete! Uploaded {uploaded_this_run} reels this run.")
    log.separator("=")


# ============================================================
#                      ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warn("Interrupted by user")
        git_push()
    except Exception as e:
        log.error(f"💥 CRITICAL ERROR: {e}")
        log.error(traceback.format_exc())
        git_push()
        sys.exit(1)
