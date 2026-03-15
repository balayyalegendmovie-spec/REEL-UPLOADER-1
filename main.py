"""
============================================================
🎬 FULLY AUTOMATED MULTI-MOVIE INSTAGRAM REEL UPLOADER
============================================================
Just upload movies to Google Drive folder → Everything else
is 100% automated: detect → download → split → thumbnail
(Gemini AI) → upload → track → next movie → repeat

Author: Auto Reel Bot
============================================================
"""

import os
import sys
import json
import time
import random
import shutil
import requests
import traceback
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta

# Video processing
from moviepy.editor import VideoFileClip

# Image / Thumbnail
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
    MAX_UPLOADS_PER_RUN = 3     # 3 uploads x ~2hr gap = ~5hr (within 6hr GitHub limit)
    DELAY_MIN = 7080            # 1 hour 58 minutes (seconds)
    DELAY_MAX = 7380            # 2 hours 3 minutes  (seconds)

    # --- File paths ---
    REELS_DIR      = "reels"
    THUMBS_DIR     = "thumbnails"
    MOVIE_FILE     = "current_movie.mp4"
    SESSION_FILE   = "session.json"
    LOG_FILE       = "movies_log.json"
    PROGRESS_FILE  = "progress.json"
    DETAIL_LOG     = "detailed_log.txt"
    GEMINI_BG      = "gemini_background.png"

    VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".webm")

    # --- Caption templates ---
    CAPTIONS = [
        "🎬 {name} | Part {p}/{t}\n\n#movie #reels #viral #trending #fyp #cinema",
        "🔥 {name} — Part {p}/{t}\n\nFollow for next part! 🍿\n\n#movie #viral #reels",
        "🎥 {name} [{p}/{t}]\n\n⬇️ Follow for more parts!\n\n#movies #cinema #viral #fyp",
        "🍿 {name} | Part {p} of {t}\n\nLike & Follow for more ❤️\n\n#movie #trending #reels",
        "📽️ {name} • Part {p}/{t}\n\nStay tuned! 🔔\n\n#film #reels #viral #trending #fyp",
    ]

    # --- Gemini vision models (pick best frame from grid) ---
    GEMINI_VISION_MODELS = [
        "gemini-2.0-flash",
        "gemini-1.5-flash",
        "gemini-1.5-pro",
    ]

    # --- Gemini image generation models (AI background, optional) ---
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

    def _ts(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _short(self):
        return datetime.now().strftime("%H:%M:%S")

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
#                   HELPER FUNCTIONS
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
    for path in [Config.MOVIE_FILE, Config.GEMINI_BG]:
        if os.path.exists(path):
            os.remove(path)
    for folder in [Config.REELS_DIR, Config.THUMBS_DIR]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
    log.info("🧹 Temporary files cleaned up")


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
#              SETUP VERIFICATION
# ============================================================
def write_session_from_secret():
    """
    If IG_SESSION GitHub Secret is set, write it to session.json on disk.
    Called once at startup before instagram_login().

    The secret value = full raw contents of session.json,
    INCLUDING the opening { and closing } — paste it exactly as-is.
    Secret name in GitHub: IG_SESSION
    """
    session_json = Config.IG_SESSION.strip()
    if not session_json:
        return  # no secret — will rely on a committed session.json file

    # Validate it is real JSON before writing
    try:
        parsed = json.loads(session_json)
    except json.JSONDecodeError as e:
        log.error(f"IG_SESSION secret is not valid JSON: {e}")
        log.error("Paste the FULL contents of session.json including {{ and }}")
        return

    try:
        with open(Config.SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(parsed, f, indent=4, ensure_ascii=False)
        log.info("🔑 session.json written from IG_SESSION secret")
    except IOError as e:
        log.error(f"Failed to write session.json from secret: {e}")


def verify_setup():
    critical_missing = []
    if not Config.IG_USERNAME:
        critical_missing.append("IG_USERNAME")
    if not Config.IG_PASSWORD:
        critical_missing.append("IG_PASSWORD")
    if not Config.GDRIVE_FOLDER_ID:
        critical_missing.append("GDRIVE_FOLDER_ID")
    if not Config.GDRIVE_API_KEY:
        critical_missing.append("GDRIVE_API_KEY")
    if not Config.GEMINI_API_KEY:
        log.warn("GEMINI_API_KEY not set → using video-frame thumbnails")
    if not Config.IG_SESSION and not os.path.exists(Config.SESSION_FILE):
        log.warn("IG_SESSION secret is empty and no session.json found — "
                 "login will fail. Add IG_SESSION to GitHub Secrets.")

    if critical_missing:
        for m in critical_missing:
            log.error(f"Missing required secret: {m}")
        return False

    log.info("✅ All required credentials verified")
    return True


# ============================================================
#                 GOOGLE DRIVE MANAGER
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
                log.error("Drive API: Access denied. Check API key and folder sharing.")
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
        else:
            log.error("Download produced empty or missing file")
            return False

    except Exception as e:
        log.error(f"Download failed: {e}")
        log.error(traceback.format_exc())
        return False


# ============================================================
#                  VIDEO PROCESSOR
# ============================================================
def get_video_info(video_path):
    try:
        video = VideoFileClip(video_path)
        duration = video.duration
        total_parts = 0
        for start in range(0, int(duration), Config.CLIP_LENGTH):
            end = min(start + Config.CLIP_LENGTH, duration)
            if end - start >= 5:
                total_parts += 1
        video.close()
        return duration, total_parts
    except Exception as e:
        log.error(f"Failed to read video: {e}")
        return 0, 0


def extract_clip(video_path, part_num, output_path):
    video = None
    clip  = None
    try:
        video = VideoFileClip(video_path)
        start = (part_num - 1) * Config.CLIP_LENGTH
        end   = min(start + Config.CLIP_LENGTH, video.duration)

        if end - start < 5:
            log.warn(f"Part {part_num}: too short ({end - start:.1f}s), skipping")
            return False

        clip = video.subclip(start, end)
        clip.write_videofile(output_path, codec="libx264",
                             audio_codec="aac", threads=2, logger=None)
        log.info(f"✂️ Part {part_num} extracted ({start}s → {end:.0f}s)")
        return True

    except Exception as e:
        log.error(f"Failed to extract part {part_num}: {e}")
        return False
    finally:
        if clip:
            try: clip.close()
            except: pass
        if video:
            try: video.close()
            except: pass


def extract_frame(video_path, time_sec):
    video = None
    try:
        video    = VideoFileClip(video_path)
        time_sec = max(0, min(time_sec, video.duration - 0.5))
        return Image.fromarray(video.get_frame(time_sec))
    except Exception as e:
        log.error(f"Frame extraction failed: {e}")
        return Image.new("RGB", (1920, 1080), (20, 20, 40))
    finally:
        if video:
            try: video.close()
            except: pass


def extract_frames_for_grid(video_path, frame_count=9):
    """Extract evenly-spaced frames across the movie for the selection grid"""
    video    = VideoFileClip(video_path)
    duration = video.duration
    frames   = []
    for i in range(frame_count):
        t = duration * (0.15 + i * 0.07)
        t = min(t, duration - 0.5)
        frames.append(Image.fromarray(video.get_frame(t)))
    video.close()
    return frames


def create_frame_grid(frames, tile_size=320):
    """Arrange 9 frames into a 3x3 grid"""
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
    Send the 3x3 frame grid to Gemini vision and ask it to
    pick the most attractive thumbnail frame (1-9).

    IMPORTANT: Uses the correct google-genai SDK v1+ format:
      - genai_types.Part.from_bytes(data=..., mime_type=...)
      - genai_types.Part.from_text(text=...)
    NOT raw dicts — those cause pydantic validation errors.
    """
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        log.warn("Gemini not available → using middle frame")
        return frames[4]

    # Convert grid image to JPEG bytes
    buf = BytesIO()
    grid_image.save(buf, format="JPEG", quality=85)
    image_bytes = buf.getvalue()

    prompt_text = (
        "You are selecting the best movie thumbnail frame.\n"
        "The image shows a 3x3 grid of frames numbered:\n"
        "  1 2 3\n"
        "  4 5 6\n"
        "  7 8 9\n\n"
        "Pick the frame that:\n"
        "- Has the most interesting characters or action\n"
        "- Is bright and clearly visible\n"
        "- Would attract the most viewers as a thumbnail\n\n"
        "Reply with ONLY a single digit 1 through 9. Nothing else."
    )

    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini client init failed: {e} → using middle frame")
        return frames[4]

    for model_name in Config.GEMINI_VISION_MODELS:
        try:
            log.info(f"🤖 Asking Gemini ({model_name}) to pick best frame...")

            # ✅ CORRECT FORMAT for google-genai SDK v1+
            # Use Part.from_bytes() and Part.from_text() — NOT raw dicts
            image_part = genai_types.Part.from_bytes(
                data=image_bytes,
                mime_type="image/jpeg",
            )
            text_part = genai_types.Part.from_text(text=prompt_text)

            response = client.models.generate_content(
                model=model_name,
                contents=[image_part, text_part],
            )

            raw   = response.text.strip()
            digit = next((c for c in raw if c.isdigit() and c != "0"), None)
            if digit and 1 <= int(digit) <= 9:
                chosen = int(digit)
                log.info(f"🤖 Gemini picked frame #{chosen}")
                return frames[chosen - 1]
            else:
                log.warn(f"Gemini returned unexpected: '{raw}' → trying next model")

        except Exception as e:
            log.warn(f"Gemini vision model {model_name} failed: {e}")
            continue

    log.warn("All Gemini vision models failed → using middle frame")
    return frames[4]


# ============================================================
#       GEMINI IMAGE GENERATION — optional AI background
# ============================================================
def generate_gemini_background(movie_name):
    """
    Try Gemini image generation for a cinematic poster background.
    Returns PIL Image or None (gracefully falls back).
    Requires a Gemini API key with image generation enabled.
    """
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        return None

    prompt = (
        f"Cinematic movie poster background for '{movie_name}'. "
        "Dark moody atmosphere, dramatic lighting, professional quality. "
        "NO text, NO letters, NO words in the image."
    )

    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini client init failed: {e}")
        return None

    for model_name in Config.GEMINI_IMAGE_MODELS:
        try:
            log.info(f"🎨 Trying Gemini image generation ({model_name})...")
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
                        log.info(f"🎨 AI background generated with {model_name}!")
                        return image
            log.warn(f"{model_name}: no image in response → trying next")

        except Exception as e:
            log.warn(f"Gemini image model {model_name} failed: {e}")
            continue

    log.warn("All Gemini image models failed → using best video frame as background")
    return None


# ============================================================
#            THUMBNAIL COMPOSER  (Pillow)
# ============================================================
def get_font(size):
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for fp in font_paths:
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

        # Gradient overlays (top + bottom)
        overlay = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
        odraw   = ImageDraw.Draw(overlay)
        for y in range(500):
            alpha = int(220 * (1 - y / 500))
            odraw.rectangle([(0, y), (1080, y + 1)], fill=(0, 0, 0, alpha))
        for y in range(1420, 1920):
            alpha = int(220 * ((y - 1420) / 500))
            odraw.rectangle([(0, y), (1080, y + 1)], fill=(0, 0, 0, alpha))

        thumb = Image.alpha_composite(thumb, overlay).convert("RGB")
        draw  = ImageDraw.Draw(thumb)

        font_title = get_font(68)
        font_part  = get_font(56)
        font_info  = get_font(36)

        # Movie title top (word-wrapped)
        title     = movie_name.upper()
        max_chars = 18
        if len(title) > max_chars:
            words, lines, line = title.split(), [], ""
            for w in words:
                test = (line + " " + w).strip()
                if len(test) > max_chars and line:
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
                    draw.text((x + dx, y_cur + dy), line, font=font_title, fill="black")
            draw.text((x, y_cur), line, font=font_title, fill="white")
            y_cur += bbox[3] - bbox[1] + 15

        # Part number (gold, bottom)
        part_text = f"PART {part_num} / {total_parts}"
        bbox = draw.textbbox((0, 0), part_text, font=font_part)
        tw   = bbox[2] - bbox[0]
        x    = (1080 - tw) // 2
        for dx in range(-2, 3):
            for dy in range(-2, 3):
                draw.text((x + dx, 1740 + dy), part_text, font=font_part, fill="black")
        draw.text((x, 1740), part_text, font=font_part, fill=(255, 215, 0))

        # Movie counter (grey)
        counter_text = f"Movie {movie_num} of {total_movies}"
        bbox = draw.textbbox((0, 0), counter_text, font=font_info)
        tw   = bbox[2] - bbox[0]
        draw.text(((1080 - tw) // 2, 1810), counter_text,
                  font=font_info, fill=(180, 180, 180))

        # Gold decorative line
        draw.rectangle([(200, 1720), (880, 1723)], fill=(255, 215, 0))

        thumb.save(output_path, "JPEG", quality=95)
        log.info(f"🖼️ Thumbnail created: {output_path}")
        return True

    except Exception as e:
        log.error(f"Thumbnail creation failed: {e}")
        log.error(traceback.format_exc())
        try:
            fb  = Image.new("RGB", (1080, 1920), (20, 20, 40))
            d   = ImageDraw.Draw(fb)
            fnt = get_font(60)
            d.text((100, 800), movie_name,                       font=fnt, fill="white")
            d.text((100, 900), f"Part {part_num}/{total_parts}", font=fnt, fill=(255, 215, 0))
            fb.save(output_path, "JPEG")
            return True
        except Exception:
            return False


# ============================================================
#                 INSTAGRAM MANAGER
# ============================================================
def instagram_login():
    """
    Login using a pre-generated session.json ONLY.

    Why: Instagram flags fresh logins from GitHub Actions servers as
    suspicious (different IP every run) → sends OTP, locks account.

    Solution: Generate session.json ONCE on your local PC using
    generate_session.py, then commit it to the repo. GitHub Actions
    reuses the session without triggering security checks.
    """
    if not os.path.exists(Config.SESSION_FILE):
        log.error("❌ session.json not found!")
        log.error(
            "\n"
            "=========================================================\n"
            "  ACTION REQUIRED: Generate session.json on your local PC\n"
            "=========================================================\n"
            "  1. Run:   python generate_session.py\n"
            "  2. Login with your Instagram username + password\n"
            "  3. Complete any OTP/challenge Instagram sends\n"
            "  4. Commit the generated session.json to your GitHub repo\n"
            "  5. Re-run the workflow\n"
            "=========================================================\n"
        )
        return None

    for attempt in range(1, 4):
        try:
            log.info(f"🔐 Login attempt {attempt}/3 using saved session...")
            cl = Client()
            cl.delay_range = [2, 5]
            cl.load_settings(Config.SESSION_FILE)
            cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
            cl.get_timeline_feed()    # verify session is alive
            cl.dump_settings(Config.SESSION_FILE)  # refresh + save
            log.info("🔐 Logged in via session ✅")
            return cl

        except ChallengeRequired:
            log.error("⛔ Instagram challenge required.")
            log.error("Run generate_session.py on your local PC again, "
                      "complete the challenge, then re-commit session.json")
            return None

        except Exception as e:
            log.warn(f"Session login attempt {attempt} failed: {e}")
            if attempt < 3:
                time.sleep(30 * attempt)

    log.error("All session login attempts failed.")
    log.error("Delete session.json, run generate_session.py locally, "
              "then re-commit.")
    return None


def upload_reel(cl, video_path, thumbnail_path, caption):
    """Upload a single reel. Returns True / False / 'STOP'"""
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
            log.warn(f"Rate limited → waiting {wait // 60} minutes...")
            time.sleep(wait)

        except ClientThrottledError:
            wait = 900 * retry
            log.warn(f"Throttled → waiting {wait // 60} minutes...")
            time.sleep(wait)

        except FeedbackRequired as e:
            log.error(f"Instagram feedback required: {e}")
            return "STOP"

        except ChallengeRequired:
            log.error("⛔ Instagram challenge. Regenerate session.json locally.")
            return "STOP"

        except LoginRequired:
            log.warn("Session expired mid-upload → attempting re-login...")
            try:
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                cl.dump_settings(Config.SESSION_FILE)
                log.info("Re-login successful")
            except Exception:
                log.error("Re-login failed")
                return "STOP"

        except ConnectionError:
            wait = 180 * retry
            log.warn(f"Connection error → waiting {wait // 60} minutes...")
            time.sleep(wait)

        except Exception as e:
            log.error(f"Upload error (attempt {retry}): {e}")
            log.error(traceback.format_exc())
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

    next_time = datetime.now() + timedelta(seconds=total)
    log.info(f"⏳ Delay: {total // 60}m {total % 60}s  "
             f"(next upload ≈ {next_time.strftime('%H:%M:%S')})")

    elapsed = 0
    while elapsed < total:
        chunk    = min(60, total - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        remaining = total - elapsed
        if remaining > 0 and elapsed % 600 < 60:
            log.info(f"  ⏳ {remaining // 60}m {remaining % 60}s remaining...")


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
        1 for m in data["movies"].values() if m["status"] == "completed"
    )
    data["total_movies_found"]   = len(data["movies"])
    data["total_reels_uploaded"] = sum(
        m.get("uploaded_parts", 0) for m in data["movies"].values()
    )
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
                "size_mb":          round(f["size"] / (1024 * 1024), 1),
                "started_at":       "",
                "completed_at":     "",
                "last_uploaded_at": "",
                "errors":           0,
            }
            log.info(f"🆕 New movie detected: {name}")
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
            log.info(f"🆕 Starting new: {name}")
            return name, info
    return None, None


def load_progress():
    return load_json(Config.PROGRESS_FILE, {
        "movie_name": "", "last_uploaded": 0, "total_parts": 0,
    })


def save_progress(data):
    save_json(Config.PROGRESS_FILE, data)


# ============================================================
#                    SUMMARY REPORT
# ============================================================
def print_summary(movies_log):
    log.separator("=")
    print("📊 MOVIES STATUS REPORT")
    log.separator("-")

    status_emoji = {
        "pending": "⏳", "in_progress": "🔄",
        "completed": "✅", "error": "❌",
    }

    for idx, (name, info) in enumerate(movies_log["movies"].items(), 1):
        emoji   = status_emoji.get(info["status"], "❓")
        display = movie_display_name(name)
        parts   = f"{info.get('uploaded_parts', 0)}/{info.get('total_parts', '?')}"
        size    = f"{info.get('size_mb', '?')} MB"
        print(f"  {emoji} #{idx} {display}")
        print(f"      Status: {info['status']} | Parts: {parts} | Size: {size}")
        if info.get("started_at"):
            print(f"      Started:   {info['started_at']}")
        if info.get("completed_at"):
            print(f"      Completed: {info['completed_at']}")
        if info.get("errors", 0) > 0:
            print(f"      Errors:    {info['errors']}")
        print()

    log.separator("-")
    total = len(movies_log["movies"])
    done  = movies_log.get("total_completed", 0)
    reels = movies_log.get("total_reels_uploaded", 0)
    print(f"  📈 Movies:         {done}/{total} completed")
    print(f"  📤 Reels uploaded: {reels}")
    print(f"  🕐 Last run:       {movies_log.get('last_run', 'N/A')}")
    log.separator("=")


# ============================================================
#                        MAIN
# ============================================================
def main():
    log.separator("=")
    print("🎬 FULLY AUTOMATED INSTAGRAM REEL UPLOADER")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.separator("=")

    # Write session.json from IG_SESSION GitHub Secret (if set)
    write_session_from_secret()

    if not verify_setup():
        log.error("Setup verification failed. Add missing secrets!")
        return

    drive_files = list_drive_movies()
    if not drive_files:
        log.error("No video files found in Google Drive folder!")
        return

    movies_log = load_movies_log()
    movies_log = sync_with_drive(movies_log, drive_files)
    save_movies_log(movies_log)

    movie_name, movie_info = get_next_movie(movies_log)
    if not movie_name:
        log.info("🎉 ALL MOVIES UPLOADED! Nothing to do.")
        print_summary(movies_log)
        return

    display_name = movie_display_name(movie_name)
    log.info(f"🎬 Current movie: {display_name}")
    log.info(f"📌 Status: {movie_info['status']}")

    if not download_movie(movie_info["drive_id"], Config.MOVIE_FILE):
        log.error(f"Failed to download '{display_name}'. Will retry next run.")
        movie_info["errors"] = movie_info.get("errors", 0) + 1
        save_movies_log(movies_log)
        git_push()
        return

    duration, total_parts = get_video_info(Config.MOVIE_FILE)
    if total_parts == 0:
        log.error(f"Could not read video '{display_name}'. Marking as error.")
        movie_info["status"] = "error"
        save_movies_log(movies_log)
        git_push()
        return

    movie_info["total_parts"] = total_parts
    log.info(f"📏 Duration: {int(duration) // 60}m {int(duration) % 60}s "
             f"→ {total_parts} parts")

    if movie_info["status"] == "pending":
        movie_info["status"]     = "in_progress"
        movie_info["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    movies_log["current_movie"] = movie_name
    save_movies_log(movies_log)

    progress = load_progress()
    if progress.get("movie_name") != movie_name:
        progress = {"movie_name": movie_name,
                    "last_uploaded": 0, "total_parts": total_parts}
    last_uploaded = progress["last_uploaded"]
    log.info(f"📊 Upload progress: {last_uploaded}/{total_parts}")

    # ── Pick thumbnail background ────────────────────────────
    gemini_bg = None

    if os.path.exists(Config.GEMINI_BG):
        try:
            gemini_bg = Image.open(Config.GEMINI_BG)
            log.info("🎨 Loaded cached thumbnail background")
        except Exception:
            gemini_bg = None

    if gemini_bg is None:
        gemini_bg = generate_gemini_background(display_name)

        if gemini_bg is None:
            log.info("🎬 Extracting frames for Gemini thumbnail selection...")
            frames    = extract_frames_for_grid(Config.MOVIE_FILE)
            grid      = create_frame_grid(frames)
            gemini_bg = choose_best_frame_with_gemini(grid, frames)

        try:
            gemini_bg.save(Config.GEMINI_BG)
            log.info("💾 Thumbnail background cached")
        except Exception as e:
            log.warn(f"Could not cache background: {e}")

    # ── Login ────────────────────────────────────────────────
    cl = instagram_login()
    if cl is None:
        log.error("Cannot login. Generate session.json locally and commit it!")
        save_progress(progress)
        save_movies_log(movies_log)
        git_push()
        return

    os.makedirs(Config.REELS_DIR,  exist_ok=True)
    os.makedirs(Config.THUMBS_DIR, exist_ok=True)

    movie_names  = list(movies_log["movies"].keys())
    movie_num    = movie_names.index(movie_name) + 1
    total_movies = len(movie_names)

    # ── Upload loop ──────────────────────────────────────────
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
        log.info(f"📦 Processing Part {part_num}/{total_parts} — '{display_name}'")

        clip_path = os.path.join(Config.REELS_DIR, f"part_{part_num}.mp4")
        if not extract_clip(Config.MOVIE_FILE, part_num, clip_path):
            log.warn(f"Skipping part {part_num}")
            progress["last_uploaded"] = part_num
            save_progress(progress)
            continue

        thumb_path = os.path.join(Config.THUMBS_DIR, f"thumb_{part_num}.jpg")
        if gemini_bg:
            bg_image = gemini_bg.copy()
        else:
            mid_time = ((part_num - 1) * Config.CLIP_LENGTH) + (Config.CLIP_LENGTH // 2)
            mid_time = min(mid_time, duration - 1)
            bg_image = extract_frame(Config.MOVIE_FILE, mid_time)

        create_thumbnail(bg_image, display_name, part_num, total_parts,
                         movie_num, total_movies, thumb_path)

        caption = random.choice(Config.CAPTIONS).format(
            name=display_name, p=part_num, t=total_parts
        )

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
            log.error(f"❌ Part {part_num} failed after all retries")
            log.upload(display_name, part_num, total_parts, "FAILED")
            movie_info["errors"] = movie_info.get("errors", 0) + 1
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()
            log.info("⏳ Waiting 10 minutes before continuing...")
            time.sleep(600)
            continue

        for f in [clip_path, thumb_path]:
            if os.path.exists(f):
                try: os.remove(f)
                except: pass

        if (uploaded_this_run < Config.MAX_UPLOADS_PER_RUN
                and part_num < total_parts
                and not stop_uploading):
            smart_delay(uploaded_this_run)

    if progress["last_uploaded"] >= total_parts:
        log.separator("*")
        log.info(f"🎉🎉🎉 Movie '{display_name}' FULLY UPLOADED! 🎉🎉🎉")
        log.separator("*")
        movie_info["status"]         = "completed"
        movie_info["uploaded_parts"] = total_parts
        movie_info["completed_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        movies_log["current_movie"]  = ""
        progress = {"movie_name": "", "last_uploaded": 0, "total_parts": 0}
        cleanup_temp()

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
        log.error(f"💥 CRITICAL UNHANDLED ERROR: {e}")
        log.error(traceback.format_exc())
        git_push()
        sys.exit(1)
