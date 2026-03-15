"""
============================================================
🎬 FULLY AUTOMATED MULTI-MOVIE INSTAGRAM + YOUTUBE UPLOADER
============================================================
FLOW (printed live while running):
  STARTUP    → write sessions from secrets → verify credentials
  DRIVE      → scan folder → sort episodes correctly (1,2,3...52)
  DOWNLOAD   → download current episode/movie from Google Drive
  VIDEO      → ffprobe duration → calculate 59s parts count
  THUMBNAIL  → extract 9 frames (ffmpeg) → Gemini picks best
  LOGIN      → Instagram session + YouTube OAuth token (no OTP)
  UPLOAD     → cut clip → make thumbnail
               → upload to Instagram Reels
               → upload to YouTube Shorts
  DONE       → save progress → push to GitHub → print summary

SUPPORTS ALL FILENAME FORMATS:
  Doraemon_S16_–_Episode_1_–_A_story_of_...mkv   → "Doraemon S16 Ep.1"
  Doraemon_S16_–_Episode_52_–_Some_name.mkv       → "Doraemon S16 Ep.52"
  Inception_2010_Part1.mp4                         → "Inception 2010"
  Avatar_Full_Movie.mp4                            → "Avatar Full Movie"

EPISODE SORT ORDER:
  Drive API sorts alphabetically (Episode_10 before Episode_2).
  This code sorts by Season + Episode NUMBER so order is always correct.

SESSIONS / TOKENS (generate locally once, store as GitHub Secrets):
  IG_SESSION       → full contents of session.json  (Instagram)
  YT_TOKEN         → full contents of yt_token.json (YouTube OAuth2)
  YT_CLIENT_ID     → from Google Cloud Console OAuth2 credentials
  YT_CLIENT_SECRET → from Google Cloud Console OAuth2 credentials
============================================================
"""

import os
import re
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

os.environ["PYTHONUNBUFFERED"] = "1"

def flush_print(msg=""):
    print(msg, flush=True)

from PIL import Image, ImageDraw, ImageFont
from instagrapi import Client
from instagrapi.exceptions import (
    LoginRequired, ChallengeRequired, FeedbackRequired,
    PleaseWaitFewMinutes, ClientThrottledError,
)
import gdown

YT_AVAILABLE = False
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from googleapiclient.discovery import build as yt_build
    from googleapiclient.http import MediaFileUpload
    YT_AVAILABLE = True
except ImportError:
    flush_print("⚠️ google-api-python-client not installed → YouTube disabled")

GEMINI_AVAILABLE = False
try:
    from google import genai
    from google.genai import types as genai_types
    GEMINI_AVAILABLE = True
except ImportError:
    flush_print("⚠️ google-genai not installed → video-frame thumbnails only")


# ============================================================
#                    CONFIGURATION
# ============================================================
class Config:
    IG_USERNAME      = os.environ.get("IG_USERNAME", "")
    IG_PASSWORD      = os.environ.get("IG_PASSWORD", "")
    IG_SESSION       = os.environ.get("IG_SESSION", "")
    YT_TOKEN         = os.environ.get("YT_TOKEN", "")
    YT_CLIENT_ID     = os.environ.get("YT_CLIENT_ID", "")
    YT_CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET", "")
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "")
    GDRIVE_API_KEY   = os.environ.get("GDRIVE_API_KEY", "")
    GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")

    # 59s so clips qualify as YouTube Shorts (≤60s) AND Instagram Reels
    CLIP_LENGTH          = 59
    MAX_UPLOADS_PER_RUN  = 1       # 1 per run × 12 cron runs/day = 12/day

    UPLOAD_TO_INSTAGRAM  = True
    UPLOAD_TO_YOUTUBE    = True

    REELS_DIR       = "reels"
    THUMBS_DIR      = "thumbnails"
    MOVIE_FILE      = "current_movie.mp4"
    IG_SESSION_FILE = "session.json"
    YT_TOKEN_FILE   = "yt_token.json"
    LOG_FILE        = "movies_log.json"
    PROGRESS_FILE   = "progress.json"
    DETAIL_LOG      = "detailed_log.txt"
    # Thumbnail bg is now per-movie: thumb_bg_{movie_name_hash}.jpg
    # so Episode 2 never reuses Episode 1's background
    THUMB_BG_DIR    = "thumb_cache"

    VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".mov", ".webm")

    IG_CAPTIONS = [
        "🎬 {name} | Part {p}/{t}\n\n#movie #reels #viral #trending #fyp #cinema",
        "🔥 {name} — Part {p}/{t}\n\nFollow for next part! 🍿\n\n#movie #viral #reels",
        "🎥 {name} [{p}/{t}]\n\n⬇️ Follow for more parts!\n\n#movies #cinema #viral #fyp",
        "🍿 {name} | Part {p} of {t}\n\nLike & Follow for more ❤️\n\n#movie #trending #reels",
        "📽️ {name} • Part {p}/{t}\n\nStay tuned! 🔔\n\n#film #reels #viral #trending #fyp",
    ]
    YT_TITLES = [
        "{name} | Part {p}/{t} #Shorts",
        "{name} — Part {p} of {t} #Shorts",
        "🎬 {name} Part {p}/{t} #Shorts #Movie",
    ]
    YT_DESCRIPTION = (
        "{name} | Part {p} of {t}\n\n"
        "Watch the full movie in parts on this channel!\n"
        "Like & Subscribe for more parts 🔔\n\n"
        "#shorts #movie #viral #trending #cinema"
    )
    YT_CATEGORY_ID = "1"   # 1=Film & Animation
    YT_PRIVACY     = "public"

    GEMINI_VISION_MODELS = ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"]
    GEMINI_IMAGE_MODELS  = ["gemini-2.0-flash-exp-image-generation",
                            "imagen-3.0-generate-002"]


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
        print(f"[{self._short()}] ✅ {msg}", flush=True)
        self._write(f"[{self._ts()}] INFO  | {msg}")

    def warn(self, msg):
        print(f"[{self._short()}] ⚠️  {msg}", flush=True)
        self._write(f"[{self._ts()}] WARN  | {msg}")

    def error(self, msg):
        print(f"[{self._short()}] ❌ {msg}", flush=True)
        self._write(f"[{self._ts()}] ERROR | {msg}")

    def step(self, num, total, msg):
        print(f"\n[{self._short()}] ━━━ STEP {num}/{total}: {msg} ━━━", flush=True)
        self._write(f"[{self._ts()}] STEP  | {num}/{total}: {msg}")

    def upload(self, platform, movie, part, total, status):
        line = f"[{self._ts()}] UPLOAD|{platform}|{movie}|Part {part}/{total}|{status}"
        print(f"  📤 [{platform}] {movie} Part {part}/{total} → {status}", flush=True)
        self._write(line)

    def separator(self, char="=", length=60):
        sep = char * length
        print(sep, flush=True)
        self._write(sep)


log = Logger(Config.DETAIL_LOG)


# ============================================================
#       EPISODE / FILENAME PARSER  (the key new logic)
# ============================================================
def parse_episode_info(filename):
    """
    WHAT: Extracts Season, Episode, and Part numbers from filenames.
    WHY:  Needed for correct sort order AND clean display names.

    Handles all these formats:
      Doraemon_S16_–_Episode_1_–_Story_name_–_Tam+Tel+Hin.mkv
      Doraemon_S16_–_Episode_52_–_Name.mkv
      Inception_2010_Part1.mp4
      Avatar_Full_Movie.mp4

    Returns dict with:
      display_name  → clean human-readable name for captions/thumbnails
      season        → int or None
      episode       → int or None
      file_part     → int or None (for filenames with Part1/Part2)
      sort_key      → tuple for correct numerical sort
    """
    stem = Path(filename).stem

    # Extract season number (S16, S1, Season_2, etc.)
    season_match = re.search(r'[Ss](?:eason[_\s]?)?(\d+)', stem)
    season = int(season_match.group(1)) if season_match else None

    # Extract episode number (Episode_1, Episode_52, Ep1, E01, etc.)
    ep_match = re.search(r'[Ee]p(?:isode)?[_\s\-–]*(\d+)', stem)
    episode = int(ep_match.group(1)) if ep_match else None

    # Extract file part number (Part1, Part_2, part01)
    part_match = re.search(r'[Pp]art[_\s]?(\d+)', stem)
    file_part = int(part_match.group(1)) if part_match else None

    # ── Build clean display name ──────────────────────────────
    # 1. Replace underscores and dashes with spaces
    clean = re.sub(r'[_]+', ' ', stem)
    clean = re.sub(r'\s*[–—-]\s*', ' – ', clean)

    # 2. Remove language tags at end: Tam, Tel, Hin, Eng, Sub, Dub
    #    and everything after them (often joined with + signs)
    clean = re.sub(
        r'\s*[–—-]?\s*(Tam|Tel|Hin|Eng|Sub|Dub)(\+(?:Tam|Tel|Hin|Eng|Sub|Dub))*\s*$',
        '', clean, flags=re.IGNORECASE
    )

    # 3. Shorten very long names for thumbnail/caption readability
    #    Keep: Series name + Episode number + Episode title (max 40 chars total)
    if episode is not None:
        # Extract the series name (everything before "Episode")
        series_match = re.match(r'^(.*?)\s*[–—-]?\s*[Ee]pisode', clean)
        series_name  = series_match.group(1).strip() if series_match else clean

        # Extract episode title (everything after "Episode N –")
        title_match = re.search(
            r'[Ee]pisode\s+\d+\s*[–—-]\s*(.+)$', clean)
        ep_title = title_match.group(1).strip() if title_match else ""

        # Build: "Series Ep.N – Title" (truncate title if too long)
        if ep_title:
            short_title = ep_title[:30] + "..." if len(ep_title) > 30 else ep_title
            display_name = f"{series_name} Ep.{episode} – {short_title}"
        else:
            display_name = f"{series_name} Ep.{episode}"
    else:
        display_name = clean.strip()

    # 4. Final cleanup: collapse multiple spaces
    display_name = re.sub(r'\s{2,}', ' ', display_name).strip()

    # ── Build sort key (so Episode 10 comes AFTER Episode 9) ──
    # Sort by (season, episode, file_part, filename)
    sort_key = (
        season   if season   is not None else 9999,
        episode  if episode  is not None else 9999,
        file_part if file_part is not None else 0,
        filename,
    )

    return {
        "display_name": display_name,
        "season":       season,
        "episode":      episode,
        "file_part":    file_part,
        "sort_key":     sort_key,
    }


def thumb_bg_path_for_movie(movie_name):
    """
    WHAT: Returns a unique thumbnail background cache path per movie.
    WHY:  Without this, Episode 2 would reuse Episode 1's background
          because they shared the same 'thumb_background.jpg' file.
    HOW:  Uses a short hash of the movie filename as the cache filename.
    """
    os.makedirs(Config.THUMB_BG_DIR, exist_ok=True)
    # Simple hash: take last 12 chars of hex digest
    import hashlib
    h = hashlib.md5(movie_name.encode()).hexdigest()[:12]
    return os.path.join(Config.THUMB_BG_DIR, f"thumb_bg_{h}.jpg")


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
    """Returns the clean display name for a movie filename."""
    return parse_episode_info(filename)["display_name"]


def cleanup_temp(movie_name=None):
    """
    Delete temp files after a movie completes.
    Keeps thumb_cache/ for other episodes — only removes this movie's bg.
    """
    log.info("🧹 Cleaning up temp files...")
    for path in [Config.MOVIE_FILE, Config.IG_SESSION_FILE, Config.YT_TOKEN_FILE]:
        if os.path.exists(path):
            os.remove(path)
            log.info(f"   Deleted: {path}")

    # Delete this specific movie's thumbnail background cache
    if movie_name:
        bg = thumb_bg_path_for_movie(movie_name)
        if os.path.exists(bg):
            os.remove(bg)
            log.info(f"   Deleted: {bg}")

    for folder in [Config.REELS_DIR, Config.THUMBS_DIR]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            log.info(f"   Deleted folder: {folder}")
    log.info("🧹 Cleanup done")


def _ensure_gitignore():
    gitignore = ".gitignore"
    entries   = [
        "# Auth tokens + large temp files — never commit these!",
        "session.json",
        "yt_token.json",
        "current_movie.mp4",
        "reels/",
        "thumbnails/",
        "tmp_frames/",
        "thumb_cache/",
    ]
    try:
        existing = ""
        if os.path.exists(gitignore):
            with open(gitignore, "r") as f:
                existing = f.read()
        new_entries = [e for e in entries if e not in existing]
        if new_entries:
            with open(gitignore, "a") as f:
                f.write("\n" + "\n".join(new_entries) + "\n")
            os.system(f'git add "{gitignore}"')
            log.info("🔒 .gitignore updated")
    except Exception as e:
        log.warn(f"Could not update .gitignore: {e}")


def git_push():
    log.info("📁 Pushing progress to GitHub...")
    try:
        os.system('git config user.name "Reel Bot"')
        os.system('git config user.email "bot@reelbot.com"')
        _ensure_gitignore()
        for f in [Config.LOG_FILE, Config.PROGRESS_FILE, Config.DETAIL_LOG]:
            if os.path.exists(f):
                os.system(f'git add "{f}"')
                log.info(f"   Staged: {f}")
        os.system('git diff --staged --quiet || git commit -m "🤖 Auto: progress update"')
        os.system('git push')
        log.info("📁 GitHub push complete")
    except Exception as e:
        log.error(f"Git push failed: {e}")


# ============================================================
#         STEP 1 — WRITE SESSIONS FROM SECRETS
# ============================================================
def write_session_from_secret():
    log.step(1, 10, "Write auth tokens from GitHub Secrets to disk")

    for secret_value, filepath, name in [
        (Config.IG_SESSION, Config.IG_SESSION_FILE, "IG_SESSION → session.json"),
        (Config.YT_TOKEN,   Config.YT_TOKEN_FILE,   "YT_TOKEN → yt_token.json"),
    ]:
        val = secret_value.strip()
        if val:
            log.info(f"{name} secret found — writing...")
            try:
                parsed = json.loads(val)
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(parsed, f, indent=4)
                log.info(f"🔑 {filepath} written (NOT committed to git)")
            except json.JSONDecodeError as e:
                log.error(f"{name} secret is not valid JSON: {e}")
                log.error("Paste FULL contents including {{ and }}")
            except IOError as e:
                log.error(f"Could not write {filepath}: {e}")
        else:
            if os.path.exists(filepath):
                log.info(f"{name} secret empty — using existing {filepath}")
            else:
                log.warn(f"{name} secret empty and {filepath} not found")


# ============================================================
#         STEP 2 — VERIFY SETUP
# ============================================================
def verify_setup():
    log.step(2, 10, "Verify all required GitHub Secrets")
    critical_missing = []
    for value, name, desc in [
        (Config.IG_USERNAME,      "IG_USERNAME",      "Instagram username"),
        (Config.IG_PASSWORD,      "IG_PASSWORD",      "Instagram password"),
        (Config.GDRIVE_FOLDER_ID, "GDRIVE_FOLDER_ID", "Google Drive folder ID"),
        (Config.GDRIVE_API_KEY,   "GDRIVE_API_KEY",   "Google Drive API key"),
    ]:
        if value:
            log.info(f"   ✓ {name}")
        else:
            log.error(f"   ✗ {name} MISSING — {desc}")
            critical_missing.append(name)

    log.info(f"   {'✓' if Config.GEMINI_API_KEY else '~'} GEMINI_API_KEY "
             f"{'(AI thumbnails)' if Config.GEMINI_API_KEY else '(not set → video frames)'}")
    log.info(f"   {'✓' if os.path.exists(Config.IG_SESSION_FILE) else '~'} "
             f"Instagram session {'ready' if os.path.exists(Config.IG_SESSION_FILE) else 'MISSING'}")
    log.info(f"   {'✓' if os.path.exists(Config.YT_TOKEN_FILE) else '~'} "
             f"YouTube token {'ready' if os.path.exists(Config.YT_TOKEN_FILE) else 'not set → YT skipped'}")

    if critical_missing:
        log.error(f"STOPPING: missing secrets: {', '.join(critical_missing)}")
        return False
    log.info("✅ All required secrets verified")
    return True


# ============================================================
#         STEP 3 — GOOGLE DRIVE SCAN (with correct sort)
# ============================================================
def list_drive_movies():
    """
    WHAT: List all video files in the Drive folder.
    SORT: By Season + Episode NUMBER (not alphabetically).
          Drive API orderBy=name gives WRONG order for multi-digit episodes.
          Episode_10 comes before Episode_2 alphabetically.
          We fix this by re-sorting after fetching.
    """
    log.step(3, 10, "Scan Google Drive folder for video files")
    folder_id  = Config.GDRIVE_FOLDER_ID
    api_key    = Config.GDRIVE_API_KEY
    url        = "https://www.googleapis.com/drive/v3/files"
    all_files  = []
    page_token = None

    log.info(f"Calling Drive API — folder: {folder_id}")

    while True:
        params = {
            "q":       f"'{folder_id}' in parents and trashed=false",
            "key":     api_key,
            "fields":  "nextPageToken,files(id,name,size,mimeType,createdTime)",
            "pageSize": 100,
            # NOTE: we intentionally do NOT use orderBy here because
            # alphabetical order is wrong for multi-digit episode numbers.
            # We sort correctly ourselves after fetching.
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            r = requests.get(url, params=params, timeout=30)
            log.info(f"Drive API: HTTP {r.status_code}")

            if r.status_code == 403:
                log.error("403 Access denied — share folder as 'Anyone with link' → Viewer")
                return []
            if r.status_code == 404:
                log.error("404 Folder not found — check GDRIVE_FOLDER_ID")
                return []
            if r.status_code != 200:
                log.error(f"Drive API error {r.status_code}: {r.text[:300]}")
                return []

            data = r.json()
            for f in data.get("files", []):
                name = f["name"]
                if any(name.lower().endswith(ext) for ext in Config.VIDEO_EXTS):
                    info    = parse_episode_info(name)
                    size_mb = round(int(f.get("size", 0)) / (1024*1024), 1)
                    all_files.append({
                        "id":           f["id"],
                        "name":         name,
                        "size":         int(f.get("size", 0)),
                        "created":      f.get("createdTime", ""),
                        "display_name": info["display_name"],
                        "sort_key":     info["sort_key"],
                        "season":       info["season"],
                        "episode":      info["episode"],
                    })
                    log.info(f"   Found: {name}")
                    log.info(f"          → Display: '{info['display_name']}' "
                             f"| Season={info['season']} Episode={info['episode']} "
                             f"({size_mb} MB)")

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        except requests.exceptions.RequestException as e:
            log.error(f"Drive API network error: {e}")
            return []

    # ── CORRECT SORT: by Season + Episode NUMBER ──────────────
    # This ensures Episode_10 comes AFTER Episode_9, not before Episode_2
    all_files.sort(key=lambda f: f["sort_key"])

    log.info(f"Drive scan complete: {len(all_files)} video(s) found")
    log.info("Episode order after correct numerical sort:")
    for i, f in enumerate(all_files, 1):
        log.info(f"   {i}. {f['display_name']} ({f['name']})")

    return all_files


def download_movie(file_id, output_path):
    log.step(6, 10, "Download movie from Google Drive")
    log.info(f"File ID: {file_id}  →  {output_path}")
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
        log.info("Starting download...")
        gdown.download(f"https://drive.google.com/uc?id={file_id}",
                       output_path, quiet=False)
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            log.info(f"✅ Download complete! {size_mb:.1f} MB")
            return True
        log.error("Download file is empty!")
        return False
    except Exception as e:
        log.error(f"Download failed: {e}")
        log.error(traceback.format_exc())
        return False


# ============================================================
#         STEP 7 — VIDEO INFO
# ============================================================
def ffprobe_duration(video_path):
    log.info("Reading duration with ffprobe...")
    try:
        cmd    = ["ffprobe", "-v", "error",
                  "-show_entries", "format=duration",
                  "-of", "default=noprint_wrappers=1:nokey=1",
                  video_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        duration = float(result.stdout.strip())
        log.info(f"Duration: {duration:.2f}s = {int(duration)//60}m {int(duration)%60}s")
        return duration
    except Exception as e:
        log.error(f"ffprobe failed: {e}")
        return 0.0


def get_video_info(video_path):
    log.step(7, 10, "Analyse video duration and calculate parts count")
    duration = ffprobe_duration(video_path)
    if duration <= 0:
        return 0, 0
    total_parts = sum(
        1 for start in range(0, int(duration), Config.CLIP_LENGTH)
        if min(start + Config.CLIP_LENGTH, duration) - start >= 5
    )
    log.info(f"Will split into {total_parts} parts × {Config.CLIP_LENGTH}s each")
    return duration, total_parts


# ============================================================
#         CLIP EXTRACTION (ffmpeg, fast stream copy)
# ============================================================
def extract_clip(video_path, part_num, total_parts, output_path):
    """
    ffmpeg -c copy = stream copy, NO re-encoding.
    ~2-5 seconds per clip. Previously moviepy took 3-8 MINUTES.
    """
    start = (part_num - 1) * Config.CLIP_LENGTH
    log.info(f"✂️  Part {part_num}/{total_parts} | {start}s→{start+Config.CLIP_LENGTH}s | "
             "ffmpeg stream-copy (no re-encode)")
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start), "-i", video_path,
        "-t", str(Config.CLIP_LENGTH),
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",
        output_path,
    ]
    try:
        t0     = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            log.error(f"ffmpeg error: {result.stderr[-400:]}")
            return False
        if os.path.exists(output_path) and os.path.getsize(output_path) > 10_000:
            mb = os.path.getsize(output_path) / (1024*1024)
            log.info(f"   ✅ Clip ready in {time.time()-t0:.1f}s — {mb:.1f} MB")
            return True
        log.error("ffmpeg produced empty output")
        return False
    except subprocess.TimeoutExpired:
        log.error("ffmpeg timed out")
        return False
    except Exception as e:
        log.error(f"extract_clip error: {e}")
        return False


# ============================================================
#         THUMBNAIL GENERATION
# ============================================================
def extract_frame_ffmpeg(video_path, time_sec, output_jpg):
    log.info(f"   Frame at t={time_sec:.1f}s → {output_jpg}")
    cmd = ["ffmpeg", "-y", "-ss", str(time_sec), "-i", video_path,
           "-frames:v", "1", "-q:v", "2", output_jpg]
    try:
        subprocess.run(cmd, capture_output=True, timeout=30)
        if os.path.exists(output_jpg) and os.path.getsize(output_jpg) > 0:
            img = Image.open(output_jpg).copy()
            log.info(f"   Frame: {img.width}×{img.height}px")
            return img
    except Exception as e:
        log.error(f"Frame extract failed: {e}")
    return Image.new("RGB", (1280, 720), (20, 20, 40))


def extract_frames_for_grid(video_path, duration, frame_count=9):
    log.info(f"Extracting {frame_count} frames from video for thumbnail selection...")
    frames  = []
    tmp_dir = "tmp_frames"
    os.makedirs(tmp_dir, exist_ok=True)
    for i in range(frame_count):
        t   = min(duration * (0.15 + i * 0.07), duration - 1.0)
        out = os.path.join(tmp_dir, f"frame_{i}.jpg")
        log.info(f"   Frame {i+1}/{frame_count}: t={t:.1f}s")
        frames.append(extract_frame_ffmpeg(video_path, t, out))
    shutil.rmtree(tmp_dir, ignore_errors=True)
    log.info("All frames extracted")
    return frames


def create_frame_grid(frames, tile_size=320):
    log.info("Building 3×3 frame grid for Gemini selection...")
    grid = Image.new("RGB", (tile_size * 3, tile_size * 3))
    for idx, img in enumerate(frames):
        grid.paste(img.resize((tile_size, tile_size)),
                   ((idx % 3) * tile_size, (idx // 3) * tile_size))
    return grid


def choose_best_frame_with_gemini(grid_image, frames):
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        log.warn("Gemini not available → middle frame")
        return frames[4]
    buf = BytesIO()
    grid_image.save(buf, format="JPEG", quality=85)
    prompt = (
        "You are selecting the best movie thumbnail frame.\n"
        "Grid numbered:\n  1 2 3\n  4 5 6\n  7 8 9\n\n"
        "Pick the brightest, clearest frame with visible characters.\n"
        "Reply with ONLY a single digit 1-9."
    )
    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception as e:
        log.warn(f"Gemini init failed: {e} → middle frame")
        return frames[4]
    for model in Config.GEMINI_VISION_MODELS:
        try:
            log.info(f"Asking Gemini ({model}) to pick best frame...")
            resp = client.models.generate_content(
                model=model,
                contents=[
                    genai_types.Part.from_bytes(data=buf.getvalue(), mime_type="image/jpeg"),
                    genai_types.Part.from_text(text=prompt),
                ],
            )
            digit = next((c for c in resp.text.strip() if c.isdigit() and c != "0"), None)
            if digit and 1 <= int(digit) <= 9:
                log.info(f"✅ Gemini chose frame #{digit}")
                return frames[int(digit) - 1]
        except Exception as e:
            log.warn(f"Gemini {model} failed: {e}")
    log.warn("All Gemini models failed → middle frame")
    return frames[4]


def generate_gemini_background(movie_name):
    if not GEMINI_AVAILABLE or not Config.GEMINI_API_KEY:
        return None
    prompt = (
        f"Cinematic movie poster background for '{movie_name}'. "
        "Dark moody atmosphere, dramatic lighting. NO text, NO words."
    )
    try:
        client = genai.Client(api_key=Config.GEMINI_API_KEY)
    except Exception:
        return None
    for model in Config.GEMINI_IMAGE_MODELS:
        try:
            log.info(f"Trying Gemini image gen ({model})...")
            resp = client.models.generate_content(
                model=model, contents=prompt,
                config=genai_types.GenerateContentConfig(
                    response_modalities=["IMAGE", "TEXT"]))
            if resp.candidates:
                for part in resp.candidates[0].content.parts:
                    if hasattr(part, "inline_data") and part.inline_data:
                        img = Image.open(BytesIO(part.inline_data.data))
                        log.info(f"✅ AI background generated ({model})")
                        return img
        except Exception as e:
            log.warn(f"Gemini image {model} failed: {e}")
    log.warn("Gemini image gen failed → best video frame will be used")
    return None


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
    log.info(f"   Compositing thumbnail Part {part_num}/{total_parts}...")
    try:
        thumb = bg_image.copy().resize((1080, 1920), Image.LANCZOS).convert("RGBA")
        overlay = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
        odraw   = ImageDraw.Draw(overlay)
        for y in range(500):
            odraw.rectangle([(0,y),(1080,y+1)], fill=(0,0,0,int(220*(1-y/500))))
        for y in range(1420, 1920):
            odraw.rectangle([(0,y),(1080,y+1)], fill=(0,0,0,int(220*((y-1420)/500))))
        thumb = Image.alpha_composite(thumb, overlay).convert("RGB")
        draw  = ImageDraw.Draw(thumb)

        font_title = get_font(68)
        font_part  = get_font(56)
        font_info  = get_font(36)

        # Word-wrap title (max 18 chars per line)
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

        y_cur = 80
        for line in lines:
            bbox = draw.textbbox((0,0), line, font=font_title)
            tw   = bbox[2] - bbox[0]
            x    = (1080 - tw) // 2
            for dx in range(-3,4):
                for dy in range(-3,4):
                    draw.text((x+dx,y_cur+dy), line, font=font_title, fill="black")
            draw.text((x, y_cur), line, font=font_title, fill="white")
            y_cur += bbox[3] - bbox[1] + 12

        pt   = f"PART {part_num} / {total_parts}"
        bbox = draw.textbbox((0,0), pt, font=font_part)
        x    = (1080 - (bbox[2]-bbox[0])) // 2
        for dx in range(-2,3):
            for dy in range(-2,3):
                draw.text((x+dx,1740+dy), pt, font=font_part, fill="black")
        draw.text((x,1740), pt, font=font_part, fill=(255,215,0))

        ct   = f"Movie {movie_num} of {total_movies}"
        bbox = draw.textbbox((0,0), ct, font=font_info)
        draw.text(((1080-(bbox[2]-bbox[0]))//2, 1810),
                  ct, font=font_info, fill=(180,180,180))
        draw.rectangle([(200,1720),(880,1723)], fill=(255,215,0))

        thumb.save(output_path, "JPEG", quality=95)
        log.info(f"   ✅ Thumbnail saved")
        return True
    except Exception as e:
        log.error(f"Thumbnail failed: {e}")
        try:
            fb = Image.new("RGB", (1080, 1920), (20, 20, 40))
            d  = ImageDraw.Draw(fb)
            f2 = get_font(60)
            d.text((100,800), movie_name,                       font=f2, fill="white")
            d.text((100,900), f"Part {part_num}/{total_parts}", font=f2, fill=(255,215,0))
            fb.save(output_path, "JPEG")
            return True
        except Exception:
            return False


# ============================================================
#         INSTAGRAM LOGIN
# ============================================================
def instagram_login():
    log.info("── Instagram Login ──────────────────────────────────")
    if not os.path.exists(Config.IG_SESSION_FILE):
        log.error("session.json not found. Check IG_SESSION secret.")
        return None
    for attempt in range(1, 4):
        try:
            log.info(f"Instagram login attempt {attempt}/3...")
            cl = Client()
            cl.delay_range = [2, 5]
            cl.load_settings(Config.IG_SESSION_FILE)
            cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
            cl.get_timeline_feed()
            cl.dump_settings(Config.IG_SESSION_FILE)
            log.info(f"✅ Instagram logged in")
            return cl
        except ChallengeRequired:
            log.error("⛔ Instagram challenge. FIX: regenerate session.json → update IG_SESSION")
            return None
        except Exception as e:
            log.warn(f"Instagram attempt {attempt} failed: {e}")
            if attempt < 3:
                time.sleep(30 * attempt)
    log.error("All Instagram login attempts failed")
    return None


# ============================================================
#         YOUTUBE LOGIN
# ============================================================
def youtube_login():
    log.info("── YouTube Login ────────────────────────────────────")
    if not YT_AVAILABLE:
        log.warn("google-api-python-client not installed → YouTube skipped")
        return None
    if not os.path.exists(Config.YT_TOKEN_FILE):
        log.warn("yt_token.json not found → YouTube skipped")
        log.warn("FIX: Run generate_yt_token.py locally, add YT_TOKEN secret")
        return None
    try:
        with open(Config.YT_TOKEN_FILE, "r") as f:
            token_data = json.load(f)
        creds = Credentials(
            token         = token_data.get("token"),
            refresh_token = token_data.get("refresh_token"),
            token_uri     = token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id     = token_data.get("client_id") or Config.YT_CLIENT_ID,
            client_secret = token_data.get("client_secret") or Config.YT_CLIENT_SECRET,
            scopes        = token_data.get("scopes",
                                           ["https://www.googleapis.com/auth/youtube.upload"]),
        )
        if creds.expired and creds.refresh_token:
            log.info("Token expired — auto-refreshing...")
            creds.refresh(GoogleAuthRequest())
            refreshed = {
                "token": creds.token, "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri, "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": list(creds.scopes) if creds.scopes else [],
            }
            with open(Config.YT_TOKEN_FILE, "w") as f:
                json.dump(refreshed, f, indent=4)
            log.info("Token refreshed and saved")
        yt_service = yt_build("youtube", "v3", credentials=creds)
        log.info("✅ YouTube authenticated")
        return yt_service
    except Exception as e:
        log.error(f"YouTube login failed: {e}")
        log.warn("YouTube uploads will be skipped this run")
        return None


# ============================================================
#         INSTAGRAM UPLOAD
# ============================================================
def upload_to_instagram(cl, video_path, thumbnail_path, caption):
    log.info(f"   📸 Instagram upload | "
             f"{os.path.getsize(video_path)/1024/1024:.1f} MB")
    for retry in range(1, 4):
        try:
            log.info(f"   Attempt {retry}/3 — cl.clip_upload()...")
            t0     = time.time()
            kwargs = {"path": video_path, "caption": caption}
            if thumbnail_path and os.path.exists(thumbnail_path):
                kwargs["thumbnail"] = Path(thumbnail_path)
            cl.clip_upload(**kwargs)
            log.info(f"   ✅ Instagram done in {time.time()-t0:.1f}s")
            return True
        except PleaseWaitFewMinutes:
            time.sleep(600 * retry)
        except ClientThrottledError:
            time.sleep(900 * retry)
        except FeedbackRequired as e:
            log.error(f"FeedbackRequired: {e}")
            return "STOP"
        except ChallengeRequired:
            log.error("Challenge required → update IG_SESSION secret")
            return "STOP"
        except LoginRequired:
            log.warn("Session expired — re-logging...")
            try:
                cl.login(Config.IG_USERNAME, Config.IG_PASSWORD)
                cl.dump_settings(Config.IG_SESSION_FILE)
            except Exception:
                return "STOP"
        except ConnectionError:
            time.sleep(180 * retry)
        except Exception as e:
            log.error(f"Instagram attempt {retry} error: {e}")
            if retry < 3:
                time.sleep(300 * retry)
    log.error("Instagram upload failed after 3 attempts")
    return False


# ============================================================
#         YOUTUBE UPLOAD
# ============================================================
def upload_to_youtube(yt_service, video_path, thumbnail_path,
                      movie_name, part_num, total_parts):
    if yt_service is None:
        log.warn("   YouTube service not available — skipping")
        return False

    title       = random.choice(Config.YT_TITLES).format(
        name=movie_name, p=part_num, t=total_parts)
    description = Config.YT_DESCRIPTION.format(
        name=movie_name, p=part_num, t=total_parts)
    log.info(f"   ▶️  YouTube Shorts | {os.path.getsize(video_path)/1024/1024:.1f} MB")
    log.info(f"   Title: {title}")

    body = {
        "snippet": {
            "title": title, "description": description,
            "categoryId": Config.YT_CATEGORY_ID,
            "tags": ["shorts", "movie", "viral", movie_name],
        },
        "status": {
            "privacyStatus": Config.YT_PRIVACY,
            "selfDeclaredMadeForKids": False,
        },
    }

    for retry in range(1, 4):
        try:
            log.info(f"   Attempt {retry}/3 — videos.insert()...")
            t0    = time.time()
            media = MediaFileUpload(video_path, mimetype="video/mp4",
                                    resumable=True, chunksize=10*1024*1024)
            req   = yt_service.videos().insert(
                part="snippet,status", body=body, media_body=media)
            response = None
            while response is None:
                status, response = req.next_chunk()
                if status:
                    log.info(f"   YouTube upload: {int(status.progress()*100)}%")

            video_id = response.get("id", "unknown")
            log.info(f"   ✅ YouTube done in {time.time()-t0:.1f}s")
            log.info(f"   URL: https://www.youtube.com/shorts/{video_id}")

            if thumbnail_path and os.path.exists(thumbnail_path):
                try:
                    yt_service.thumbnails().set(
                        videoId=video_id,
                        media_body=MediaFileUpload(thumbnail_path, mimetype="image/jpeg"),
                    ).execute()
                    log.info("   ✅ YouTube thumbnail uploaded")
                except Exception as te:
                    log.warn(f"   Thumbnail failed (video still uploaded): {te}")
            return True

        except Exception as e:
            err = str(e)
            log.error(f"   YouTube attempt {retry} failed: {err}")
            if "quotaExceeded" in err or "403" in err:
                log.error("   YouTube quota exceeded — resets midnight Pacific Time")
                return False
            if retry < 3:
                time.sleep(60 * retry)

    log.error("YouTube upload failed after 3 attempts")
    return False


# ============================================================
#         MOVIE TRACKER
# ============================================================
def load_movies_log():
    default = {
        "movies": {},
        "current_movie": "",
        "total_movies_found": 0,
        "total_completed": 0,
        "total_ig_uploaded": 0,
        "total_yt_uploaded": 0,
        "last_run": "",
        "episode_order": [],   # stores correct sorted order of filenames
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
    data["total_movies_found"] = len(data["movies"])
    data["total_ig_uploaded"]  = sum(
        m.get("ig_uploaded_parts", 0) for m in data["movies"].values())
    data["total_yt_uploaded"]  = sum(
        m.get("yt_uploaded_parts", 0) for m in data["movies"].values())
    save_json(Config.LOG_FILE, data)


def sync_with_drive(movies_log, drive_files):
    """
    Add new files to tracker and update the correct episode_order list.
    The episode_order list stores filenames in correct numerical sort order
    so get_next_movie() always picks the right next episode.
    """
    # Update the episode order list (sorted by season+episode number)
    movies_log["episode_order"] = [f["name"] for f in drive_files]
    log.info(f"Episode order saved: {len(movies_log['episode_order'])} entries")

    added = 0
    for f in drive_files:
        name = f["name"]
        if name not in movies_log["movies"]:
            info = parse_episode_info(name)
            movies_log["movies"][name] = {
                "drive_id":          f["id"],
                "status":            "pending",
                "total_parts":       0,
                "ig_uploaded_parts": 0,
                "yt_uploaded_parts": 0,
                "size_mb":           round(f["size"] / (1024*1024), 1),
                "display_name":      info["display_name"],
                "season":            info["season"],
                "episode":           info["episode"],
                "started_at":        "",
                "completed_at":      "",
                "last_uploaded_at":  "",
                "errors":            0,
            }
            log.info(f"🆕 New: {info['display_name']} ({name})")
            added += 1
    if added:
        log.info(f"Added {added} new movie(s)")
    else:
        log.info("No new movies detected")
    return movies_log


def get_next_movie(movies_log):
    """
    Pick next movie in correct episode order.
    Uses episode_order list (sorted by season+episode number)
    so processing is always Episode 1 → 2 → 3 ... → 52, not alphabetical.
    """
    order = movies_log.get("episode_order", [])

    # First: resume any in_progress movie (in order)
    for name in order:
        info = movies_log["movies"].get(name)
        if info and info["status"] == "in_progress":
            display = info.get("display_name", name)
            log.info(f"▶️ Resuming: {display}")
            return name, info

    # Then: start next pending movie (in order)
    for name in order:
        info = movies_log["movies"].get(name)
        if info and info["status"] == "pending":
            display = info.get("display_name", name)
            log.info(f"🆕 Starting: {display}")
            return name, info

    # Fallback: if episode_order is empty (first run), scan movies dict
    for name, info in movies_log["movies"].items():
        if info["status"] == "in_progress":
            return name, info
    for name, info in movies_log["movies"].items():
        if info["status"] == "pending":
            return name, info

    return None, None


def load_progress():
    return load_json(Config.PROGRESS_FILE,
                     {"movie_name": "", "last_uploaded": 0, "total_parts": 0})


def save_progress(data):
    save_json(Config.PROGRESS_FILE, data)


def smart_delay(n):
    """No-op — delay handled by cron schedule (every 2 hours)."""
    log.info("No in-script delay — cron handles the 2hr gap")


# ============================================================
#         SUMMARY
# ============================================================
def print_summary(movies_log):
    log.separator("=")
    print("📊 MOVIES STATUS REPORT", flush=True)
    log.separator("-")
    emoji_map = {"pending":"⏳","in_progress":"🔄","completed":"✅","error":"❌"}
    order = movies_log.get("episode_order", list(movies_log["movies"].keys()))
    for idx, name in enumerate(order, 1):
        info = movies_log["movies"].get(name)
        if not info:
            continue
        emoji   = emoji_map.get(info["status"], "❓")
        display = info.get("display_name", name)
        ig_p    = info.get("ig_uploaded_parts", 0)
        yt_p    = info.get("yt_uploaded_parts", 0)
        total   = info.get("total_parts", "?")
        print(f"  {emoji} #{idx} {display}", flush=True)
        print(f"      {info['status']} | 📸 IG: {ig_p}/{total} | "
              f"▶️ YT: {yt_p}/{total} | {info.get('size_mb','?')} MB", flush=True)
        if info.get("completed_at"):
            print(f"      Completed: {info['completed_at']}", flush=True)
        print(flush=True)
    log.separator("-")
    total = len(movies_log["movies"])
    done  = movies_log.get("total_completed", 0)
    print(f"  📈 Episodes: {done}/{total} completed", flush=True)
    print(f"  📸 Instagram: {movies_log.get('total_ig_uploaded',0)} reels uploaded", flush=True)
    print(f"  ▶️  YouTube:   {movies_log.get('total_yt_uploaded',0)} Shorts uploaded", flush=True)
    print(f"  🕐 Last run: {movies_log.get('last_run','N/A')}", flush=True)
    log.separator("=")


# ============================================================
#                        MAIN
# ============================================================
def main():
    log.separator("=")
    print("🎬 FULLY AUTOMATED INSTAGRAM + YOUTUBE REEL UPLOADER", flush=True)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"📋 Platforms: "
          f"{'✅ Instagram' if Config.UPLOAD_TO_INSTAGRAM else '❌ Instagram'}  "
          f"{'✅ YouTube' if Config.UPLOAD_TO_YOUTUBE else '❌ YouTube'}", flush=True)
    log.separator("=")

    write_session_from_secret()
    if not verify_setup():
        return

    drive_files = list_drive_movies()
    if not drive_files:
        log.error("No videos found in Drive folder")
        return

    log.step(4, 10, "Sync movie tracker with Drive contents")
    movies_log = load_movies_log()
    movies_log = sync_with_drive(movies_log, drive_files)
    save_movies_log(movies_log)

    log.step(5, 10, "Select next movie/episode to process")
    movie_name, movie_info = get_next_movie(movies_log)
    if not movie_name:
        log.info("🎉 All episodes fully uploaded!")
        print_summary(movies_log)
        return

    display_name = movie_info.get("display_name", movie_display_name(movie_name))
    log.info(f"Selected: {display_name}")
    log.info(f"File:     {movie_name}")
    log.info(f"Status:   {movie_info['status']}")
    if movie_info.get("season"):
        log.info(f"Season:   {movie_info['season']}  Episode: {movie_info['episode']}")

    if not download_movie(movie_info["drive_id"], Config.MOVIE_FILE):
        movie_info["errors"] = movie_info.get("errors", 0) + 1
        save_movies_log(movies_log)
        git_push()
        return

    duration, total_parts = get_video_info(Config.MOVIE_FILE)
    if total_parts == 0:
        movie_info["status"] = "error"
        save_movies_log(movies_log)
        git_push()
        return

    movie_info["total_parts"] = total_parts

    log.step(8, 10, "Update status and load upload progress")
    if movie_info["status"] == "pending":
        movie_info["status"]     = "in_progress"
        movie_info["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    movies_log["current_movie"] = movie_name
    save_movies_log(movies_log)

    progress = load_progress()
    if progress.get("movie_name") != movie_name:
        log.info("New episode — resetting part counter to 0")
        progress = {"movie_name": movie_name,
                    "last_uploaded": 0, "total_parts": total_parts}
    last_uploaded = progress["last_uploaded"]
    log.info(f"Progress: {last_uploaded}/{total_parts} parts done")
    if last_uploaded > 0:
        log.info(f"Resuming from Part {last_uploaded + 1}")

    # ── Thumbnail background (per-movie cache) ────────────────
    log.step(9, 10, "Prepare thumbnail background")
    bg_cache_path = thumb_bg_path_for_movie(movie_name)
    thumb_bg = None

    if os.path.exists(bg_cache_path):
        try:
            thumb_bg = Image.open(bg_cache_path)
            log.info(f"✅ Cached background loaded for '{display_name}' "
                     f"({thumb_bg.width}×{thumb_bg.height}px)")
        except Exception:
            thumb_bg = None

    if thumb_bg is None:
        log.info(f"No cache for '{display_name}' — generating background...")
        thumb_bg = generate_gemini_background(display_name)
        if thumb_bg is None:
            frames   = extract_frames_for_grid(Config.MOVIE_FILE, duration)
            grid     = create_frame_grid(frames)
            thumb_bg = choose_best_frame_with_gemini(grid, frames)
        try:
            thumb_bg.save(bg_cache_path, "JPEG", quality=95)
            log.info(f"✅ Background cached: {bg_cache_path}")
        except Exception as e:
            log.warn(f"Could not cache background: {e}")

    log.step(10, 10, "Login to Instagram and YouTube")
    cl         = instagram_login() if Config.UPLOAD_TO_INSTAGRAM else None
    yt_service = youtube_login()   if Config.UPLOAD_TO_YOUTUBE   else None

    if cl is None and yt_service is None:
        log.error("Both platforms failed login — stopping")
        save_progress(progress)
        save_movies_log(movies_log)
        git_push()
        return

    os.makedirs(Config.REELS_DIR,  exist_ok=True)
    os.makedirs(Config.THUMBS_DIR, exist_ok=True)

    movie_names  = movies_log.get("episode_order",
                                  list(movies_log["movies"].keys()))
    movie_num    = (movie_names.index(movie_name) + 1
                    if movie_name in movie_names else 1)
    total_movies = len(movie_names)

    log.info(f"Episode {movie_num} of {total_movies} | "
             f"Parts remaining: {total_parts - last_uploaded}")
    log.separator("=")

    # ── UPLOAD LOOP ───────────────────────────────────────────
    uploaded_this_run = 0
    stop_ig           = False

    for part_num in range(last_uploaded + 1, total_parts + 1):

        if uploaded_this_run >= Config.MAX_UPLOADS_PER_RUN:
            log.info(f"🛑 Run limit ({Config.MAX_UPLOADS_PER_RUN}) reached — "
                     "continuing next scheduled run")
            break

        log.separator("-")
        log.info(f"📦 PART {part_num}/{total_parts} | '{display_name}'")

        clip_path = os.path.join(Config.REELS_DIR, f"part_{part_num}.mp4")
        if not extract_clip(Config.MOVIE_FILE, part_num, total_parts, clip_path):
            log.warn(f"Clip failed — skipping Part {part_num}")
            progress["last_uploaded"] = part_num
            save_progress(progress)
            continue

        thumb_path = os.path.join(Config.THUMBS_DIR, f"thumb_{part_num}.jpg")
        if thumb_bg:
            bg_image = thumb_bg.copy()
        else:
            mid_t   = min(((part_num-1)*Config.CLIP_LENGTH)+(Config.CLIP_LENGTH//2),
                          duration - 1)
            tmp_jpg = os.path.join(Config.THUMBS_DIR, f"tmp_{part_num}.jpg")
            bg_image = extract_frame_ffmpeg(Config.MOVIE_FILE, mid_t, tmp_jpg)
        create_thumbnail(bg_image, display_name, part_num, total_parts,
                         movie_num, total_movies, thumb_path)

        ig_caption = random.choice(Config.IG_CAPTIONS).format(
            name=display_name, p=part_num, t=total_parts)

        ig_ok = False
        yt_ok = False

        if Config.UPLOAD_TO_INSTAGRAM and cl and not stop_ig:
            log.info(f"[{part_num}/{total_parts}] → Instagram...")
            ig_result = upload_to_instagram(cl, clip_path, thumb_path, ig_caption)
            if ig_result == "STOP":
                log.upload("Instagram", display_name, part_num, total_parts, "FATAL_STOP")
                stop_ig = True
            elif ig_result is True:
                ig_ok = True
                log.upload("Instagram", display_name, part_num, total_parts, "SUCCESS")
            else:
                log.upload("Instagram", display_name, part_num, total_parts, "FAILED")

        if Config.UPLOAD_TO_YOUTUBE and yt_service:
            log.info(f"[{part_num}/{total_parts}] → YouTube Shorts...")
            yt_ok = upload_to_youtube(
                yt_service, clip_path, thumb_path,
                display_name, part_num, total_parts)
            log.upload("YouTube", display_name, part_num, total_parts,
                       "SUCCESS" if yt_ok else "FAILED")

        if ig_ok or yt_ok:
            uploaded_this_run += 1
            progress["last_uploaded"] = part_num
            if ig_ok:
                movie_info["ig_uploaded_parts"] = part_num
            if yt_ok:
                movie_info["yt_uploaded_parts"] = part_num
            movie_info["last_uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log.info(f"✅ Part {part_num}/{total_parts} done | "
                     f"IG={'✅' if ig_ok else '❌'}  YT={'✅' if yt_ok else '❌'}")
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()
        else:
            log.error(f"Part {part_num} failed on all platforms")
            movie_info["errors"] = movie_info.get("errors", 0) + 1
            save_progress(progress)
            save_movies_log(movies_log)
            git_push()

        for f in [clip_path, thumb_path]:
            if os.path.exists(f):
                os.remove(f)

    # ── Episode complete? ─────────────────────────────────────
    log.separator("*")
    if progress["last_uploaded"] >= total_parts:
        log.info(f"🎉🎉🎉 '{display_name}' FULLY UPLOADED! 🎉🎉🎉")
        movie_info["status"]       = "completed"
        movie_info["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        movies_log["current_movie"] = ""
        progress = {"movie_name": "", "last_uploaded": 0, "total_parts": 0}
        cleanup_temp(movie_name)

        # Log what episode comes next
        order = movies_log.get("episode_order", [])
        if movie_name in order:
            idx = order.index(movie_name)
            if idx + 1 < len(order):
                next_name = order[idx + 1]
                next_info = parse_episode_info(next_name)
                log.info(f"⏭️  Next episode: {next_info['display_name']}")
            else:
                log.info("🏆 That was the LAST episode in the list!")
    else:
        left = total_parts - progress["last_uploaded"]
        log.info(f"{uploaded_this_run} part(s) uploaded | {left} remaining")
        log.info("Next part will upload on the next scheduled run (~2 hours)")
    log.separator("*")

    save_progress(progress)
    save_movies_log(movies_log)
    git_push()
    print_summary(movies_log)
    log.separator("=")
    log.info(f"✅ RUN COMPLETE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.separator("=")


# ============================================================
#                      ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warn("Interrupted (Ctrl+C)")
        git_push()
    except Exception as e:
        log.error(f"💥 CRITICAL ERROR: {e}")
        log.error(traceback.format_exc())
        git_push()
        sys.exit(1)
