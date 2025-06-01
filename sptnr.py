with open("VERSION", "r") as file:
    __version__ = file.read().strip()

import argparse
import base64
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import random
import tempfile

from dotenv import load_dotenv
import requests
from colorama import init, Fore, Style
from tqdm import tqdm
from datetime import timedelta

# Load environment variables from .env file if it exists
if os.path.exists(".env"):
    load_dotenv()

# Record the start time
start_time = time.time()

LOCK_DIR = "logs"
LOCK_FILENAME = "song_update_lock.json"
LOCK_FILE = LOCK_DIR + "/" + LOCK_FILENAME

# Config
NAV_BASE_URL = os.getenv("NAV_BASE_URL")
NAV_USER = os.getenv("NAV_USER")
NAV_PASS = os.getenv("NAV_PASS")
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Colors
LIGHT_PURPLE = Fore.MAGENTA + Style.BRIGHT
LIGHT_GREEN = Fore.GREEN + Style.BRIGHT
LIGHT_RED = Fore.RED + Style.BRIGHT
LIGHT_BLUE = Fore.BLUE + Style.BRIGHT
LIGHT_CYAN = Fore.CYAN + Style.BRIGHT
LIGHT_YELLOW = Fore.YELLOW + Style.BRIGHT
BOLD = Style.BRIGHT
RESET = Style.RESET_ALL

# Setup logs
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOGFILE = os.path.join(LOG_DIR, f"spotify-popularity_{int(time.time())}.log")

HEX_ENCODED_PASS = NAV_PASS.encode().hex()
TOKEN_AUTH = base64.b64encode(
    f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
).decode()
TOKEN_URL = "https://accounts.spotify.com/api/token"

class SpotifyTokenManager:
    def __init__(self, client_id, client_secret, token_url):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.token = None
        self.expires_at = 0
        self._authenticate()

    def _authenticate(self):
        token_auth = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        response = requests.post(
            self.token_url,
            headers={"Authorization": f"Basic {token_auth}"},
            data={"grant_type": "client_credentials"},
        )
        if response.status_code != 200:
            error_info = response.json()
            error_description = error_info.get("error_description", "Unknown error")
            logging.error(
                f"{LIGHT_RED}Spotify Authentication Error: {error_description}{RESET}"
            )
            sys.exit(1)
        token_data = response.json()
        self.token = token_data["access_token"]
        self.expires_at = time.time() + token_data["expires_in"] - 60  # refresh 1 min early

    def get_token(self):
        if time.time() >= self.expires_at:
            self._authenticate()
        return self.token

class NoColorFormatter(logging.Formatter):
    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

    def format(self, record):
        record.msg = self.ansi_escape.sub("", record.msg)
        return super(NoColorFormatter, self).format(record)


def load_lock():
    if not os.path.exists(LOCK_FILE):
        return {}
    try:
        with open(LOCK_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logging.error(f"{LIGHT_RED}Lock file '{LOCK_FILE}' is corrupt or not valid JSON. Starting with an empty lock.{RESET}")
        os.rename(LOCK_FILE, LOCK_FILE + ".corrupt")
        return {}
    except Exception as e:
        logging.error(f"{LIGHT_RED}Error loading lock file '{LOCK_FILE}': {e}{RESET}")
        return {}

def save_lock(lock):
    # Write to a temp file first, then atomically replace the lock file
    dir_name = os.path.dirname(LOCK_FILE) or "."
    with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False) as tf:
        json.dump(lock, tf)
        tempname = tf.name
    os.replace(tempname, LOCK_FILE)

def should_update(song_id):
    lock_expiry = get_lock_expiry()
    if lock_expiry == 0:
        return True
    last_update_ts = LOCK.get(song_id)
    if not last_update_ts:
        return True
    return (time.time() - last_update_ts) > (lock_expiry * 86400)


def get_lock_expiry():
    if (BASE_LOCK_DURATION == 0):
        return 0  # No lock duration, force update every time

    base_expiry = timedelta(days=BASE_LOCK_DURATION)
    jitter = timedelta(hours=random.uniform(-LOCK_JITTER/2, LOCK_JITTER/2))
    expiry = base_expiry + jitter
    # Ensure expiry is at least 1 day
    expiry = max(expiry, timedelta(days=1))
    return time.time() + expiry.total_seconds()

# Set up the stream handler (console logging) without timestamp
logging.basicConfig(
    level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()]
)

# Set up the file handler (file logging) with timestamp
file_handler = logging.FileHandler(LOGFILE, "a")
file_handler.setFormatter(NoColorFormatter("[%(asctime)s] %(message)s"))
logging.getLogger().addHandler(file_handler)

# Authentication
spotify_token_manager = SpotifyTokenManager(
    SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, TOKEN_URL
)
SPOTIFY_TOKEN = spotify_token_manager.get_token()

init(autoreset=True)

# Default flags
PREVIEW = 0
START = 0
LIMIT = 0
ARTIST_IDs = []
ALBUM_IDs = []

# Variables
ARTISTS_PROCESSED = 0
TOTAL_TRACKS = 0
FOUND_AND_UPDATED = 0
NOT_FOUND = 0
UNMATCHED_TRACKS = []

# Parse arguments
description_text = "process command-line flags for sync"
parser = argparse.ArgumentParser()

parser.add_argument(
    "-p",
    "--preview",
    action="store_true",
    help="execute script in preview mode (no changes made)",
)
parser.add_argument(
    "-a",
    "--artist",
    action="append",
    help="process the artist using the Navidrome artist ID (ignores START and LIMIT)",
    type=str,
)
parser.add_argument(
    "-b",
    "--album",
    action="append",
    help="process the album using the Navidrome album ID (ignores START and LIMIT)",
    type=str,
)
parser.add_argument(
    "-s",
    "--start",
    default=0,
    type=int,
    help="start processing from artist at index [NUM] (0-based index, so 0 is the first artist)",
)
parser.add_argument(
    "-l",
    "--limit",
    default=0,
    type=int,
    help="limit to processing [NUM] artists from the start index",
)
parser.add_argument(
    "-d",
    "--lock-duration",
    type=int,
    default=7,
    help="Number of days to lock song updates (0 to force update every time)",
)
parser.add_argument(
    "-j",
    "--lock-jitter",
    type=int,
    default=24,
    help="Number of hours to add random jitter to the lock duration",
)

parser.add_argument(
    "-v", "--version", action="version", version=f"%(prog)s {__version__}"
)


args = parser.parse_args()

ARTIST_IDs = args.artist if args.artist else []
ALBUM_IDs = args.album if args.album else []
START = args.start
LIMIT = args.limit
BASE_LOCK_DURATION = args.lock_duration
LOCK_JITTER = args.lock_jitter

logging.info(f"{BOLD}Version:{RESET} {LIGHT_YELLOW}sptnr v{__version__}{RESET}")

LOCK = load_lock()

SHOULD_DELAY = False

if args.preview:
    logging.info(f"{LIGHT_YELLOW}Preview mode, no changes will be made.{RESET}")
    PREVIEW = 1

# Check if both ARTIST_ID and START/LIMIT are provided
if ARTIST_IDs and (START != 0 or LIMIT != 0):
    START = 0
    LIMIT = 0
    logging.info(
        f"{LIGHT_YELLOW}Warning: The --artist flag overrides --start and --limit. Ignoring these settings.{RESET}"
    )

if not args.preview:
    logging.info(
        f"{BOLD}Syncing Spotify {LIGHT_CYAN}popularity{RESET}{BOLD} with Navidrome {LIGHT_BLUE}rating{RESET}...{RESET}"
    )


def validate_url(url):
    if not re.match(r"https?://", url):
        logging.error(
            f"{LIGHT_RED}Config Error: URL must start with 'http://' or 'https://'.{RESET}"
        )
        return False
    if url.endswith("/"):
        logging.error(
            f"{LIGHT_RED}Config Error: URL must not end with a trailing slash.{RESET}"
        )
        return False
    return True


def url_encode(string):
    return urllib.parse.quote_plus(string)


def get_rating_from_popularity(popularity):
    popularity = float(popularity)
    if popularity < 16.66:
        return 0
    elif popularity < 33.33:
        return 1
    elif popularity < 50:
        return 2
    elif popularity < 66.66:
        return 3
    elif popularity < 83.33:
        return 4
    else:
        return 5


def process_track(track_id, artist_name, album, track_name):

    # Declare global variables
    global FOUND_AND_UPDATED, UNMATCHED_TRACKS, NOT_FOUND, TOTAL_TRACKS


    if not should_update(track_id):
        print(f"Skipping {track_name}, recently updated.")
        return

    def search_spotify(query, max_retries=3):
        global SHOULD_DELAY
        SHOULD_DELAY = True

        SPOTIFY_TOKEN = spotify_token_manager.get_token()

        spotify_url = f"https://api.spotify.com/v1/search?q={query}&type=track&limit=1"
        headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}

        for attempt in range(max_retries):
            try:
                response = requests.get(spotify_url, headers=headers, timeout=10)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logging.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors with retry
                if response.status_code >= 500:
                    wait_time = (attempt + 1) * 2  # Exponential backoff factor
                    logging.warning(f"Spotify server error {response.status_code}. Attempt {attempt + 1}/{max_retries}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait_time = (attempt + 1) * 2
                logging.warning(f"Connection error: {e}. Attempt {attempt + 1}/{max_retries}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {e}")
                break
        
        # If we get here, all retries failed
        logging.error(f"Failed after {max_retries} attempts for query: {query}")
        return None

    def remove_parentheses_content(s):
        # Only remove parentheses if they do NOT contain important keywords
        keywords = ["remix", "instrumental", "edit", "version", "mix", "karaoke", "live", "acoustic", "demo"]
        def replacer(match):
            content = match.group(1).lower()
            if any(k in content for k in keywords):
                return f"({match.group(1)})"  # Keep it
            return ""
        return re.sub(r"\((.*?)\)", replacer, s).strip()

    search_attempts = [
        # Primary attempt with all info
        lambda: f"{url_encode(track_name)}%20artist:{url_encode(artist_name)}%20album:{url_encode(album)}",
        
        # Secondary attempt without album
        lambda: f"{url_encode(remove_parentheses_content(track_name))}%20artist:{url_encode(artist_name)}",
        
        # Tertiary attempt with modified track name
        lambda: f"{url_encode(track_name.replace('Part', 'Pt.'))}%20artist:{url_encode(artist_name)}"
    ]

    spotify_data = None
    for attempt in search_attempts:
        # logging.info(f"Searching Spotify for: {LIGHT_CYAN}{attempt()}{RESET}")
        spotify_data = search_spotify(attempt())
        if spotify_data and spotify_data.get("tracks", {}).get("items"):
            break

    if spotify_data and spotify_data.get("tracks", {}).get("items"):
        # Success case - process the track
        track = spotify_data["tracks"]["items"][0]
        popularity = track.get("popularity", 0)
        rating = get_rating_from_popularity(popularity)
        popularity_str = f"{popularity} " if 0 <= popularity <= 9 else str(popularity)
        
        #log matched track name from spotify
        sp_track_name = track["name"]

        logging.info(f"    p:{LIGHT_CYAN}{popularity_str}{RESET} → r:{LIGHT_BLUE}{rating}{RESET} | {LIGHT_GREEN}{track_name} - {sp_track_name}{RESET}")
        
        if PREVIEW != 1:
            try:
                nav_url = f"{NAV_BASE_URL}/rest/setRating?u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=myapp&id={track_id}&rating={rating}"
                requests.get(nav_url, timeout=5)
                FOUND_AND_UPDATED += 1
                LOCK[track_id] = time.time()
                save_lock(LOCK)
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to update rating in Navidrome: {e}")
    else:
        logging.info(f"    p:{LIGHT_RED}??{RESET} → r:{LIGHT_BLUE}0{RESET} | {LIGHT_RED}(not found) {track_name}{RESET}")
        UNMATCHED_TRACKS.append(f"{artist_name} - {album} - {track_name}")
        NOT_FOUND += 1

        # If not found, set rating to 0
        if PREVIEW != 1:
            try:
                nav_url = f"{NAV_BASE_URL}/rest/setRating?u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=myapp&id={track_id}&rating=0"
                requests.get(nav_url, timeout=5)
                LOCK[track_id] = time.time()
                save_lock(LOCK)
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to update rating in Navidrome: {e}")
        
        

    TOTAL_TRACKS += 1

def process_album(album_id):

    global SHOULD_DELAY

    if SHOULD_DELAY:
        # sleep for a short time to avoid hitting rate limits too quickly
        time.sleep(4)

    SHOULD_DELAY = False
    nav_url = f"{NAV_BASE_URL}/rest/getAlbum?id={album_id}&u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=spotify_sync&f=json"
    response = requests.get(nav_url).json()

    album_info = response["subsonic-response"]["album"]
    album_artist = album_info["artist"]
    tracks = [
        (song["id"], album_artist, song["album"], song["title"])
        for song in album_info.get("song", [])
    ]

    for track in tracks:
        process_track(*track)


def process_artist(artist_id):
    nav_url = f"{NAV_BASE_URL}/rest/getArtist?id={artist_id}&u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=spotify_sync&f=json"
    response = requests.get(nav_url).json()

    albums = [
        (album["id"], album["name"])
        for album in response["subsonic-response"]["artist"].get("album", [])
    ]

    for album_id, album_name in albums:
        logging.info(f"  Album: {LIGHT_YELLOW}{album_name}{RESET} ({album_id})")
        process_album(album_id)


def fetch_data(url):
    try:
        response = requests.get(url)
        response_data = json.loads(response.text)

        if "subsonic-response" not in response_data:
            logging.error(
                f"{LIGHT_RED}Unexpected response format from Navidrome.{RESET}"
            )
            sys.exit(1)

        nav_response = response_data["subsonic-response"]

        if "error" in nav_response:
            error_message = nav_response["error"].get("message", "Unknown error")
            logging.error(f"{LIGHT_RED}Navidrome Error: {error_message}{RESET}")
            sys.exit(1)

        return nav_response

    except requests.exceptions.ConnectionError:
        logging.error(
            f"{LIGHT_RED}Connection Error: Failed to connect to the provided URL. Please check if the URL is correct and the server is reachable.{RESET}"
        )
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(
            f"{LIGHT_RED}Connection Error: An error occurred while trying to connect to Navidrome: {e}{RESET}"
        )
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(
            f"{LIGHT_RED}JSON Parsing Error: Failed to parse JSON response from Navidrome. Please check if the provided URL is a valid Navidrome server.{RESET}"
        )
        sys.exit(1)


try:
    validate_url(NAV_BASE_URL)
except ValueError as e:
    logging.error(f"{LIGHT_RED}{e}{RESET}")
    sys.exit(1)

if ARTIST_IDs:
    for ARTIST_ID in ARTIST_IDs:
        url = f"{NAV_BASE_URL}/rest/getArtist?id={ARTIST_ID}&u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=spotify_sync&f=json"
        data = fetch_data(url)
        ARTIST_NAME = data["artist"]["name"]

        logging.info("")
        logging.info(f"Artist: {LIGHT_PURPLE}{ARTIST_NAME}{RESET} ({ARTIST_ID})")
        process_artist(ARTIST_ID)

elif ALBUM_IDs:
    for ALBUM_ID in ALBUM_IDs:
        url = f"{NAV_BASE_URL}/rest/getAlbum?id={ALBUM_ID}&u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=spotify_sync&f=json"
        data = fetch_data(url)
        ARTIST_NAME = data["album"]["artist"]
        ARTIST_ID = data["album"]["artistId"]
        ALBUM_NAME = data["album"]["name"]

        logging.info("")
        logging.info(f"Artist: {LIGHT_PURPLE}{ARTIST_NAME}{RESET} ({ARTIST_ID})")
        logging.info(f"  Album: {LIGHT_YELLOW}{ALBUM_NAME}{RESET} ({ALBUM_ID})")
        process_album(ALBUM_ID)

else:
    url = f"{NAV_BASE_URL}/rest/getArtists?u={NAV_USER}&p=enc:{HEX_ENCODED_PASS}&v=1.12.0&c=spotify_sync&f=json"
    data = fetch_data(url)
    ARTIST_DATA = [
        (artist["id"], artist["name"])
        for index_entry in data["artists"]["index"]
        for artist in index_entry["artist"]
    ]

    if START == 0 and LIMIT == 0:
        data_slice = ARTIST_DATA
        total_count = len(ARTIST_DATA)
    else:
        if LIMIT == 0:
            data_slice = ARTIST_DATA[START:]
        else:
            data_slice = ARTIST_DATA[START : START + LIMIT]
        total_count = len(data_slice)

    logging.info(f"Total artists to process: {LIGHT_GREEN}{total_count}{RESET}")

    for index, ARTIST_ENTRY in tqdm(
        enumerate(data_slice), total=total_count, leave=False
    ):
        ARTIST_ID, ARTIST_NAME = ARTIST_ENTRY

        logging.info("")
        logging.info(
            f"Artist: {LIGHT_PURPLE}{ARTIST_NAME}{RESET} ({ARTIST_ID})[{index+args.start}]"
        )
        process_artist(ARTIST_ID)

        ARTISTS_PROCESSED += 1


# Display the results
logging.info("")
MATCH_PERCENTAGE = (FOUND_AND_UPDATED / TOTAL_TRACKS) * 100 if TOTAL_TRACKS != 0 else 0
FORMATTED_MATCH_PERCENTAGE = round(MATCH_PERCENTAGE, 2)  # Rounding to 2 decimal places
TOTAL_BLOCKS = 20

color_found = LIGHT_GREEN if FOUND_AND_UPDATED == TOTAL_TRACKS else LIGHT_YELLOW
color_found_white = LIGHT_GREEN if FOUND_AND_UPDATED == TOTAL_TRACKS else BOLD
color_not_found = LIGHT_GREEN if NOT_FOUND == 0 else LIGHT_RED
blocks_found = "█" * round(FOUND_AND_UPDATED * TOTAL_BLOCKS / TOTAL_TRACKS)
blocks_not_found = "█" * (TOTAL_BLOCKS - len(blocks_found))
full_blocks_found = f"{color_found_white}{blocks_found}{RESET}"
full_blocks_not_found = f"{color_not_found}{blocks_not_found}{RESET}"

# Calculate elapsed time
elapsed_time = time.time() - start_time
hours, remainder = divmod(elapsed_time, 3600)
minutes, seconds = divmod(remainder, 60)

parts = []
if hours:
    parts.append(f"{int(hours)}h")
if minutes:
    parts.append(f"{int(minutes)}m")
if seconds or not parts:  # Show seconds if it's the only value, even if it's 0
    parts.append(f"{int(seconds)}s")

formatted_elapsed_time = " ".join(parts)

# logging.info(f"Processing completed in {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
logging.info(
    f"Tracks: {LIGHT_PURPLE}{TOTAL_TRACKS}{RESET} | Found: {color_found}{FOUND_AND_UPDATED}{RESET} |{full_blocks_found}{full_blocks_not_found}| Not Found: {color_not_found}{NOT_FOUND}{RESET} | Match: {color_found}{FORMATTED_MATCH_PERCENTAGE}%{RESET} | Time: {LIGHT_PURPLE}{formatted_elapsed_time}{RESET}"
)
