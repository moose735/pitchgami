"""
daily_runner.py
---------------
Runs nightly after games complete. Fetches all starting pitcher outings
from today's completed MLB games, performs the Scorigami lookup for each,
and either posts to X or writes to a dry-run output file.

Usage:
    python daily_runner.py                    # posts live to X
    python daily_runner.py --dry-run          # prints tweets, no posting
    python daily_runner.py --date 2024-07-04  # run for a specific date
    python daily_runner.py --dry-run --date 2024-10-01

Environment variables (for live posting):
    X_API_KEY
    X_API_SECRET
    X_ACCESS_TOKEN
    X_ACCESS_TOKEN_SECRET
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests

from scorigami import ScorigamiEngine, ScorigamiResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/daily_runner.log"),
    ],
)
log = logging.getLogger(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"
DB_PATH = os.environ.get("SCORIGAMI_DB_PATH", "data/pitcher_scorigami.db")


# ── MLB Stats API helpers ───────────────────────────────────────────────────────

def get_completed_games(game_date: str) -> list[int]:
    """Return list of gamePks for all completed games on game_date."""
    url = (
        f"{MLB_API}/schedule?sportId=1&date={game_date}"
        f"&gameType=R,P,F,D,L,W"  # regular season + postseason
        f"&fields=dates,games,gamePk,status,abstractGameState"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    pks = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            if game.get("status", {}).get("abstractGameState") == "Final":
                pks.append(game["gamePk"])
    return pks


def get_starting_pitchers(game_pk: int) -> list[dict]:
    """
    For a completed game, return the starting pitcher for each team.
    Returns list of dicts with keys: pitcher_name, team, ip, er, bb, so
    """
    url = f"{MLB_API}/game/{game_pk}/boxscore"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    box = resp.json()

    results = []
    for side in ("home", "away"):
        team_data = box.get("teams", {}).get(side, {})
        pitchers   = team_data.get("pitchers", [])
        players    = team_data.get("players", {})
        team_name  = team_data.get("team", {}).get("abbreviation", "???")

        if not pitchers:
            continue

        starter_key  = f"ID{pitchers[0]}"
        starter_data = players.get(starter_key, {})
        stats        = starter_data.get("stats", {}).get("pitching", {})
        name         = starter_data.get("person", {}).get("fullName", "Unknown")

        ip_raw = float(stats.get("inningsPitched", 0) or 0)
        if ip_raw == 0:
            log.warning(f"  Starter {name} has 0 IP — skipping (opener?)")
            continue

        # Normalise IP to X.0, X.1, X.2 notation
        ip = _outs_to_ip(ip_raw)
        h   = int(stats.get("hits",        0) or 0)
        er  = int(stats.get("earnedRuns",  0) or 0)
        bb  = int(stats.get("baseOnBalls", 0) or 0)
        so  = int(stats.get("strikeOuts",  0) or 0)

        results.append({
            "pitcher_name": name,
            "team":         team_name,
            "ip":           ip,
            "h":            h,
            "er":           er,
            "bb":           bb,
            "so":           so,
        })

    return results


def _outs_to_ip(raw: float) -> float:
    """Convert MLB API's decimal IP (6.333…) to standard X.1/X.2 notation."""
    full = int(raw)
    frac = round(raw - full, 3)
    if frac < 0.17:
        thirds = 0
    elif frac < 0.5:
        thirds = 1
    else:
        thirds = 2
    return round(full + thirds / 10, 1)


# ── X (Twitter) posting ─────────────────────────────────────────────────────────

def post_to_x(tweet_text: str) -> Optional[str]:
    """
    Post a tweet using the X API v2.
    Returns the tweet ID on success, None on failure.

    Requires env vars:
        X_API_KEY, X_API_SECRET,
        X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET
    """
    try:
        from requests_oauthlib import OAuth1Session
    except ImportError:
        log.error("requests_oauthlib not installed. Run: pip install requests-oauthlib")
        return None

    api_key    = os.environ["X_API_KEY"]
    api_secret = os.environ["X_API_SECRET"]
    token      = os.environ["X_ACCESS_TOKEN"]
    token_sec  = os.environ["X_ACCESS_TOKEN_SECRET"]

    oauth = OAuth1Session(api_key, api_secret, token, token_sec)
    resp  = oauth.post(
        "https://api.twitter.com/2/tweets",
        json={"text": tweet_text},
    )

    if resp.status_code == 201:
        tweet_id = resp.json()["data"]["id"]
        log.info(f"  Posted tweet: {tweet_id}")
        return tweet_id
    else:
        log.error(f"  X API error {resp.status_code}: {resp.text}")
        return None


# ── Tweet formatter with thread support ────────────────────────────────────────

def format_tweet(result: ScorigamiResult) -> str:
    """Delegates to the result's own formatter."""
    return result.format_tweet()


# ── Daily log of processed games ───────────────────────────────────────────────

PROCESSED_LOG = "data/processed_games.json"


def load_processed() -> set:
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG) as f:
            return set(json.load(f))
    return set()


def save_processed(processed: set):
    os.makedirs("data", exist_ok=True)
    with open(PROCESSED_LOG, "w") as f:
        json.dump(sorted(processed), f)


# ── Main ────────────────────────────────────────────────────────────────────────

def run(game_date: str, dry_run: bool):
    os.makedirs("logs", exist_ok=True)

    log.info(f"=== Pitcher Scorigami Daily Run | {game_date} | dry_run={dry_run} ===")

    # 1. Fetch completed games
    log.info("Fetching completed games from MLB Stats API…")
    try:
        game_pks = get_completed_games(game_date)
    except Exception as e:
        log.error(f"Failed to fetch schedule: {e}")
        sys.exit(1)

    if not game_pks:
        log.info("No completed games found for today. Exiting.")
        return

    log.info(f"Found {len(game_pks)} completed game(s)")

    # 2. Load already-processed games (idempotency)
    processed = load_processed()

    # 3. Collect all starting pitcher outings
    all_outings = []
    for pk in game_pks:
        if str(pk) in processed:
            log.info(f"  Game {pk} already processed — skipping")
            continue
        try:
            starters = get_starting_pitchers(pk)
            for s in starters:
                s["game_pk"] = pk
            all_outings.extend(starters)
            log.info(f"  Game {pk}: {[s['pitcher_name'] for s in starters]}")
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"  Error fetching game {pk}: {e}")

    if not all_outings:
        log.info("No new outings to process.")
        return

    # 4. Run Scorigami lookups
    log.info(f"\nRunning Scorigami lookups for {len(all_outings)} starter(s)…")
    results: list[ScorigamiResult] = []

    with ScorigamiEngine(DB_PATH) as engine:
        stats = engine.db_stats()
        log.info(f"DB: {stats['total_outings']:,} outings | "
                 f"{stats['unique_statlines']:,} unique statlines | "
                 f"{stats['earliest']} – {stats['latest']}")

        for outing in all_outings:
            result = engine.lookup(
                pitcher_name=outing["pitcher_name"],
                game_date=game_date,
                team=outing["team"],
                ip=outing["ip"],
                h=outing["h"],
                er=outing["er"],
                bb=outing["bb"],
                so=outing["so"],
            )
            results.append(result)
            log.info(
                f"  {result.pitcher_name}: {result.ip_display()} IP "
                f"{result.h} H {result.er} ER {result.bb} BB {result.so} K | "
                f"count={result.count} scorigami={result.is_scorigami}"
            )

        # 5. Post tweets (scorigamis first, then by rarity)
        results.sort(key=lambda r: (not r.is_scorigami, r.count))

        tweet_output = []
        for result in results:
            tweet_text = format_tweet(result)
            tweet_output.append({"pitcher": result.pitcher_name, "tweet": tweet_text})

            if dry_run:
                print("\n" + "─" * 60)
                print(tweet_text)
                print("─" * 60)
            else:
                tweet_id = post_to_x(tweet_text)
                time.sleep(3)  # avoid rate limits between tweets

        # 6. Insert today's outings into DB so they count for future lookups
        season = int(game_date[:4])
        for outing in all_outings:
            engine.insert_outing(
                pitcher_name=outing["pitcher_name"],
                game_date=game_date,
                team=outing["team"],
                ip=outing["ip"],
                h=outing["h"],
                er=outing["er"],
                bb=outing["bb"],
                so=outing["so"],
                season=season,
            )

    # 7. Save processed game PKs
    for outing in all_outings:
        processed.add(str(outing["game_pk"]))
    save_processed(processed)

    # 8. Write dry-run output JSON for review
    if dry_run:
        out_path = f"data/dry_run_{game_date}.json"
        with open(out_path, "w") as f:
            json.dump(tweet_output, f, indent=2)
        log.info(f"\nDry-run output written to {out_path}")

    log.info(f"\nDone. Processed {len(results)} starter(s).")


def main():
    parser = argparse.ArgumentParser(description="Pitcher Scorigami daily runner")
    parser.add_argument(
        "--date",
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print tweets instead of posting to X",
    )
    args = parser.parse_args()

    # Default to yesterday (games should be fully completed)
    if args.date:
        game_date = args.date
    else:
        game_date = (date.today() - timedelta(days=1)).isoformat()

    run(game_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
