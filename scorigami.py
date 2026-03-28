"""
scorigami.py
------------
Core lookup engine for Pitcher Scorigami.

Given a statline (IP, H, ER, BB, SO), queries the historical database
and returns:
  - count: how many times this exact statline has been recorded
  - last_occurrence: the most recent prior date + pitcher name
  - is_scorigami: True if count == 0 (never seen before)
"""

import sqlite3
import os
from dataclasses import dataclass
from typing import Optional
from datetime import date

DB_PATH = os.environ.get("SCORIGAMI_DB_PATH", "data/pitcher_scorigami.db")


@dataclass
class ScorigamiResult:
    pitcher_name: str
    game_date: str          # YYYY-MM-DD (today's game)
    team: str
    ip: float
    h: int
    er: int
    bb: int
    so: int
    count: int              # number of times this statline has occurred (historically)
    last_pitcher: Optional[str]
    last_date: Optional[str]
    is_scorigami: bool

    def ip_display(self) -> str:
        """Return '6.1' as '6⅓', '6.2' as '6⅔', etc."""
        frac = round(self.ip % 1, 1)
        full = int(self.ip)
        if frac == 0.0:
            return f"{full}"
        elif frac == 0.1:
            return f"{full}⅓"
        else:
            return f"{full}⅔"

    def format_tweet(self) -> str:
        """
        Format the tweet text for this outing.
        Stays under 280 characters.
        """
        ip_str = self.ip_display()
        statline = f"{ip_str} IP, {self.h} H, {self.er} ER, {self.bb} BB, {self.so} K"

        if self.is_scorigami:
            return (
                f"⚾ PITCHER SCORIGAMI! ⚾\n\n"
                f"{self.pitcher_name} ({self.team}) just threw a statline NEVER seen before in MLB history!\n\n"
                f"📊 {statline}\n\n"
                f"This combination of IP/H/ER/BB/K has NEVER been recorded by a starting pitcher. Ever. 🔥\n\n"
                f"#PitcherScorigami #MLB #Baseball"
            )
        elif self.count == 1:
            return (
                f"⚾ Pitcher Scorigami Check\n\n"
                f"{self.pitcher_name} ({self.team}): {statline}\n\n"
                f"This statline has been done just 1 time before in MLB history.\n"
                f"Last: {self.last_pitcher} on {self._fmt_date(self.last_date)}\n\n"
                f"#PitcherScorigami #MLB"
            )
        else:
            return (
                f"⚾ Pitcher Scorigami Check\n\n"
                f"{self.pitcher_name} ({self.team}): {statline}\n\n"
                f"This statline has been recorded {self.count:,} times in MLB history.\n"
                f"Most recent: {self.last_pitcher} ({self._fmt_date(self.last_date)})\n\n"
                f"#PitcherScorigami #MLB"
            )

    @staticmethod
    def _fmt_date(date_str: Optional[str]) -> str:
        if not date_str:
            return "unknown"
        try:
            d = date.fromisoformat(date_str)
            return d.strftime("%B %-d, %Y")
        except Exception:
            return date_str


class ScorigamiEngine:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self):
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row

    def close(self):
        if self._conn:
            self._conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *_):
        self.close()

    def lookup(
        self,
        pitcher_name: str,
        game_date: str,
        team: str,
        ip: float,
        h: int,
        er: int,
        bb: int,
        so: int,
    ) -> ScorigamiResult:
        """
        Look up a statline against all historical data.
        game_date is excluded from the count (we only compare to prior games).
        """
        assert self._conn, "Call connect() first"

        # Count all historical occurrences BEFORE today
        count_row = self._conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM outings
            WHERE ip=? AND h=? AND er=? AND bb=? AND so=?
              AND game_date < ?
            """,
            (ip, h, er, bb, so, game_date),
        ).fetchone()
        count = count_row["cnt"] if count_row else 0

        # Find the most recent prior occurrence
        last_row = self._conn.execute(
            """
            SELECT pitcher_name, game_date
            FROM outings
            WHERE ip=? AND h=? AND er=? AND bb=? AND so=?
              AND game_date < ?
            ORDER BY game_date DESC
            LIMIT 1
            """,
            (ip, h, er, bb, so, game_date),
        ).fetchone()

        last_pitcher = last_row["pitcher_name"] if last_row else None
        last_date    = last_row["game_date"]    if last_row else None

        return ScorigamiResult(
            pitcher_name=pitcher_name,
            game_date=game_date,
            team=team,
            ip=ip,
            h=h,
            er=er,
            bb=bb,
            so=so,
            count=count,
            last_pitcher=last_pitcher,
            last_date=last_date,
            is_scorigami=(count == 0),
        )

    def bulk_lookup(self, outings: list[dict]) -> list[ScorigamiResult]:
        """Look up multiple outings at once."""
        return [self.lookup(**o) for o in outings]

    def insert_outing(
        self,
        pitcher_name: str,
        game_date: str,
        team: str,
        ip: float,
        h: int,
        er: int,
        bb: int,
        so: int,
        season: int,
    ):
        """Add a new outing to the database after posting."""
        self._conn.execute(
            "INSERT OR IGNORE INTO outings "
            "(pitcher_name, game_date, team, ip, h, er, bb, so, season) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (pitcher_name, game_date, team, ip, h, er, bb, so, season),
        )
        self._conn.commit()

    def db_stats(self) -> dict:
        """Return summary stats about the database."""
        row = self._conn.execute(
            "SELECT COUNT(*) as total, MIN(game_date) as earliest, MAX(game_date) as latest "
            "FROM outings"
        ).fetchone()
        unique = self._conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT ip,h,er,bb,so FROM outings)"
        ).fetchone()[0]
        return {
            "total_outings": row["total"],
            "earliest":      row["earliest"],
            "latest":        row["latest"],
            "unique_statlines": unique,
        }
