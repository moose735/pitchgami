"""
scorigami.py
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
    game_date: str
    team: str
    ip: float
    h: int
    er: int
    bb: int
    so: int
    count: int
    last_pitcher: Optional[str]
    last_date: Optional[str]
    is_scorigami: bool
    total_unique: int = 0
    season_unique: int = 0

    def ip_display(self) -> str:
        frac = round(self.ip % 1, 1)
        full = int(self.ip)
        if frac == 0.0:
            return f"{full}"
        elif frac == 0.1:
            return f"{full}\u2153"
        else:
            return f"{full}\u2154"

    def format_tweet(self, season_rank: int = 0, total_rank: int = 0) -> str:
        ip_str   = self.ip_display()
        statline = f"{ip_str} IP, {self.h} H, {self.er} ER, {self.bb} BB, {self.so} K"
        header   = f"{self.pitcher_name} ({self.team}): {statline}"

        if self.is_scorigami:
            t = total_rank if total_rank else self.total_unique
            s = season_rank if season_rank else self.season_unique
            return (
                f"{header}\n\n"
                f"\U0001f6a8 PITCHGAMI \U0001f6a8\n\n"
                f"This combination of IP/H/ER/BB/K has NEVER been recorded by a starting pitcher. Ever. \U0001f525\n"
                f"It's the {self._ordinal(t)} unique SP statline in MLB history, "
                f"and the {self._ordinal(s)} unique combination of the {self.game_date[:4]} season.\n\n"
                f"#Pitchgami #MLB #Baseball"
            )
        else:
            recency = (
                f"This statline has been done just 1 time before in MLB history.\n"
                f"Most recent: {self.last_pitcher} ({self._fmt_date(self.last_date)})"
                if self.count == 1 else
                f"This statline has been recorded {self.count:,} times in MLB history.\n"
                f"Most recent: {self.last_pitcher} ({self._fmt_date(self.last_date)})"
            )
            return (
                f"{header}\n\n"
                f"No Pitchgami.\n\n"
                f"{recency}\n\n"
                f"#Pitchgami #MLB"
            )

    @staticmethod
    def _ordinal(n: int) -> str:
        if 11 <= (n % 100) <= 13:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n:,}{suffix}"

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
        assert self._conn, "Call connect() first"

        count_row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM outings "
            "WHERE ip=? AND h=? AND er=? AND bb=? AND so=? AND game_date < ?",
            (ip, h, er, bb, so, game_date),
        ).fetchone()
        count = count_row["cnt"] if count_row else 0

        last_row = self._conn.execute(
            "SELECT pitcher_name, game_date FROM outings "
            "WHERE ip=? AND h=? AND er=? AND bb=? AND so=? AND game_date < ? "
            "ORDER BY game_date DESC LIMIT 1",
            (ip, h, er, bb, so, game_date),
        ).fetchone()

        last_pitcher = last_row["pitcher_name"] if last_row else None
        last_date    = last_row["game_date"]    if last_row else None

        total_unique = self._conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT ip,h,er,bb,so FROM outings WHERE game_date < ?)",
            (game_date,),
        ).fetchone()[0]

        season = game_date[:4]
        season_unique = self._conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT ip,h,er,bb,so FROM outings "
            "WHERE season=? AND game_date < ?)",
            (season, game_date),
        ).fetchone()[0]

        if count == 0:
            total_unique  += 1
            season_unique += 1

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
            total_unique=total_unique,
            season_unique=season_unique,
        )

    def bulk_lookup(self, outings: list[dict]) -> list[ScorigamiResult]:
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
        self._conn.execute(
            "INSERT OR IGNORE INTO outings "
            "(pitcher_name, game_date, team, ip, h, er, bb, so, season) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (pitcher_name, game_date, team, ip, h, er, bb, so, season),
        )
        self._conn.commit()

    def db_stats(self) -> dict:
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