"""
BackfillProgress — resume-safe progress tracking for the historical backfill.

Writes one line per completed (station_ref, year) pair to a flat log file.
On restart, loads the file and skips any pair already completed — preventing
duplicate API calls and duplicate inserts.

Separate instances are used for river levels and weather so each backfill
can be run and resumed independently.
"""

from __future__ import annotations
import os


class BackfillProgress:
    """
    Tracks completed (station_ref, year) pairs for a single backfill job.

    Usage:
        progress = BackfillProgress("logs/backfill_river_progress.log")

        if progress.is_done("1491TH", 2005):
            continue   # already fetched — skip API call entirely

        # ... fetch from API and insert to DB ...

        progress.mark_done("1491TH", 2005)  # persist immediately
    """

    def __init__(self, log_path: str) -> None:
        self.log_path = log_path
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        self._done: set[str] = self._load()

    def _load(self) -> set[str]:
        """Load completed keys from log file. Returns empty set if file does not exist."""
        if not os.path.exists(self.log_path):
            return set()
        with open(self.log_path) as f:
            return {line.strip() for line in f if line.strip()}

    def _key(self, station_ref: str, year: int) -> str:
        return f"{station_ref}:{year}"

    def is_done(self, station_ref: str, year: int) -> bool:
        """Return True if this (station_ref, year) was already completed."""
        return self._key(station_ref, year) in self._done

    def mark_done(self, station_ref: str, year: int) -> None:
        """
        Mark this (station_ref, year) as complete. Writes to log immediately
        so progress survives a crash on the next iteration.
        """
        key = self._key(station_ref, year)
        if key not in self._done:
            self._done.add(key)
            with open(self.log_path, "a") as f:
                f.write(f"{key}\n")

    def count_done(self) -> int:
        """Return total number of completed station-year pairs."""
        return len(self._done)

    def reset(self) -> None:
        """
        Delete the log file and clear in-memory state.
        Use only when you want to re-run the entire backfill from scratch.
        """
        self._done.clear()
        if os.path.exists(self.log_path):
            os.remove(self.log_path)
