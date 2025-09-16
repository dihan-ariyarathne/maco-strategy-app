from __future__ import annotations

from app.ingestion.yahoo_backfill import run as run_yahoo


def main() -> None:
    run_yahoo()


if __name__ == "__main__":
    main()
