from __future__ import annotations

from app.ingestion.finnhub_snapshot import run as run_snapshot


def main() -> None:
    run_snapshot()


if __name__ == "__main__":
    main()
