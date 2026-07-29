"""Administrative projection rebuild; deliberately not exposed through FastAPI."""
from __future__ import annotations

import os

from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector


def main() -> None:
    dsn = os.environ["THYPHON_DATABASE_URL"]
    count = PostgresAuctionOverviewProjector(dsn).rebuild()
    print(f"Rebuilt auction-overview from {count} auction events")


if __name__ == "__main__":
    main()
