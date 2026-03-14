"""
Seed Venues — command line tool for Excel ingestion.
Fulfills Phase 1 Requirement 2 of the Technical Plan.
"""
import asyncio
import os
import sys

# Add current dir to path
sys.path.append(os.getcwd())

from discovery.venue_parser import VenueParser
from database.repositories.ticketing.venues import get_venue_repository
from database.connection import initialize_orm, close_orm
from utils.logger import logger

async def seed_from_excel(file_path: str):
    if not os.path.exists(file_path):
        logger.error(f"Excel file not found: {file_path}")
        return

    await initialize_orm()
    try:
        venues_data = VenueParser.from_excel(file_path)
        repo = get_venue_repository()
        
        for data in venues_data:
            venue = await repo.upsert_venue(data)
            logger.info(f"Seeded venue: {venue.name} (ID: {venue._id})")
            
        logger.info(f"Done! Seeded {len(venues_data)} venues.")
    finally:
        await close_orm()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python seed_venues.py <path_to_excel>")
        sys.exit(1)
        
    asyncio.run(seed_from_excel(sys.argv[1]))
