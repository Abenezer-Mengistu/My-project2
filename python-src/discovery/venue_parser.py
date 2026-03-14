"""
Venue parser — handles Excel ingestion using Pandas.
Part of the modular 'discovery/' component.
"""
from __future__ import annotations

import pandas as pd
from typing import List, Dict, Any

from utils.logger import logger
from database.models.ticketing.venue import Venue


class VenueParser:
    """Parses venue lists from various sources, primarily Excel."""

    @staticmethod
    def from_excel(file_path: str) -> List[Dict[str, Any]]:
        """
        Ingest venue list from an Excel sheet.
        Expected columns: name, stubhub_url, handler, location (optional).
        """
        logger.info(f"Ingesting venues from Excel: {file_path}")
        try:
            df = pd.read_excel(file_path)
            # Normalize column names
            df.columns = [col.lower().replace("_", " ").strip() for col in df.columns]
            
            venues = []
            for _, row in df.iterrows():
                venue_data = {
                    "name": str(row.get("name", "")).strip(),
                    "stubhub_url": str(row.get("stubhub url", row.get("url", ""))).strip(),
                    "handler": str(row.get("handler", "stubhub-discovery")).strip(),
                    "location": str(row.get("location", "")) if pd.notna(row.get("location")) else None
                }
                if venue_data["name"] and venue_data["stubhub_url"]:
                    venues.append(venue_data)
                    
            logger.info(f"Successfully parsed {len(venues)} venues from Excel")
            return venues
        except Exception as e:
            logger.error(f"Failed to parse Excel file: {e}")
            raise
    @staticmethod
    def to_excel(venues: List[Dict[str, Any]], file_path: str) -> None:
        """
        Save venue list to an Excel sheet.
        """
        logger.info(f"Saving {len(venues)} venues to Excel: {file_path}")
        try:
            df = pd.DataFrame(venues)
            # Reorder columns to a standard format if they exist
            standard_cols = ["name", "stubhub_url", "handler", "location"]
            cols = [c for c in standard_cols if c in df.columns] + [c for c in df.columns if c not in standard_cols]
            df = df[cols]
            
            df.to_excel(file_path, index=False)
            logger.info(f"Successfully saved {len(venues)} venues to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save Excel file: {e}")
            raise
