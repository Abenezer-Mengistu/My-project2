"""
Geometry utility for coordinate transformations.
Replaces utils/geometry.ts. Uses pyproj for EPSG transformations.
"""
from __future__ import annotations

import math
from typing import Any

import pyproj


class GeometryConverter:
    """Utility for converting coordinates between EPSG systems."""

    _transformers: dict[str, pyproj.Transformer] = {}

    @classmethod
    def _get_transformer(cls, from_epsg: int, to_epsg: int) -> pyproj.Transformer:
        key = f"{from_epsg}:{to_epsg}"
        if key not in cls._transformers:
            cls._transformers[key] = pyproj.Transformer.from_crs(
                f"EPSG:{from_epsg}", f"EPSG:{to_epsg}", always_xy=True
            )
        return cls._transformers[key]

    @classmethod
    def transform(cls, x: float, y: float, from_epsg: int, to_epsg: int) -> tuple[float, float]:
        """Transform coordinates (x, y) from one EPSG to another."""
        transformer = cls._get_transformer(from_epsg, to_epsg)
        return transformer.transform(x, y)

    @classmethod
    def wgs84_to_bng(cls, lon: float, lat: float) -> tuple[float, float]:
        """Convert WGS84 (EPSG:4326) to British National Grid (EPSG:27700)."""
        return cls.transform(lon, lat, 4326, 27700)

    @classmethod
    def bng_to_wgs84(cls, easting: float, northing: float) -> tuple[float, float]:
        """Convert British National Grid (EPSG:27700) to WGS84 (EPSG:4326)."""
        return cls.transform(easting, northing, 27700, 4326)

    @classmethod
    def wgs84_to_web_mercator(cls, lon: float, lat: float) -> tuple[float, float]:
        """Convert WGS84 (EPSG:4326) to Web Mercator (EPSG:3857)."""
        return cls.transform(lon, lat, 4326, 3857)

    @classmethod
    def get_bounding_box(cls, points: list[tuple[float, float]]) -> list[float]:
        """Get bounding box [min_lon, min_lat, max_lon, max_lat] for a list of points."""
        if not points:
            return [0.0, 0.0, 0.0, 0.0]
        
        min_x = min(p[0] for p in points)
        min_y = min(p[1] for p in points)
        max_x = max(p[0] for p in points)
        max_y = max(p[1] for p in points)
        
        return [min_x, min_y, max_x, max_y]

    @classmethod
    def calculate_distance(cls, p1: tuple[float, float], p2: tuple[float, float]) -> float:
        """Calculate Haversine distance between two WGS84 points in meters."""
        lon1, lat1 = p1
        lon2, lat2 = p2
        
        r = 6371000  # Earth radius in meters
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        
        a = math.sin(dphi / 2)**2 + \
            math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return r * c
