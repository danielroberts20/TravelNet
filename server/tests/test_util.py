"""
test_util.py — Unit tests for util.py haversine helpers.

Covers:
  - haversine_m: same point → 0, known distance pair, symmetry
  - haversine_km: same point → 0, relationship haversine_km ≈ haversine_m / 1000
"""

import math
import pytest

from util import haversine_m, haversine_km


class TestHaversineM:

    def test_same_point_is_zero(self):
        assert haversine_m(51.5074, -0.1278, 51.5074, -0.1278) == pytest.approx(0.0)

    def test_known_distance_london_paris(self):
        # London (51.5074, -0.1278) to Paris (48.8566, 2.3522) ≈ 340 km
        dist = haversine_m(51.5074, -0.1278, 48.8566, 2.3522)
        assert 330_000 < dist < 350_000

    def test_symmetry(self):
        d1 = haversine_m(51.5074, -0.1278, 48.8566, 2.3522)
        d2 = haversine_m(48.8566, 2.3522, 51.5074, -0.1278)
        assert d1 == pytest.approx(d2)

    def test_returns_float(self):
        assert isinstance(haversine_m(0, 0, 0, 1), float)

    def test_equatorial_one_degree_longitude(self):
        # At the equator, 1° longitude ≈ 111,195 m
        dist = haversine_m(0.0, 0.0, 0.0, 1.0)
        assert 111_000 < dist < 112_000


class TestHaversineKm:

    def test_same_point_is_zero(self):
        assert haversine_km(51.5074, -0.1278, 51.5074, -0.1278) == pytest.approx(0.0)

    def test_consistent_with_metres_version(self):
        # haversine_km should equal haversine_m / 1000 to within rounding tolerance
        lat1, lon1, lat2, lon2 = 51.5074, -0.1278, 48.8566, 2.3522
        km = haversine_km(lat1, lon1, lat2, lon2)
        m = haversine_m(lat1, lon1, lat2, lon2)
        assert km == pytest.approx(m / 1000, rel=1e-3)

    def test_known_distance_london_paris_km(self):
        dist = haversine_km(51.5074, -0.1278, 48.8566, 2.3522)
        assert 330 < dist < 350

    def test_symmetry(self):
        d1 = haversine_km(0.0, 0.0, 10.0, 10.0)
        d2 = haversine_km(10.0, 10.0, 0.0, 0.0)
        assert d1 == pytest.approx(d2)
