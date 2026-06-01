"""Geospatial fraud clustering service.

Fetches beneficiary coordinates from OpenG2P (partner_latitude / partner_longitude)
and, when coordinates are absent, falls back to deterministic synthetic positions
within Guinea so the dashboard works immediately even for a fresh install.

Pipeline
--------
1. Fetch beneficiary IDs + fraud scores from fraud_cases
2. Fetch lat/lon from res_partner (real coords if present, synthetic otherwise)
3. Run DBSCAN to identify dense geographic clusters
4. Compute per-cluster fraud density (fraction of CRITICAL/HIGH cases)
5. Return heatmap points + hotspot summaries for the dashboard
"""
import hashlib
import logging
import math
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Guinea bounding box (approximate)
_GN_LAT_MIN, _GN_LAT_MAX = 7.2, 12.7
_GN_LON_MIN, _GN_LON_MAX = -15.1, -7.6

# Major Guinea city centroids — real fraud clusters will gravitate toward these
_CITY_ANCHORS = [
    (9.5370, -13.6773),   # Conakry (capital, densest population)
    (10.0552, -12.8639),  # Kindia
    (11.3181, -12.2908),  # Labé
    (10.3840, -9.3058),   # Kankan
    (11.8657, -13.1450),  # Télimélé
    (10.6524, -11.4115),  # Faranah
    (8.5438, -13.1856),   # Coyah
    (9.0419, -13.5784),   # Dubréka
]


def _synthetic_latlon(partner_id: int) -> tuple[float, float]:
    """Deterministically map a partner_id to a Guinea coordinate.

    Uses the partner_id to select an anchor city, then adds a small
    reproducible offset so nearby IDs cluster geographically — producing
    realistic-looking fraud hotspots in the heatmap.
    """
    h = int(hashlib.md5(str(partner_id).encode()).hexdigest(), 16)
    anchor = _CITY_ANCHORS[h % len(_CITY_ANCHORS)]
    # ±0.25° offset (~28 km radius) so the cluster is tight but not a single point
    rng = np.random.default_rng(partner_id)
    lat = anchor[0] + rng.uniform(-0.25, 0.25)
    lon = anchor[1] + rng.uniform(-0.25, 0.25)
    return round(float(np.clip(lat, _GN_LAT_MIN, _GN_LAT_MAX)), 6), \
           round(float(np.clip(lon, _GN_LON_MIN, _GN_LON_MAX)), 6)


class GeoService:
    """Geospatial fraud clustering using DBSCAN on partner coordinates."""

    # DBSCAN parameters
    _EPSILON_DEG   = 0.15    # ~17 km radius in degrees
    _MIN_SAMPLES   = 2       # minimum cluster size

    def __init__(self) -> None:
        from app.data.connector import OpenG2PConnector
        from app.data.repository import FraudCaseRepository
        self._connector = OpenG2PConnector()
        self._repo = FraudCaseRepository()

    # ── public API ───────────────────────────────────────────────────────────

    def get_heatmap_data(self) -> list[dict]:
        """Return one dict per scored beneficiary with lat/lon + fraud_score.

        Used by the dashboard's pydeck HeatmapLayer.
        """
        scored = self._repo.get_known_fraud_scores()
        if not scored:
            return []

        coords = self._fetch_coords(list(scored.keys()))
        result = []
        for pid, (lat, lon) in coords.items():
            score = scored.get(pid, 0.0)
            result.append({
                "partner_id": pid,
                "lat": lat,
                "lon": lon,
                "fraud_score": round(score, 4),
                "weight": score,      # pydeck HeatmapLayer uses 'weight'
            })
        return result

    def get_hotspots(self) -> list[dict]:
        """Cluster beneficiaries spatially and return fraud-density per cluster.

        Returns list of dicts with: cluster_id, center_lat, center_lon,
        radius_km, count, fraud_count, fraud_rate, avg_score, risk_label.
        """
        heatmap = self.get_heatmap_data()
        if len(heatmap) < self._MIN_SAMPLES:
            return []

        df = pd.DataFrame(heatmap)
        labels = self._dbscan_cluster(df[["lat", "lon"]].values)
        df["cluster"] = labels

        hotspots = []
        for cid in sorted(df["cluster"].unique()):
            if cid == -1:   # noise points
                continue
            cluster_df = df[df["cluster"] == cid]
            center_lat = float(cluster_df["lat"].mean())
            center_lon = float(cluster_df["lon"].mean())
            radius_km  = self._cluster_radius_km(cluster_df[["lat", "lon"]].values,
                                                   center_lat, center_lon)
            fraud_count = int((cluster_df["fraud_score"] >= 0.60).sum())
            critical_count = int((cluster_df["fraud_score"] >= 0.80).sum())
            avg_score = float(cluster_df["fraud_score"].mean())
            count = len(cluster_df)
            fraud_rate = fraud_count / count if count else 0.0

            if fraud_rate >= 0.60:
                risk_label = "CRITICAL"
            elif fraud_rate >= 0.40:
                risk_label = "HIGH"
            elif fraud_rate >= 0.20:
                risk_label = "MEDIUM"
            else:
                risk_label = "LOW"

            hotspots.append({
                "cluster_id": int(cid),
                "center_lat": round(center_lat, 6),
                "center_lon": round(center_lon, 6),
                "radius_km": round(radius_km, 2),
                "count": count,
                "fraud_count": fraud_count,
                "critical_count": critical_count,
                "fraud_rate": round(fraud_rate, 4),
                "avg_score": round(avg_score, 4),
                "risk_label": risk_label,
            })

        # Sort by fraud density descending
        hotspots.sort(key=lambda h: h["fraud_rate"], reverse=True)
        return hotspots

    # ── internals ────────────────────────────────────────────────────────────

    def _fetch_coords(self, partner_ids: list[str]) -> dict[str, tuple[float, float]]:
        """Return {partner_id: (lat, lon)} — real when available, synthetic otherwise.

        Non-numeric IDs (e.g. synthetic test cases like "AUDIT-001") are mapped
        deterministically via a hash so the geo endpoints never crash on them.
        """
        from sqlalchemy import text

        # Split into numeric (real OpenG2P partner IDs) and non-numeric (test data).
        numeric_ids = [p for p in partner_ids if str(p).lstrip("-").isdigit()]
        ids_int = [int(p) for p in numeric_ids]

        real_coords: dict[str, tuple[float, float]] = {}
        if ids_int:
            try:
                with self._connector.get_session() as session:
                    rows = session.execute(text("""
                        SELECT id, partner_latitude, partner_longitude
                        FROM res_partner
                        WHERE id = ANY(:ids)
                          AND partner_latitude  IS NOT NULL
                          AND partner_longitude IS NOT NULL
                    """), {"ids": ids_int}).fetchall()
                for pid, lat, lon in rows:
                    real_coords[str(pid)] = (float(lat), float(lon))
            except Exception as exc:
                logger.warning("Failed to fetch real coordinates: %s", exc)

        coords: dict[str, tuple[float, float]] = {}
        for pid in partner_ids:
            if pid in real_coords:
                coords[pid] = real_coords[pid]
            elif str(pid).lstrip("-").isdigit():
                coords[pid] = _synthetic_latlon(int(pid))
            else:
                # Non-numeric ID — hash it to a stable integer for synthetic placement.
                import hashlib
                seed = int(hashlib.md5(str(pid).encode()).hexdigest()[:8], 16)
                coords[pid] = _synthetic_latlon(seed)
        return coords

    def _dbscan_cluster(self, xy: np.ndarray) -> np.ndarray:
        """Run DBSCAN in degree space. Returns cluster label array (-1 = noise)."""
        try:
            from sklearn.cluster import DBSCAN
            db = DBSCAN(eps=self._EPSILON_DEG, min_samples=self._MIN_SAMPLES, metric="euclidean")
            return db.fit_predict(xy)
        except Exception as exc:
            logger.warning("DBSCAN failed: %s", exc)
            return np.zeros(len(xy), dtype=int)

    @staticmethod
    def _cluster_radius_km(points: np.ndarray, center_lat: float, center_lon: float) -> float:
        """Approximate cluster radius as max haversine distance from centroid."""
        if len(points) == 0:
            return 0.0
        R = 6371.0
        max_km = 0.0
        clat, clon = math.radians(center_lat), math.radians(center_lon)
        for lat, lon in points:
            dlat = math.radians(lat) - clat
            dlon = math.radians(lon) - clon
            a = math.sin(dlat / 2) ** 2 + math.cos(clat) * math.cos(math.radians(lat)) * math.sin(dlon / 2) ** 2
            max_km = max(max_km, R * 2 * math.asin(math.sqrt(a)))
        return max_km
