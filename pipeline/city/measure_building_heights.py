#!/usr/bin/env python3
"""Measure every Vancouver building's height from published LiDAR.

    python3 pipeline/city/measure_building_heights.py [--limit-tiles N]
        -> raw.ca_vancouver_building_heights_lidar  (one row per 2015 footprint)

WHY THIS EXISTS. The City publishes two footprint layers. The 2009 one carries
LiDAR heights; the 2015 one (154,124 outlines, newer and more complete) carries
none. A common approach draws every building at the median LiDAR return over
its footprint, and a map that extruded Vancouver's 2015 outlines to a guessed
height would be the "flat-extruded boxes" failure slot 6 names.

THE INSTRUMENT. Natural Resources Canada republishes the region's airborne
LiDAR as 1 m cloud-optimised rasters (HRDEM, Open Government Licence – Canada):
a digital SURFACE model (first return: roofs, trees) and a digital TERRAIN model
(bare earth). Height above ground = DSM − DTM. For every footprint we take the
pixels whose centres fall inside it and publish the distribution, not a single
number, as published LiDAR building datasets do:

    n_px        pixels inside the outline (≈ m²)
    h_p50       median height above ground — the number the map extrudes to
    h_p90       90th percentile — a tower's roof line, robust to one antenna
    h_max       maximum — kept, never drawn alone (masts, cranes, birds)
    h_mean      mean
    ground_p50  median DTM elevation (m, CGVD2013) — the building's own ground
    h_p25, h_p75, h_p95  further quantiles
    share_upper the share of the outline's pixels at or above the midpoint of
                h_p50 and h_p90 — how much of the plan is the TOWER when one
                outline holds a tower on a podium (the encoder's roof levels)

WHICH SURVEY. `BC-Lower_Mainland_2016-1m` — flown 2016, one year after the 2015
outlines were traced, so an outline and its heights describe the same city. The
City's own 2013 survey is also published and is older than the outlines.

WHAT IT DOES NOT MEASURE, stated because a reader will assume otherwise:
  * a building finished after the 2016 flight has no footprint here at all;
  * a roof under a tree canopy reads as the tree (median resists partial cover);
  * the raster is 1 m, so an outline under ~4 m² has too few pixels to trust —
    n_px is published so the encoder can refuse it.

Reads the footprints from raw (the adapter's own table), writes to raw: this is
a MEASURED source, and like every source it enters the warehouse at raw and is
typed in staging. Deterministic given the same rasters and footprints.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer
from rasterio.features import rasterize
from rasterio.windows import from_bounds
from shapely.geometry import shape
from shapely.ops import transform as shp_transform

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # pipeline/
from common import PROJECT_ID, get_bigquery_client, RAW_DATASET, CORE_DATASET, MARTS_DATASET  # noqa: E402

SURVEY = "BC-Lower_Mainland_2016-1m"
BASE = "https://canelevation-dem.s3.ca-central-1.amazonaws.com/hrdem-lidar"
DSM = f"{BASE}/{SURVEY}-dsm.tif"
DTM = f"{BASE}/{SURVEY}-dtm.tif"
STAC_ITEM = "https://datacube.services.geo.ca/stac/api/collections/hrdem-lidar/items/" + SURVEY
SRC_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.ca_vancouver_building_footprints_2015"
OUT_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.ca_vancouver_building_heights_lidar"
TILE_M = 1000.0


def load_footprints(client) -> tuple[np.ndarray, list]:
    rows = client.query(f"SELECT object_id, geom FROM `{SRC_TABLE}` ORDER BY object_id").result()
    to3979 = Transformer.from_crs("EPSG:4326", "EPSG:3979", always_xy=True).transform
    ids, geoms = [], []
    for r in rows:
        try:
            g = shape(json.loads(r.geom)["geometry"])
        except Exception:
            continue
        if g.is_empty:
            continue
        ids.append(int(r.object_id))
        geoms.append(shp_transform(to3979, g))
    return np.array(ids, dtype=np.int64), geoms


def measure(ids: np.ndarray, geoms: list, limit_tiles: int | None) -> pd.DataFrame:
    bounds = np.array([g.bounds for g in geoms])  # minx miny maxx maxy
    x0 = math.floor(bounds[:, 0].min() / TILE_M) * TILE_M
    y0 = math.floor(bounds[:, 1].min() / TILE_M) * TILE_M
    x1 = math.ceil(bounds[:, 2].max() / TILE_M) * TILE_M
    y1 = math.ceil(bounds[:, 3].max() / TILE_M) * TILE_M
    labels_all, h_all, g_all = [], [], []
    tiles = [(tx, ty) for tx in np.arange(x0, x1, TILE_M) for ty in np.arange(y0, y1, TILE_M)]
    if limit_tiles:
        tiles = tiles[:limit_tiles]
    with rasterio.open(DSM) as dsm, rasterio.open(DTM) as dtm:
        assert dsm.crs.to_epsg() == 3979 and dtm.crs.to_epsg() == 3979
        for k, (tx, ty) in enumerate(tiles):
            sel = np.nonzero((bounds[:, 2] > tx) & (bounds[:, 0] < tx + TILE_M)
                             & (bounds[:, 3] > ty) & (bounds[:, 1] < ty + TILE_M))[0]
            if sel.size == 0:
                continue
            win = from_bounds(tx, ty, tx + TILE_M, ty + TILE_M, dsm.transform).round_offsets().round_lengths()
            s = dsm.read(1, window=win, boundless=True, fill_value=dsm.nodata)
            t = dtm.read(1, window=win, boundless=True, fill_value=dtm.nodata)
            tr = dsm.window_transform(win)
            lab = rasterize(((geoms[i], int(i) + 1) for i in sel), out_shape=s.shape,
                            transform=tr, fill=0, dtype="int32", all_touched=False)
            ok = (lab > 0) & (s != dsm.nodata) & (t != dtm.nodata)
            if not ok.any():
                continue
            labels_all.append(lab[ok] - 1)
            h_all.append((s[ok] - t[ok]).astype(np.float32))
            g_all.append(t[ok].astype(np.float32))
            if k % 25 == 0:
                print(f"  tile {k + 1}/{len(tiles)}: {sel.size} outlines, {int(ok.sum())} px", flush=True)
    lab = np.concatenate(labels_all)
    h = np.concatenate(h_all)
    g = np.concatenate(g_all)
    order = np.lexsort((h, lab))            # by label, then height ascending
    lab, h, g = lab[order], h[order], g[order]
    starts = np.flatnonzero(np.r_[True, lab[1:] != lab[:-1]])
    ends = np.r_[starts[1:], lab.size]
    out = []
    def qn(hh, p):
        return float(hh[min(hh.size - 1, max(0, int(math.ceil(p * hh.size)) - 1))])

    for s0, e0 in zip(starts, ends):
        hh = h[s0:e0]
        n = hh.size
        p50 = float(hh[(n - 1) // 2] if n % 2 else (hh[n // 2 - 1] + hh[n // 2]) / 2)
        p90 = qn(hh, 0.9)
        mid = (p50 + p90) / 2
        out.append({
            "h_p25": qn(hh, 0.25), "h_p75": qn(hh, 0.75), "h_p95": qn(hh, 0.95),
            "share_upper": float((hh >= mid).sum() / n) if p90 > p50 else 0.0,
            "object_id": int(ids[lab[s0]]),
            "n_px": int(n),
            "h_p50": p50,
            "h_p90": p90,
            "h_max": float(hh[-1]),
            "h_mean": float(hh.mean()),
            "ground_p50": float(np.median(g[s0:e0])),
        })
    return pd.DataFrame(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-tiles", type=int, default=None, help="measure only the first N tiles (smoke test; does not load)")
    args = ap.parse_args()
    client = get_bigquery_client()
    ids, geoms = load_footprints(client)
    print(f"footprints: {len(ids)}", flush=True)
    df = measure(ids, geoms, args.limit_tiles)
    print(f"measured: {len(df)} of {len(ids)} outlines", flush=True)
    print(df.describe().T.to_string(), flush=True)
    if args.limit_tiles:
        return 0
    df["survey_id"] = SURVEY
    df["dsm_url"] = DSM
    df["dtm_url"] = DTM
    df["source_url"] = STAC_ITEM
    df["license"] = "Open Government Licence - Canada"
    df["_synced_at"] = datetime.now(timezone.utc)
    from google.cloud import bigquery
    job = client.load_table_from_dataframe(
        df, OUT_TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    print(f"loaded {len(df)} rows → {OUT_TABLE}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
