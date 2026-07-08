"""
GEDIMetrics v1.0.5 — subsetter.py

Changes vs v1.0.4:
  - Added `version` parameter to GEDISubsetter ('002' or '003', default '002')
  - BASE_FIELDS now version-aware:
      V003 L2A: quality path → /quality_flag (field renamed to l2a_quality_flag_rel3 in HDF5
                but the root-level /quality_flag alias is still present; we read both)
      V003 L2B: quality path → /l2b_quality_flag_rel3 (new name in V003 HDF5)
      V003 L4A: quality path → /l4_quality_flag (unchanged)
  - QUALITY_COLUMN_NAMES updated for V003 column names in the output GeoDataFrame
  - SECONDARY_QUALITY_FIELDS updated: V003 L2A retains l2a_quality_flag_rel2 as secondary;
    V003 L2B retains l2b_quality_flag_rel2 and l2a_quality_flag_rel3 as secondary flags
  - degrade_flag comment updated: V003 behaviour unchanged (still non-binary, keep == 0)
  - GEDI04_C always treated as V002 (V003 not yet available)
  - pipeline.py must pass version= when instantiating GEDISubsetter
"""

import os
import h5py
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gp
import numpy as np
import warnings
warnings.filterwarnings("ignore")

try:
    from .utils.utils import get_date_from_gedi_fn
except ImportError:
    from utils.utils import get_date_from_gedi_fn


# ── Products locked to V002 ────────────────────────────────────────────────────
# Mirror of finder._V002_ONLY_PRODUCTS — subsetter must apply same lock.
_V002_ONLY_PRODUCTS = {'GEDI04_A', 'GEDI04_C'}


# ── BASE_FIELDS ────────────────────────────────────────────────────────────────
# Variables always extracted regardless of user selection.
# Keyed by (product, version) tuple. Falls back to (product, '002') if the
# exact version key is absent (forward-compatible with future products).
#
# V003 quality flag changes (from User Guide V3, May 2026):
#   L2A: root-level /quality_flag → now named l2a_quality_flag_rel3 internally,
#        but the HDF5 root group STILL exposes it as /quality_flag for continuity.
#        We read /quality_flag and store it as 'l2a_quality_flag_rel3' in the GDF.
#   L2B: /l2b_quality_flag → renamed /l2b_quality_flag_rel3 in V003 HDF5.
#        The V002 flag is also present as /l2b_quality_flag_rel2.
#   L4A: /l4_quality_flag — unchanged between V002 and V003.

BASE_FIELDS = {
    # ── V002 ──────────────────────────────────────────────────────────────────
    ('GEDI02_A', '002'): {
        'lat': '/lat_lowestmode',
        'lon': '/lon_lowestmode',
        'shot': '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    ('GEDI02_B', '002'): {
        'lat': '/geolocation/lat_lowestmode',
        'lon': '/geolocation/lon_lowestmode',
        'shot': '/geolocation/shot_number',
        'degrade': '/geolocation/degrade_flag',
        'quality': '/l2b_quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/geolocation/delta_time',
    },
    ('GEDI04_A', '002'): {
        'lat': '/lat_lowestmode',
        'lon': '/lon_lowestmode',
        'shot': '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/l4_quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    # L4C — always V002. wsci_quality_flag is the primary filter.
    # surface_flag exists in the HDF5 but the surface filter is NOT applied:
    # wsci_quality_flag already excludes water and urban by design.
    ('GEDI04_C', '002'): {
        'lat': '/lat_lowestmode',
        'lon': '/lon_lowestmode',
        'shot': '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/wsci_quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },

    # ── V003 ──────────────────────────────────────────────────────────────────
    # L2A V003: quality_flag in the HDF5 root group now corresponds to
    # l2a_quality_flag_rel3 (the new stricter definition). We read it under
    # its root-level path and store it with the V003 column name.
    ('GEDI02_A', '003'): {
        'lat': '/lat_lowestmode',
        'lon': '/lon_lowestmode',
        'shot': '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/l2a_quality_flag_rel3',  # /quality_flag removed in V003 HDF5
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    # L2B V003: in V003 lat/lon/shot moved to root beam level (consistent with L2A V003).
    # Fallback to /geolocation/ handled in _resolve_geo_paths() at runtime.
    ('GEDI02_B', '003'): {
        'lat': '/lat_lowestmode',
        'lon': '/lon_lowestmode',
        'shot': '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/l2b_quality_flag_rel3',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    # L4A V003: l4_quality_flag unchanged. Sub-orbit granule structure handled
    # by pipeline.py — subsetter sees a normal HDF5 file either way.
    ('GEDI04_A', '003'): {
        'lat': '/lat_lowestmode',
        'lon': '/lon_lowestmode',
        'shot': '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/l4_quality_flag',       # unchanged in V003
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    # L4C V003 intentionally absent — not yet released.
}


# ── QUALITY_COLUMN_NAMES ───────────────────────────────────────────────────────
# The column name used in the output GeoDataFrame for the primary quality flag.
# Keyed by (product, version).

QUALITY_COLUMN_NAMES = {
    ('GEDI02_A', '002'): 'quality_flag',
    ('GEDI02_B', '002'): 'l2b_quality_flag',
    ('GEDI04_A', '002'): 'l4_quality_flag',
    ('GEDI04_C', '002'): 'wsci_quality_flag',
    # V003 — column names match the official V003 field names
    ('GEDI02_A', '003'): 'l2a_quality_flag_rel3',
    ('GEDI02_B', '003'): 'l2b_quality_flag_rel3',
    ('GEDI04_A', '003'): 'l4_quality_flag',       # unchanged
}


# ── SECONDARY_QUALITY_FIELDS ───────────────────────────────────────────────────
# Optional flags extracted alongside the primary quality flag.
# Not used as filters — kept for user analysis.
# Keyed by (product, version).

SECONDARY_QUALITY_FIELDS = {
    ('GEDI02_B', '002'): {
        'l2a_quality_flag': '/l2a_quality_flag',
    },
    ('GEDI04_C', '002'): {
        'l2_quality_flag': '/l2_quality_flag',
    },
    # V003 secondary flags
    # L2A V003: preserve the V002-era flag for comparison with the new rel3 flag
    ('GEDI02_A', '003'): {
        'l2a_quality_flag_rel2': '/geolocation/l2a_quality_flag_rel2',
        'l2_algrunflag': [
            '/geolocation/l2_algrunflag',
            '/l2_algrunflag',
        ],
    },
    # L2B V003: preserve both the V002-era flag and the rel3 L2A flag
    ('GEDI02_B', '003'): {
        'l2b_quality_flag_rel2': [
            '/l2b_quality_flag_rel2',
            '/geolocation/l2b_quality_flag_rel2',
        ],
        'l2a_quality_flag_rel3': [
            '/l2a_quality_flag_rel3',
            '/geolocation/l2a_quality_flag_rel3',
        ],
        'l2_algrunflag': [
            '/l2_algrunflag',
            '/geolocation/l2_algrunflag',
        ],
    },
    ('GEDI04_C', '003'): {   # future-proofing placeholder (V003 not released)
        'l2_quality_flag': '/l2_quality_flag',
    },
}


ALL_BEAMS = [
    'BEAM0000', 'BEAM0001', 'BEAM0010', 'BEAM0011',
    'BEAM0101', 'BEAM0110', 'BEAM1000', 'BEAM1011'
]


class GEDISubsetter:
    """
    Extrae y filtra datos de un granule HDF5 para un producto GEDI.

    Args:
        roi          : [UL_lat, UL_lon, LR_lat, LR_lon] en EPSG:4326
        product      : 'GEDI02_A' | 'GEDI02_B' | 'GEDI04_A' | 'GEDI04_C'
        version      : '002' | '003' (default '002')
                       Products in _V002_ONLY_PRODUCTS ignore this and use '002'.
        selected_vars: lista de tuplas (hdf5_path, rh_idx_o_None)
        filters      : dict con quality, sensitivity, exclude_degrade, surface_flags
        beams        : lista de beams o None (= todos)
        roi_gdf      : GeoDataFrame del polígono exacto (opcional)
    """

    def __init__(self, roi, product, selected_vars,
                 version='002', filters=None, beams=None, roi_gdf=None):
        self.roi = roi
        self.product = product
        self.selected_vars = selected_vars
        self.filters = filters or {}
        self.roi_gdf = roi_gdf

        # Version lock for products without V003
        if product in _V002_ONLY_PRODUCTS and version != '002':
            self.version = '002'
        else:
            self.version = version

        # Resolve BASE_FIELDS — fall back to V002 entry if V003 not yet defined
        key = (product, self.version)
        fallback_key = (product, '002')
        self.base = BASE_FIELDS.get(key) or BASE_FIELDS.get(
            fallback_key, BASE_FIELDS[('GEDI04_A', '002')]
        )
        if key not in BASE_FIELDS and self.version != '002':
            print(f"[Subsetter] {product} V{self.version}: BASE_FIELDS not defined, "
                  f"falling back to V002 field paths.")

        # Beam selection
        if beams is None:
            self.beam_subset = ALL_BEAMS
        elif isinstance(beams, list):
            self.beam_subset = beams
        else:
            self.beam_subset = [b.strip() for b in beams.split(',') if b.strip()]
            if not self.beam_subset:
                self.beam_subset = ALL_BEAMS

        self._build_roi_geometry()

    # ── Geometría ROI ─────────────────────────────────────────────────────────

    def _build_roi_geometry(self):
        if self.roi_gdf is not None:
            self.final_clip = self.roi_gdf
            minx, miny, maxx, maxy = self.roi_gdf.total_bounds
            self.ROI = Polygon([
                (minx, maxy), (maxx, maxy),
                (maxx, miny), (minx, miny)
            ])
        else:
            ul_lat, ul_lon, lr_lat, lr_lon = self.roi
            self.ROI = Polygon([
                (ul_lon, ul_lat), (lr_lon, ul_lat),
                (lr_lon, lr_lat), (ul_lon, lr_lat)
            ])
            self.final_clip = gp.GeoDataFrame(
                index=[0], geometry=[self.ROI], crs='EPSG:4326')

    # ── API pública ───────────────────────────────────────────────────────────

    def subset_to_gdf(self, granule_path: str):
        """
        Procesa un archivo HDF5 y devuelve un GeoDataFrame con:
          - Variables base (shot_number, lat, lon, beam, delta_time, quality, degrade, surface)
          - Variables seleccionadas por el usuario
          - Arrays /rh expandidos en columnas rh25, rh50, etc.
          - Filtros aplicados

        Devuelve None si no hay footprints dentro del ROI tras filtros.
        """
        beam_info = (f"{len(self.beam_subset)} beams"
                     if self.beam_subset != ALL_BEAMS else "all beams")
        print(f"[Subsetter] Processing: {os.path.basename(granule_path)}"
              f" ({self.product} V{self.version}, {beam_info})")

        with h5py.File(granule_path, 'r') as hf:
            all_paths = []
            hf.visit(lambda name: all_paths.append(name)
                     if isinstance(hf[name], h5py.Dataset) else None)

            # Diagnostic: verify key paths exist in first available beam
            _first_beam = next((b for b in self.beam_subset if b in hf), None)
            if _first_beam:
                _lat_check = f"{_first_beam}{self.base['lat']}"
                _alt_check = f"{_first_beam}/geolocation/lat_lowestmode"
                if _lat_check not in hf and _alt_check not in hf:
                    print(f"[Subsetter] {self.product} V{self.version}: "
                          f"WARNING lat path '{self.base['lat']}' not found. "
                          f"Check BASE_FIELDS for this product/version.")

            frames = []
            for beam in self.beam_subset:
                if beam not in hf:
                    continue
                df = self._process_beam(hf, beam, all_paths)
                if df is not None and len(df) > 0:
                    frames.append(df)

        if not frames:
            print(f"[Subsetter] {self.product}: No intersecting shots in bounding box.")
            return None

        gdf = pd.concat(frames, ignore_index=True)
        gdf = gp.GeoDataFrame(
            gdf,
            geometry=gp.points_from_xy(gdf['longitude'], gdf['latitude']),
            crs='EPSG:4326'
        )

        gdf = gp.overlay(gdf, self.final_clip, how='intersection')
        gdf = gdf.dropna(subset=['geometry'])
        gdf = gdf[gdf['geometry'].is_valid & ~gdf['geometry'].is_empty]

        if len(gdf) == 0:
            print(f"[Subsetter] {self.product}: No footprints inside ROI after spatial clip.")
            return None

        try:
            gdf['date'] = get_date_from_gedi_fn(granule_path)
        except Exception:
            pass

        count_before_filters = len(gdf)
        gdf = self._apply_filters(gdf)

        if len(gdf) == 0:
            print(f"[Subsetter] ⚠ {self.product}: 0 footprints after filters "                  f"({count_before_filters} in ROI). Relax quality/degrade/surface "                  f"filters or expand the date range.")  # noqa: E501
            return None

        print(f"[Subsetter] {self.product}: {len(gdf)} footprints retained")
        return gdf

    # ── Procesamiento por beam ────────────────────────────────────────────────

    def _process_beam(self, hf, beam, all_paths):
        base = self.base

        lat_path = f"{beam}{base['lat']}"
        lon_path = f"{beam}{base['lon']}"
        shot_path = f"{beam}{base['shot']}"
        deg_path = f"{beam}{base['degrade']}"
        qual_path = f"{beam}{base['quality']}"
        surf_path = f"{beam}{base['surface']}"
        dtime_path = f"{beam}{base['delta_time']}"

        # Resolve geo paths with fallback for L2B V003
        # V003 may expose lat/lon at root OR in /geolocation/ subgroup
        lat_path, lon_path, shot_path, deg_path, dtime_path = \
            self._resolve_geo_paths(hf, beam, lat_path, lon_path,
                                    shot_path, deg_path, dtime_path)

        if lat_path not in hf or lon_path not in hf or shot_path not in hf:
            return None

        lats = hf[lat_path][()]
        lons = hf[lon_path][()]
        shots = hf[shot_path][()]

        env = self.ROI.envelope.bounds
        mask = (
            (lons >= env[0]) & (lons <= env[2]) &
            (lats >= env[1]) & (lats <= env[3])
        )
        idx = np.where(mask)[0]
        if len(idx) == 0:
            return None

        mindex, maxdex = int(idx.min()), int(idx.max()) + 1

        df = pd.DataFrame({
            'beam': beam,
            'shot_number': shots[mindex:maxdex][mask[mindex:maxdex]],
            'latitude': lats[mindex:maxdex][mask[mindex:maxdex]],
            'longitude': lons[mindex:maxdex][mask[mindex:maxdex]],
        })

        # Primary quality column name depends on product + version
        quality_col = QUALITY_COLUMN_NAMES.get(
            (self.product, self.version),
            QUALITY_COLUMN_NAMES.get((self.product, '002'), 'quality_flag')
        )

        qual_path = self._resolve_quality_path(hf, beam, qual_path)
        for col_name, path in [
            ('delta_time', dtime_path),
            (quality_col, qual_path),
            ('degrade_flag', deg_path),
            ('surface_flag', surf_path),
        ]:
            full_path = path if path.startswith(beam) else f"{beam}{path.lstrip('/')}"
            if full_path in hf:
                try:
                    df[col_name] = hf[full_path][mindex:maxdex][mask[mindex:maxdex]]
                except Exception:
                    pass

        # Secondary quality flags (version-aware)
        sec_fields = SECONDARY_QUALITY_FIELDS.get(
            (self.product, self.version),
            SECONDARY_QUALITY_FIELDS.get((self.product, '002'), {})
        )
        for col_name, rel_paths in sec_fields.items():
            if col_name in df.columns:
                continue
            if isinstance(rel_paths, str):
                rel_paths = [rel_paths]
            full_path = next(
                (f"{beam}{rel_path}" for rel_path in rel_paths
                 if f"{beam}{rel_path}" in hf),
                None
            )
            if full_path:
                try:
                    df[col_name] = hf[full_path][mindex:maxdex][mask[mindex:maxdex]]
                except Exception:
                    pass

        # User-selected variables
        for hdf5_path, rh_idx in self.selected_vars:
            full_path = f"{beam}{hdf5_path}"
            if full_path not in hf:
                full_path = f"{beam}/{hdf5_path.lstrip('/')}"
            if full_path not in hf:
                continue

            dataset = hf[full_path]

            # Case 1: /rh array → expand to rh<N> column
            if rh_idx is not None:
                try:
                    rh_data = dataset[mindex:maxdex]
                    col_data = rh_data[mask[mindex:maxdex], rh_idx]
                    metric_name = hdf5_path.strip('/').split('/')[-1] or 'rh'
                    df[f'{metric_name}{rh_idx}'] = col_data
                except Exception as e:
                    print(f"[Subsetter] Could not extract {hdf5_path}[{rh_idx}]: {e}")
                continue

            shape = dataset.shape

            # Case 2: 1D scalar variable
            if len(shape) == 1 and shape[0] == hf[shot_path].shape[0]:
                col_name = hdf5_path.lstrip('/').replace('/', '_')
                try:
                    df[col_name] = dataset[mindex:maxdex][mask[mindex:maxdex]]
                except Exception as e:
                    print(f"[Subsetter] Could not extract {hdf5_path}: {e}")
                continue

            # Case 3: 2D array (cover_z, pai_z, pavd_z)
            if len(shape) == 2:
                col_name = hdf5_path.lstrip('/').replace('/', '_')
                try:
                    data_2d = dataset[mindex:maxdex][mask[mindex:maxdex]]
                    for i in range(shape[1]):
                        df[f'{col_name}_{i}'] = data_2d[:, i]
                except Exception as e:
                    print(f"[Subsetter] Could not extract {hdf5_path} 2D: {e}")
                continue

            print(f"[Subsetter] Skipped (unhandled shape {shape}): {hdf5_path}")

        return df

    # ── Aplicar filtros ───────────────────────────────────────────────────────

    def _resolve_geo_paths(self, hf, beam, lat_path, lon_path,
                           shot_path, deg_path, dtime_path):
        """Resolve lat/lon/shot/degrade/delta_time paths for a beam.

        For L2B V002 these live in /geolocation/; for V003 they may be at
        root beam level (aligned with L2A V003). Try the configured path
        first; if absent, try the alternative location silently.
        """
        # Only applies when the product uses /geolocation/ paths
        if '/geolocation/' not in lat_path and '/geolocation/' not in lon_path:
            # Already root-level (L2A, L4A, L4C, L2B V003 after fix)
            # Try geolocation subgroup as fallback if root not found
            if lat_path not in hf:
                _ = lat_path.replace(beam, f"{beam}/geolocation", 1) \
                    if '/geolocation' not in lat_path \
                    else lat_path
                # build geolocation alternatives
                alt_lat = f"{beam}/geolocation/lat_lowestmode"
                alt_lon = f"{beam}/geolocation/lon_lowestmode"
                alt_shot = f"{beam}/geolocation/shot_number"
                alt_deg = f"{beam}/geolocation/degrade_flag"
                alt_dtime = f"{beam}/geolocation/delta_time"
                if alt_lat in hf:
                    return alt_lat, alt_lon, alt_shot, alt_deg, alt_dtime
            return lat_path, lon_path, shot_path, deg_path, dtime_path

        # Configured path uses /geolocation/ — try it first, fallback to root
        if lat_path in hf:
            return lat_path, lon_path, shot_path, deg_path, dtime_path

        # geolocation path not found — try root beam level
        root_lat = f"{beam}/lat_lowestmode"
        root_lon = f"{beam}/lon_lowestmode"
        root_shot = f"{beam}/shot_number"
        root_deg = f"{beam}/degrade_flag"
        root_dtime = f"{beam}/delta_time"
        if root_lat in hf:
            return root_lat, root_lon, root_shot, root_deg, root_dtime

        # Neither found — return original (will trigger the None return in caller)
        return lat_path, lon_path, shot_path, deg_path, dtime_path

    def _resolve_quality_path(self, hf, beam, configured_path):
        """Return the first available quality dataset path for this product."""
        if configured_path in hf:
            return configured_path

        candidates = []
        if self.product == 'GEDI02_A':
            candidates = [
                '/quality_flag',
                '/l2a_quality_flag',
                '/l2a_quality_flag_rel3',
                '/geolocation/l2a_quality_flag',
                '/geolocation/l2a_quality_flag_rel2',
                '/geolocation/l2a_quality_flag_rel3',
            ]
        elif self.product == 'GEDI02_B':
            candidates = [
                '/l2b_quality_flag_rel3',
                '/l2b_quality_flag',
                '/l2b_quality_flag_rel2',
                '/geolocation/l2b_quality_flag_rel3',
                '/geolocation/l2b_quality_flag',
                '/geolocation/l2b_quality_flag_rel2',
            ]
        elif self.product == 'GEDI04_A':
            candidates = ['/l4_quality_flag', '/quality_flag']
        elif self.product == 'GEDI04_C':
            candidates = ['/wsci_quality_flag', '/quality_flag']

        for rel_path in candidates:
            full_path = f"{beam}{rel_path}"
            if full_path in hf:
                return full_path

        return configured_path

    def _apply_filters(self, gdf: gp.GeoDataFrame) -> gp.GeoDataFrame:
        """
        Aplica los filtros definidos en la pestaña Filters de la UI.
        Usa el quality_col correcto para la versión del producto.
        """
        f = self.filters
        initial = len(gdf)

        # ── Quality flag ──────────────────────────────────────────────────────
        quality_min = f.get('quality', {}).get(self.product, 0)
        quality_col = QUALITY_COLUMN_NAMES.get(
            (self.product, self.version),
            QUALITY_COLUMN_NAMES.get((self.product, '002'), 'quality_flag')
        )
        if quality_min > 0:
            if quality_col in gdf.columns:
                before = len(gdf)
                vals = gdf[quality_col].value_counts(dropna=False).to_dict()
                gdf = gdf[gdf[quality_col].fillna(0) >= quality_min]
                print(f"[Subsetter] {self.product}: {quality_col} >= {quality_min} "
                      f"removed {before - len(gdf)} footprints; values={vals}")
            else:
                print(f"[Subsetter] {self.product}: WARNING quality field "
                      f"'{quality_col}' not found; quality filter skipped")

        # ── Degrade flag ──────────────────────────────────────────────────────
        # V002 and V003 share the same non-binary degrade_flag semantics:
        # only degrade_flag == 0 means non-degraded. Filter unchanged.
        if f.get('exclude_degrade', False) and 'degrade_flag' in gdf.columns:
            before = len(gdf)
            vals = gdf['degrade_flag'].value_counts(dropna=False).to_dict()
            gdf = gdf[gdf['degrade_flag'].fillna(1) == 0]
            print(f"[Subsetter] {self.product}: exclude_degrade removed "
                  f"{before - len(gdf)} footprints; values={vals}")

        # ── Surface flag ──────────────────────────────────────────────────────
        # Not applied to L4C: wsci_quality_flag already excludes water/urban.
        surface_flags = f.get('surface_flags', [])
        if (surface_flags
                and 'surface_flag' in gdf.columns
                and self.product != 'GEDI04_C'):
            before = len(gdf)
            vals = gdf['surface_flag'].value_counts(dropna=False).to_dict()
            gdf = gdf[gdf['surface_flag'].isin(surface_flags)]
            print(f"[Subsetter] {self.product}: surface_flag in {surface_flags} "
                  f"removed {before - len(gdf)} footprints; values={vals}")
        elif surface_flags and self.product == 'GEDI04_C':
            print(f"[Subsetter] {self.product}: surface filter skipped "
                  f"(wsci_quality_flag already excludes water/urban)")

        removed = initial - len(gdf)
        if removed > 0:
            print(f"[Subsetter] {self.product}: {removed} footprints "
                  f"removed by filters ({len(gdf)} remaining)")

        return gdf
