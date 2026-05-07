"""
GEDIMetrics — subsetter.py
Extrae variables seleccionadas de un granule HDF5 GEDI y devuelve
un GeoDataFrame listo para el merge multi-producto.

Cambios respecto al original:
  1. Recibe selected_vars como lista de (hdf5_path, rh_idx) desde la UI
     en lugar de listas hardcodeadas por producto.
  2. El array /rh se expande en columnas individuales (rh25, rh50, etc.)
     según los índices seleccionados — no se guarda como array completo.
  3. subset_to_gdf() devuelve un GeoDataFrame en lugar de escribir el archivo.
     El pipeline decide cuándo y cómo guardar (después del merge).
  4. Se aplican los filtros (quality, degrade, surface)
     sobre el GeoDataFrame antes de devolverlo. Sensitivity se evalúa
     después del merge para respetar joins multi-producto.
  5. Corrección del import relativo (utils).
"""

import os
import h5py
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gp
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Import relativo corregido
try:
    from .utils.utils import get_date_from_gedi_fn
except ImportError:
    from utils.utils import get_date_from_gedi_fn


# Variables base — siempre se extraen, no son opcionales
# (lat, lon y shot_number se usan para geolocalizar y hacer el merge)
BASE_FIELDS = {
    'GEDI02_A': {
        'lat':    '/lat_lowestmode',
        'lon':    '/lon_lowestmode',
        'shot':   '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    'GEDI02_B': {
        'lat':    '/geolocation/lat_lowestmode',
        'lon':    '/geolocation/lon_lowestmode',
        'shot':   '/geolocation/shot_number',
        'degrade': '/geolocation/degrade_flag',
        'quality': '/l2b_quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/geolocation/delta_time',
    },
    'GEDI04_A': {
        'lat':    '/lat_lowestmode',
        'lon':    '/lon_lowestmode',
        'shot':   '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/l4_quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },
    # L4C — Waveform Structural Complexity Index (WSCI).
    # Uses wsci_quality_flag as primary quality column (stricter than l2_quality_flag:
    # restricts to forested land cover with high sensitivity). l2_quality_flag is also
    # present and is preserved as a secondary flag.
    # surface_flag is extracted (it exists in L4C, inherited from L2A) but the
    # surface filter is NOT applied for L4C: wsci_quality_flag already excludes
    # water and urban surfaces by design.
    'GEDI04_C': {
        'lat':    '/lat_lowestmode',
        'lon':    '/lon_lowestmode',
        'shot':   '/shot_number',
        'degrade': '/degrade_flag',
        'quality': '/wsci_quality_flag',
        'surface': '/surface_flag',
        'delta_time': '/delta_time',
    },

}

# Columnas reales de calidad por producto. Se mantienen separadas para evitar
# filtrar GEDI04_A con el nombre generico quality_flag.
QUALITY_COLUMN_NAMES = {
    'GEDI02_A': 'quality_flag',
    'GEDI02_B': 'l2b_quality_flag',
    'GEDI04_A': 'l4_quality_flag',
    'GEDI04_C': 'wsci_quality_flag',
}

# Flags de calidad secundarios que conviene conservar si existen, pero no se
# usan como filtro principal salvo que el usuario los seleccione en variables.
# GEDI04_A/l2_quality_flag se omite del output para mantener la tabla limpia;
# el filtro principal de L4A queda representado por l4_quality_flag.
# GEDI04_C: l2_quality_flag se conserva como flag secundario para que el usuario
# pueda compararlo con wsci_quality_flag (que es estricto sobre cobertura forestal).
SECONDARY_QUALITY_FIELDS = {
    'GEDI02_B': {'l2a_quality_flag': '/l2a_quality_flag'},
    'GEDI04_C': {'l2_quality_flag':  '/l2_quality_flag'},
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
        product      : 'GEDI02_A' | 'GEDI02_B' | 'GEDI04_A'
        selected_vars: lista de tuplas (hdf5_path, rh_idx_o_None)
                       generada por dialog._collect_vars()
        filters      : dict con quality, sensitivity, exclude_degrade, surface_flags
        beams        : lista de beams o None (= todos)
        roi_gdf      : GeoDataFrame del polígono exacto (opcional)
    """

    def __init__(self, roi, product, selected_vars,
                 filters=None, beams=None, roi_gdf=None):
        self.roi           = roi
        self.product       = product
        self.selected_vars = selected_vars   # [(path, rh_idx), ...]
        self.filters       = filters or {}
        self.roi_gdf       = roi_gdf
        # beams puede ser lista, string CSV, o None (= todos)
        if beams is None:
            self.beam_subset = ALL_BEAMS
        elif isinstance(beams, list):
            self.beam_subset = beams
        else:
            # string CSV: 'BEAM0101,BEAM0110,...'
            self.beam_subset = [b.strip() for b in beams.split(',') if b.strip()]
            if not self.beam_subset:
                self.beam_subset = ALL_BEAMS
        self.base          = BASE_FIELDS.get(product, BASE_FIELDS['GEDI04_A'])

        self._build_roi_geometry()

    # ── Geometría ROI ────────────────────────────────────────
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

    # ── API pública ──────────────────────────────────────────
    def subset_to_gdf(self, granule_path: str):
        """
        Procesa un archivo HDF5 y devuelve un GeoDataFrame con:
          - Variables base (shot_number, lat, lon, beam, delta_time,
            quality_flag, degrade_flag, surface_flag)
          - Variables seleccionadas por el usuario (checkboxes UI)
          - Arrays /rh expandidos en columnas rh25, rh50, etc.
          - Filtros aplicados (quality, sensitivity, degrade, surface)

        Devuelve None si no hay footprints dentro del ROI.
        """
        beam_info = (f"{len(self.beam_subset)} beams"
                     if self.beam_subset != ALL_BEAMS
                     else "all beams")
        print(f"[Subsetter] Processing: {os.path.basename(granule_path)}"
              f" ({self.product}, {beam_info})")

        with h5py.File(granule_path, 'r') as hf:
            # Listar todos los datasets del HDF5
            all_paths = []
            hf.visit(lambda name: all_paths.append(name)
                     if isinstance(hf[name], h5py.Dataset) else None)

            frames = []
            for beam in self.beam_subset:
                if beam not in hf:
                    continue
                df = self._process_beam(hf, beam, all_paths)
                if df is not None and len(df) > 0:
                    frames.append(df)

        if not frames:
            print(f"[Subsetter] No intersecting shots in {self.product}")
            return None

        gdf = pd.concat(frames, ignore_index=True)
        gdf = gp.GeoDataFrame(
            gdf,
            geometry=gp.points_from_xy(gdf['longitude'], gdf['latitude']),
            crs='EPSG:4326'
        )

        # Clip exacto al polígono del usuario
        gdf = gp.overlay(gdf, self.final_clip, how='intersection')
        gdf = gdf.dropna(subset=['geometry'])
        gdf = gdf[gdf['geometry'].is_valid & ~gdf['geometry'].is_empty]

        if len(gdf) == 0:
            print(f"[Subsetter] No shots after exact clip for {self.product}")
            return None

        # Añadir columna de fecha desde el nombre del archivo
        try:
            gdf['date'] = get_date_from_gedi_fn(granule_path)
        except Exception:
            pass

        # Aplicar filtros
        gdf = self._apply_filters(gdf)

        if len(gdf) == 0:
            print(f"[Subsetter] No shots passed filters for {self.product}")
            return None

        print(f"[Subsetter] {self.product}: {len(gdf)} footprints retained")
        return gdf

    # ── Procesamiento por beam ───────────────────────────────
    def _process_beam(self, hf, beam, all_paths):
        """Extrae datos de un beam y devuelve DataFrame o None."""
        base = self.base

        # Rutas absolutas de campos base en este beam
        lat_path    = f"{beam}{base['lat']}"
        lon_path    = f"{beam}{base['lon']}"
        shot_path   = f"{beam}{base['shot']}"
        deg_path    = f"{beam}{base['degrade']}"
        qual_path   = f"{beam}{base['quality']}"
        surf_path   = f"{beam}{base['surface']}"
        dtime_path  = f"{beam}{base['delta_time']}"

        # Verificar que los campos base existen
        if lat_path not in hf or lon_path not in hf or shot_path not in hf:
            return None

        lats  = hf[lat_path][()]
        lons  = hf[lon_path][()]
        shots = hf[shot_path][()]

        # Clip bbox rápido para obtener índices dentro del ROI
        env   = self.ROI.envelope.bounds   # (minx, miny, maxx, maxy)
        mask  = (
            (lons >= env[0]) & (lons <= env[2]) &
            (lats >= env[1]) & (lats <= env[3])
        )
        idx = np.where(mask)[0]

        if len(idx) == 0:
            return None

        # Índices mínimo y máximo para slicing eficiente
        mindex, maxdex = int(idx.min()), int(idx.max()) + 1

        # DataFrame base
        df = pd.DataFrame({
            'beam':       beam,
            'shot_number': shots[mindex:maxdex][mask[mindex:maxdex]],
            'latitude':    lats[mindex:maxdex][mask[mindex:maxdex]],
            'longitude':   lons[mindex:maxdex][mask[mindex:maxdex]],
        })

        # Campos base adicionales (delta_time, quality, degrade, surface).
        # La calidad usa el nombre real del producto:
        # GEDI02_A=quality_flag, GEDI02_B=l2b_quality_flag, GEDI04_A=l4_quality_flag.
        quality_col = QUALITY_COLUMN_NAMES.get(self.product, 'quality_flag')
        for col_name, path in [
            ('delta_time',   dtime_path),
            (quality_col,    qual_path),
            ('degrade_flag', deg_path),
            ('surface_flag', surf_path),
        ]:
            full_path = path if path.startswith(beam) else f"{beam}{path.lstrip('/')}"
            if full_path in hf:
                try:
                    df[col_name] = hf[full_path][mindex:maxdex][mask[mindex:maxdex]]
                except Exception:
                    pass

        # Conservar flags secundarios si existen. No reemplazan el filtro principal.
        for col_name, rel_path in SECONDARY_QUALITY_FIELDS.get(self.product, {}).items():
            full_path = f"{beam}{rel_path}"
            if full_path in hf and col_name not in df.columns:
                try:
                    df[col_name] = hf[full_path][mindex:maxdex][mask[mindex:maxdex]]
                except Exception:
                    pass

        # Variables seleccionadas por el usuario
        for hdf5_path, rh_idx in self.selected_vars:
            full_path = f"{beam}{hdf5_path}"
            if full_path not in hf:
                # intentar sin barra inicial
                full_path = f"{beam}/{hdf5_path.lstrip('/')}"
            if full_path not in hf:
                continue

            dataset = hf[full_path]

            # ── Caso 1: array /rh → expandir columna rh<N> ──
            if rh_idx is not None:
                try:
                    rh_data = dataset[mindex:maxdex]   # shape (N, 101)
                    col_data = rh_data[mask[mindex:maxdex], rh_idx]
                    df[f'rh{rh_idx}'] = col_data
                except Exception as e:
                    print(f"[Subsetter] Could not extract rh{rh_idx}: {e}")
                continue

            # ── Caso 2: variable escalar 1D ──
            shape = dataset.shape
            if len(shape) == 1 and shape[0] == hf[shot_path].shape[0]:
                col_name = hdf5_path.lstrip('/').replace('/', '_')
                try:
                    df[col_name] = dataset[mindex:maxdex][mask[mindex:maxdex]]
                except Exception as e:
                    print(f"[Subsetter] Could not extract {hdf5_path}: {e}")
                continue

            # ── Caso 3: array 2D (cover_z, pai_z, pavd_z) ──
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

    # ── Aplicar filtros ──────────────────────────────────────
    def _apply_filters(self, gdf: gp.GeoDataFrame) -> gp.GeoDataFrame:
        """
        Aplica los filtros definidos en la pestaña Filters de la UI.
        Opera sobre columnas que pueden o no existir (outer join puede
        tener NaN si el producto no las tiene).
        """
        f = self.filters
        initial = len(gdf)

        # — Quality flag —
        # Filtro especifico por producto:
        # GEDI02_A -> quality_flag, GEDI02_B -> l2b_quality_flag, GEDI04_A -> l4_quality_flag.
        quality_min = f.get('quality', {}).get(self.product, 0)
        quality_col = QUALITY_COLUMN_NAMES.get(self.product, 'quality_flag')
        if quality_min > 0:
            if quality_col in gdf.columns:
                before = len(gdf)
                vals = gdf[quality_col].value_counts(dropna=False).to_dict()
                gdf = gdf[gdf[quality_col].fillna(0) >= quality_min]
                print(f"[Subsetter] {self.product}: {quality_col} >= {quality_min} "
                      f"removed {before - len(gdf)} footprints; values={vals}")
            else:
                print(f"[Subsetter] {self.product}: WARNING quality field {quality_col} not found; quality filter skipped")

        # Nota: sensitivity se aplica POST-merge en pipeline.py para poder
        # evaluar columnas de varios productos después del join.

        # — Degrade flag —
        if f.get('exclude_degrade', False) and 'degrade_flag' in gdf.columns:
            before = len(gdf)
            vals = gdf['degrade_flag'].value_counts(dropna=False).to_dict()
            gdf = gdf[gdf['degrade_flag'].fillna(1) == 0]
            print(f"[Subsetter] {self.product}: exclude_degrade removed "
                  f"{before - len(gdf)} footprints; values={vals}")

        # — Surface flag —
        # No se aplica a L4C: wsci_quality_flag ya excluye agua y urbano por diseño,
        # así que un filtro adicional sobre surface_flag sería redundante y podría
        # invalidar shots que el equipo de WSCI ya validó como bosque.
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
