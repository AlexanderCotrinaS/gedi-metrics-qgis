"""
GEDIMetrics — pipeline.py
Orquesta Finder → Downloader → Subsetter (multi-producto) → Merge → Export.
"""

import os
from pathlib import Path
import geopandas as gp
import pandas as pd

from .finder     import GEDIFinder
from .downloader import GEDIDownloader
from .subsetter  import GEDISubsetter


# Columnas base que NO se duplican en el merge
# (existen en todos los productos — se toman del primero)
# quality_flag, degrade_flag, surface_flag y sensitivity
# se descartan de los productos secundarios para evitar duplicados
_BASE_COLS = {
    'shot_number', 'latitude', 'longitude',
    'beam', 'delta_time', 'geometry', 'date',
    'quality_flag', 'degrade_flag', 'surface_flag', 'sensitivity',
    'rh100',   # rh100 viene de L2A; en L2B se llama igual pero es redundante
}


class GEDIPipeline:
    """
    Pipeline completo GEDIMetrics.

    Args:
        out_directory    : carpeta de salida
        products         : lista de productos, ej ['GEDI02_A', 'GEDI02_B', 'GEDI04_A']
        version          : '002'
        date_start/end   : 'YYYY.MM.DD'
        recurring_months : bool
        roi              : [UL_lat, UL_lon, LR_lat, LR_lon]
        beams            : lista de beams o None
        selected_vars    : dict {producto: [(path, rh_idx), ...]}  ← viene de la UI
        filters          : dict con quality/sensitivity/degrade/surface  ← viene de la UI
        merge_how        : 'outer' | 'inner'
        out_gpkg         : bool — exportar GeoPackage
        out_parquet      : bool — exportar GeoParquet
        persist_login    : bool
        keep_original_file: bool
        cancel_event     : threading.Event
        roi_path         : path al polígono (opcional)
    """

    # Mapeo producto → shortname CMR
    PRODUCT_NAMES = {
        'GEDI02_A': 'GEDI02_A',
        'GEDI02_B': 'GEDI02_B',
        'GEDI04_A': 'GEDI04_A',
    }

    def __init__(self, out_directory, products, version,
                 date_start, date_end, roi,
                 recurring_months=False,
                 beams=None,
                 selected_vars=None,
                 filters=None,
                 merge_how='outer',
                 out_gpkg=True,
                 out_parquet=False,
                 persist_login=False,
                 keep_original_file=False,
                 cancel_event=None,
                 roi_path=None):

        self.out_directory      = out_directory
        self.products           = products
        self.version            = version
        self.date_start         = date_start
        self.date_end           = date_end
        self.recurring_months   = recurring_months
        self.roi                = [float(c) for c in roi]
        self.beams              = beams
        self.selected_vars      = selected_vars or {}
        self.filters            = filters or {}
        self.merge_how          = merge_how
        self.out_gpkg           = out_gpkg
        self.out_parquet        = out_parquet
        self.persist_login      = persist_login
        self.keep_original_file = keep_original_file
        self.cancel_event       = cancel_event
        self.roi_path           = roi_path

        # ROI GeoDataFrame (polígono exacto si existe)
        self.roi_gdf = None
        if self.roi_path:
            try:
                # QGIS añade |layername=... al source de la capa — limpiarlo
                clean_path = self.roi_path.split('|')[0].strip()
                gdf = gp.read_file(clean_path)
                gdf = gdf.to_crs(epsg=4326) if gdf.crs else \
                      gdf.set_crs(epsg=4326)
                self.roi_gdf = gdf
                minx, miny, maxx, maxy = gdf.total_bounds
                self.roi = [maxy, minx, miny, maxx]
                print(f"[Pipeline] ROI from polygon: {self.roi}")
            except Exception as e:
                print(f"[Pipeline] Could not read ROI polygon: {e}")

        os.makedirs(out_directory, exist_ok=True)
        self._final_output_path = None

        # Downloader único — la sesión NASA se reutiliza
        self.downloader = GEDIDownloader(
            persist_login=self.persist_login,
            save_path=self.out_directory)

    # ── Pipeline principal ────────────────────────────────────
    def run_pipeline(self):
        """
        Para cada producto seleccionado:
          1. Busca granules (Finder)
          2. Descarga cada granule (Downloader)
          3. Subsettea al ROI con variables seleccionadas (Subsetter)
          4. Merge de los GeoDataFrames por shot_number
          5. Exporta a gpkg y/o parquet
        """
        if not self.products:
            print("[Pipeline] No products selected.")
            return

        print(f"[Pipeline] Products : {', '.join(self.products)}")
        print(f"[Pipeline] Merge    : {self.merge_how} join")

        # Encontrar granules comunes a todos los productos
        # (mismo período y bbox — el shot_number garantiza alineación)
        all_granule_links = self._find_granules()

        if not all_granule_links:
            print("[Pipeline] No granules found.")
            return

        # Procesar granule por granule
        for granule_url, granule_size in all_granule_links:
            if self._cancelled():
                break

            granule_name = granule_url.split("/")[-1]   # ej: GEDI02_A_2021...h5
            h5_path      = os.path.join(self.out_directory, granule_name)
            stem         = granule_name.replace('.h5', '')

            # Saltar si el merged ya existe
            merged_gpkg = os.path.join(self.out_directory, f"{stem}_merged.gpkg")
            if os.path.exists(merged_gpkg):
                print(f"[Pipeline] Already processed: {stem}")
                continue

            # Descargar
            if not self._download(granule_url):
                continue

            if self._cancelled():
                break

            # Subsettear cada producto sobre el mismo .h5
            gdfs = self._subset_all_products(h5_path, stem)

            # Eliminar HDF5 si no se quiere conservar
            if not self.keep_original_file and os.path.exists(h5_path):
                os.remove(h5_path)
                print(f"[Pipeline] Removed HDF5: {granule_name}")

            if not gdfs:
                print(f"[Pipeline] No data for {stem}")
                continue

            # Merge
            merged = self._merge(gdfs)
            if merged is None or len(merged) == 0:
                print(f"[Pipeline] Empty merge for {stem}")
                continue

            # Filtro post-merge — añade columna quality_passed
            merged = self._apply_postmerge_filter(merged)

            # Exportar
            self._export(merged, stem)

        # Merge final — unir todos los granules en un solo archivo
        self._merge_final_outputs()

        print("[Pipeline] Done.")

    # ── Finder ───────────────────────────────────────────────
    def _find_granules(self):
        """
        Usa el primer producto como referencia para encontrar granules.
        Los granules son los mismos archivos .h5 para todos los productos
        dentro del mismo período — el Finder los identifica por bbox y fecha.
        Devuelve lista de (url, size).
        """
        ref_product = self.products[0]
        product_name = ref_product          # ej 'GEDI02_A'
        version      = self.version         # '002'

        finder = GEDIFinder(
            product          = product_name,
            version          = version,
            date_start       = self.date_start,
            date_end         = self.date_end,
            recurring_months = self.recurring_months,
            roi              = self.roi,
        )
        granules = finder.find(
            output_filepath=self.out_directory,
            save_file=True)
        print(f"[Pipeline] {len(granules)} granules found")
        return granules

    # ── Downloader ────────────────────────────────────────────
    def _download(self, url):
        ok = self.downloader.download_granule(url)
        if not ok:
            print(f"[Pipeline] Download failed: {url.split('/')[-1]} — retrying")
            for attempt in range(1, 4):
                print(f"[Pipeline] Retry {attempt}/3")
                ok = self.downloader.download_granule(url)
                if ok:
                    break
            if not ok:
                print(f"[Pipeline] Skipping after 3 failed attempts")
        return ok

    # ── Subsetter multi-producto ──────────────────────────────
    def _subset_all_products(self, h5_path, stem):
        """
        Para el mismo archivo .h5, corre el subsetter por cada producto
        seleccionado y devuelve dict {producto: GeoDataFrame}.

        Nota: un granule .h5 de GEDI02_A contiene SOLO datos L2A.
        Para L2B y L4A hay que descargar los granules correspondientes.
        Este método maneja la sustitución del nombre en la URL.
        """
        gdfs = {}

        for product in self.products:
            if self._cancelled():
                break

            # Obtener el h5 del producto correcto
            product_h5 = self._get_product_h5(h5_path, product)
            if product_h5 is None:
                print(f"[Pipeline] Could not get HDF5 for {product}")
                continue

            subsetter = GEDISubsetter(
                roi           = self.roi,
                product       = product,
                selected_vars = self.selected_vars.get(product, []),
                filters       = self.filters,
                beams         = self.beams,
                roi_gdf       = self.roi_gdf,
            )

            gdf = subsetter.subset_to_gdf(product_h5)

            # Limpiar HDF5 del producto si no es el original
            if product_h5 != h5_path and not self.keep_original_file:
                if os.path.exists(product_h5):
                    os.remove(product_h5)

            if gdf is not None and len(gdf) > 0:
                gdfs[product] = gdf

        return gdfs

    def _get_product_h5(self, base_h5_path, product):
        """
        Obtiene el path del HDF5 para un producto dado.
        - L2B: mismo nombre cambiando el shortname
        - L4A: nombre diferente — buscar en disco o descargar via CMR
        """
        basename    = os.path.basename(base_h5_path)
        ref_product = self.products[0]

        if product == ref_product:
            return base_h5_path

        # Extraer orbit number — se usa para L4A
        orbit_num = None
        for p in basename.replace('.h5', '').split('_'):
            if p.startswith('O') and p[1:].isdigit():
                orbit_num = p[1:]
                break

        # L4A: nombre de archivo difiere del L2A — buscar en disco o CMR
        if product == 'GEDI04_A':
            prefix = 'GEDI04_A'
            if orbit_num:
                existing = [
                    f for f in os.listdir(self.out_directory)
                    if f.startswith(prefix) and f'O{orbit_num}' in f
                    and f.endswith('.h5')
                ]
                if existing:
                    found = os.path.join(self.out_directory, existing[0])
                    print(f"[Pipeline] Found existing {product} on disk: {existing[0]}")
                    return found

            # No está en disco — descargar via CMR usando orbit
            # Pasamos el basename de L2A como referencia para extraer el orbit
            print(f"[Pipeline] Downloading {product} granule for orbit O{orbit_num}...")
            target_url = self._derive_url(basename, product)
            if target_url:
                fname = target_url.split('/')[-1]
                actual_path = os.path.join(self.out_directory, fname)
                if self._download(target_url):
                    return actual_path

            print(f"[Pipeline] Warning: could not obtain {product} for this granule")
            return None

        # L2B y otros: nombre casi idéntico al L2A — solo cambiar shortname
        target_basename = basename.replace(ref_product, product)
        target_path     = os.path.join(self.out_directory, target_basename)

        if os.path.exists(target_path):
            return target_path

        print(f"[Pipeline] Downloading {product} granule: {target_basename}")
        target_url = self._derive_url(target_basename, product)
        if target_url and self._download(target_url):
            return target_path

        print(f"[Pipeline] Warning: could not obtain {product} for this granule")
        return None

    def _derive_url(self, filename, product):
        """
        Construye la URL de descarga para un producto usando la API CMR de NASA.
        Para L4A busca por orbit number ya que el nombre de archivo difiere de L2A/L2B.
        """
        import requests as req

        concept_ids = {
            'GEDI02_A.002': 'C2142771958-LPCLOUD',
            'GEDI02_B.002': 'C2142776747-LPCLOUD',
            'GEDI04_A.002': 'C2237824918-ORNL_CLOUD',
        }
        key = f"{product}.{self.version}"
        if key not in concept_ids:
            print(f"[Pipeline] Unknown product key: {key}")
            return None

        concept_id = concept_ids[key]

        # Extraer orbit number del nombre de archivo (ej: O02577)
        # El nombre tiene formato: GEDI02_A_YYYYDDDHHMMSS_OXXXXX_...
        orbit_num = None
        parts = filename.replace('.h5', '').split('_')
        for p in parts:
            if p.startswith('O') and p[1:].isdigit():
                orbit_num = p[1:]   # ej: '02577'
                break

        # Para L2B: el nombre es casi idéntico al L2A — buscar directo
        if product != 'GEDI04_A':
            granule_id = filename.replace('.h5', '')
            cmr_url = (
                f"https://cmr.earthdata.nasa.gov/search/granules.json"
                f"?concept_id={concept_id}"
                f"&producer_granule_id={granule_id}"
                f"&page_size=1"
            )
            try:
                resp = req.get(cmr_url, timeout=30)
                entries = resp.json().get('feed', {}).get('entry', [])
                if entries:
                    for link in entries[0].get('links', []):
                        href = link.get('href', '')
                        if href.endswith('.h5'):
                            print(f"[Pipeline] Found URL for {product}: {href}")
                            return href
            except Exception as e:
                print(f"[Pipeline] CMR search error for {product}: {e}")
            print(f"[Pipeline] Could not find URL for {product} / {filename}")
            return None

        # Para L4A: el nombre difiere — buscar por orbit number y bbox
        # GEDI04_A usa ORNL y tiene naming diferente al L2A
        if orbit_num is None:
            print(f"[Pipeline] Could not extract orbit from {filename}")
            return None

        print(f"[Pipeline] Searching {product} granule for orbit O{orbit_num}...")

        def _get_ornl_url(entries_list, orbit):
            """Extrae URL HTTP directa de ORNL de una lista de entries CMR."""
            for entry in entries_list:
                title = (entry.get('producer_granule_id', '')
                         or entry.get('title', ''))
                if f"O{orbit}" not in title:
                    continue
                # Preferir link HTTP directo de ORNL (no s3, no opendap)
                for link in entry.get('links', []):
                    href = link.get('href', '')
                    if (href.endswith('.h5')
                            and href.startswith('https://')
                            and 's3://' not in href
                            and 'opendap' not in href):
                        return href
            return None

        try:
            # Búsqueda 1: por bbox
            [ul_lat, ul_lon, lr_lat, lr_lon] = self.roi
            bbox = f"{ul_lon},{lr_lat},{lr_lon},{ul_lat}"
            r1 = req.get(
                f"https://cmr.earthdata.nasa.gov/search/granules.json"
                f"?concept_id={concept_id}&bounding_box={bbox}&page_size=10",
                timeout=30)
            entries = r1.json().get('feed', {}).get('entry', [])
            url = _get_ornl_url(entries, orbit_num)
            if url:
                print(f"[Pipeline] Found URL for {product}: {url}")
                return url

            # Búsqueda 2: por fecha derivada del nombre del granule
            # El basename contiene YYYYDDD (día juliano) — convertir a fecha ISO
            date_str = None
            try:
                # Extraer YYYYDDD del basename: GEDI02_A_YYYYDDDHHMMSS_...
                parts_fn = filename.replace('.h5','').split('_')
                # El campo de fecha es el 3er elemento: ej 2019147190447
                datetime_field = parts_fn[2] if len(parts_fn) > 2 else ''
                if len(datetime_field) >= 7:
                    import datetime
                    year = int(datetime_field[:4])
                    doy  = int(datetime_field[4:7])
                    dt   = datetime.datetime(year, 1, 1) + datetime.timedelta(days=doy - 1)
                    date_start = dt.strftime('%Y-%m-%dT00:00:00Z')
                    date_end   = (dt + datetime.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')
                    date_str   = f"{date_start},{date_end}"
            except Exception:
                pass

            entries2 = []
            if date_str:
                r2 = req.get(
                    f"https://cmr.earthdata.nasa.gov/search/granules.json"
                    f"?concept_id={concept_id}"
                    f"&temporal={date_str}"
                    f"&bounding_box={bbox}"
                    f"&page_size=10",
                    timeout=30)
                entries2 = r2.json().get('feed', {}).get('entry', [])
            url2 = _get_ornl_url(entries2, orbit_num)
            if url2:
                print(f"[Pipeline] Found URL for {product}: {url2}")
                return url2

            # Búsqueda 3: fallback — cualquier .h5 en entries que tenga orbit
            for e_list in [entries, entries2]:
                for entry in e_list:
                    title = (entry.get('producer_granule_id', '')
                             or entry.get('title', ''))
                    if f"O{orbit_num}" not in title:
                        continue
                    for link in entry.get('links', []):
                        href = link.get('href', '')
                        if href.endswith('.h5') and href.startswith('https://'):
                            print(f"[Pipeline] Found URL for {product} (fallback): {href}")
                            return href

        except Exception as e:
            print(f"[Pipeline] CMR search error for {product}: {e}")

        print(f"[Pipeline] Could not find URL for {product} (orbit O{orbit_num})")
        return None

    # ── Merge ─────────────────────────────────────────────────
    def _merge(self, gdfs: dict):
        """
        Une los GeoDataFrames de distintos productos por shot_number.
        - merge_how='outer': conserva todos los footprints aunque falten datos
        - merge_how='inner': solo footprints presentes en TODOS los productos
        """
        if len(gdfs) == 1:
            product, gdf = next(iter(gdfs.items()))
            print(f"[Pipeline] Single product — no merge needed ({product})")
            return gdf

        products = list(gdfs.keys())
        print(f"[Pipeline] Merging {', '.join(products)} "
              f"({self.merge_how} join) by shot_number ...")

        # Sufijos para columnas duplicadas (ej. sensitivity_L2A, sensitivity_L2B)
        merged = gdfs[products[0]].copy()

        for product in products[1:]:
            right = gdfs[product].copy()

            # Columnas a NO duplicar del lado derecho
            # Se eliminan campos base + campos que se repiten en todos los productos
            drop_cols = [c for c in right.columns
                         if c in _BASE_COLS and c != 'shot_number']
            right = right.drop(columns=drop_cols, errors='ignore')

            # Añadir sufijo al producto para columnas que se repiten
            suffix = f"_{product.replace('GEDI0', 'L').replace('_A','A').replace('_B','B')}"

            merged = merged.merge(
                right,
                on='shot_number',
                how=self.merge_how,
                suffixes=('', suffix)
            )

        # Asegurarse de que sigue siendo GeoDataFrame
        if 'geometry' in merged.columns:
            merged = gp.GeoDataFrame(merged, geometry='geometry', crs='EPSG:4326')

        print(f"[Pipeline] Merged: {len(merged)} footprints, "
              f"{len(merged.columns)} columns")
        return merged

    # ── Filtro post-merge ─────────────────────────────────────
    def _apply_postmerge_filter(self, gdf: gp.GeoDataFrame) -> gp.GeoDataFrame:
        """
        Aplica sensitivity como flag después del merge.
        En lugar de eliminar shots, añade columna 'quality_passed' (bool).
        Un shot pasa si TODOS los productos que tiene datos superan el umbral.
        """
        f = self.filters
        sens_cfg  = f.get('sensitivity', {})
        min_sens  = sens_cfg.get('value', 0.90)
        apply_to  = sens_cfg.get('apply_to', {})

        # Columnas de sensitivity presentes en el GeoDataFrame
        # Pueden ser 'sensitivity' (L2A), 'sensitivity_L2B', 'sensitivity_L4A'
        sens_cols = [c for c in gdf.columns if 'sensitivity' in c.lower()]

        if not sens_cols:
            gdf['quality_passed'] = True
            return gdf

        # Para cada producto activo que tenga sensitivity, verificar el umbral
        # Solo se evalúan columnas de productos que el usuario marcó para filtrar
        conditions = []
        for col in sens_cols:
            # Determinar a qué producto corresponde esta columna
            product = None
            if col == 'sensitivity':
                product = self.products[0] if self.products else None
            elif '_L2B' in col:
                product = 'GEDI02_B'
            elif '_L4A' in col:
                product = 'GEDI04_A'

            # Solo filtrar si el usuario lo indicó para este producto
            if product and apply_to.get(product, False):
                # NaN = el shot no tiene dato de ese producto (outer join)
                # → no penalizar, solo evaluar donde hay dato
                has_data = gdf[col].notna()
                passes   = gdf[col].fillna(min_sens + 1) >= min_sens
                # El shot falla solo si TIENE dato Y ese dato está bajo el umbral
                conditions.append(~has_data | passes)

        if conditions:
            import functools
            quality_mask = functools.reduce(lambda a, b: a & b, conditions)
            gdf['quality_passed'] = quality_mask
        else:
            gdf['quality_passed'] = True

        passed = gdf['quality_passed'].sum()
        total  = len(gdf)
        print(f"[Pipeline] quality_passed: {passed}/{total} shots "
              f"({passed/total*100:.1f}%) — sensitivity >= {min_sens}")
        return gdf

    # ── Export ────────────────────────────────────────────────
    def _export(self, gdf: gp.GeoDataFrame, stem: str):
        if self.out_gpkg:
            out_path = os.path.join(self.out_directory, f"{stem}_merged.gpkg")
            try:
                gdf.to_file(out_path, driver='GPKG')
                print(f"[Pipeline] Saved GeoPackage: {os.path.basename(out_path)}")
            except Exception as e:
                print(f"[Pipeline] Error saving GeoPackage: {e}")

        if self.out_parquet:
            out_path = os.path.join(self.out_directory, f"{stem}_merged.parquet")
            try:
                gdf.to_parquet(out_path)
                print(f"[Pipeline] Saved GeoParquet: {os.path.basename(out_path)}")
            except Exception as e:
                print(f"[Pipeline] Error saving GeoParquet: {e}")

    # ── Merge final de todos los granules ────────────────────
    def _merge_final_outputs(self):
        """
        Une todos los granules procesados en un solo archivo final.
        GEDIMetrics_YYYYMMDD_HHMMSS_final.gpkg  (y .parquet si aplica)
        Solo se ejecuta si hay más de un granule procesado.
        """
        import glob
        from datetime import datetime

        out_dir = self.out_directory

        # Buscar todos los gpkg de granules individuales
        granule_gpkgs = sorted(glob.glob(
            os.path.join(out_dir, '*_merged.gpkg')))

        # Excluir el final si ya existe de una corrida anterior
        granule_gpkgs = [f for f in granule_gpkgs
                         if '_final' not in os.path.basename(f)]

        if len(granule_gpkgs) == 0:
            print("[Pipeline] No granules to merge into final file.")
            return

        if len(granule_gpkgs) == 1:
            print("[Pipeline] Only one granule — no final merge needed.")
            # Renombrar para consistencia
            src_path = granule_gpkgs[0]
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            dst_name = f"GEDIMetrics_{ts}_final.gpkg"
            dst_path = os.path.join(out_dir, dst_name)
            import shutil
            shutil.copy2(src_path, dst_path)
            print(f"[Pipeline] Final file: {dst_name}")
            self._final_output_path = dst_path
            return

        print(f"[Pipeline] Merging {len(granule_gpkgs)} granules into final file...")

        import geopandas as gp
        frames = []
        for gpkg_path in granule_gpkgs:
            try:
                gdf = gp.read_file(gpkg_path)
                frames.append(gdf)
                print(f"[Pipeline]   + {os.path.basename(gpkg_path)} "
                      f"({len(gdf)} footprints)")
            except Exception as e:
                print(f"[Pipeline]   ERROR reading {os.path.basename(gpkg_path)}: {e}")

        if not frames:
            print("[Pipeline] No valid granules to merge.")
            return

        merged = gp.pd.concat(frames, ignore_index=True)
        if hasattr(merged, 'set_geometry') and 'geometry' in merged.columns:
            merged = gp.GeoDataFrame(merged, geometry='geometry', crs='EPSG:4326')

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        print(f"[Pipeline] Final merge: {len(merged)} total footprints, "
              f"{len(merged.columns)} columns")

        # Guardar gpkg final
        if self.out_gpkg:
            final_gpkg = os.path.join(out_dir, f"GEDIMetrics_{ts}_final.gpkg")
            try:
                merged.to_file(final_gpkg, driver='GPKG')
                print(f"[Pipeline] Saved final GeoPackage: "
                      f"GEDIMetrics_{ts}_final.gpkg")
                self._final_output_path = final_gpkg
            except Exception as e:
                print(f"[Pipeline] Error saving final gpkg: {e}")

        # Guardar parquet final
        if self.out_parquet:
            final_parquet = os.path.join(
                out_dir, f"GEDIMetrics_{ts}_final.parquet")
            try:
                merged.to_parquet(final_parquet)
                print(f"[Pipeline] Saved final GeoParquet: "
                      f"GEDIMetrics_{ts}_final.parquet")
            except Exception as e:
                print(f"[Pipeline] Error saving final parquet: {e}")

    # ── Utilidades ────────────────────────────────────────────
    def _cancelled(self):
        return (self.cancel_event is not None
                and self.cancel_event.is_set())
