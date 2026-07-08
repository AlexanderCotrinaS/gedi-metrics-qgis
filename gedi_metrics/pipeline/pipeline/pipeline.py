"""
GEDIMetrics v1.0.5 — pipeline.py
Orquesta Finder → Downloader → Subsetter (multi-producto) → Merge → Export.
"""

import os
import geopandas as gp

from .finder import GEDIFinder
from .downloader import GEDIDownloader
from .subsetter import GEDISubsetter


# Columnas base que NO se duplican en el merge
# (existen en todos los productos — se toman del primero).
# La columna sensitivity NO se descarta: si el usuario la selecciona en L2B/L4A,
# se conserva con sufijo para que el filtro post-merge pueda evaluarla por producto.
_BASE_COLS = {
    'shot_number', 'latitude', 'longitude',
    'beam', 'delta_time', 'geometry', 'date',
    'quality_flag', 'degrade_flag', 'surface_flag',
    'rh100',   # rh100 viene de L2A; en L2B se llama igual pero es redundante
}

# Columnas auxiliares internas que no deben aparecer en los archivos finales.
# Columnas auxiliares internas que no deben aparecer en los archivos finales.
# Nota: l2_quality_flag YA NO se descarta. En v1.0.2 se eliminaba porque solo
# podía aparecer como redundante de L4A; en v1.0.3 es un flag secundario útil
# para L4C (permite comparar con wsci_quality_flag, que es más estricto).
_OUTPUT_DROP_COLS = {'quality_passed'}

# Orden preferido para los flags de calidad al final de la tabla de atributos.
_QUALITY_FLAG_ORDER = {
    'quality_flag': 0,
    'l2a_quality_flag': 1,
    'l2b_quality_flag': 2,
    'l4_quality_flag': 3,
    'l2_quality_flag': 4,        # se conserva como flag secundario para L4C
    'wsci_quality_flag': 5,      # filtro principal de L4C
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
        'GEDI04_C': 'GEDI04_C',
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
                 roi_path=None,
                 bearer_token=None,
                 proxy_url=None,
                 proxy_user=None,
                 proxy_pass=None,
                 proxy_auto=True):

        self.out_directory = out_directory
        self.products = products
        self.version = version
        self.date_start = date_start
        self.date_end = date_end
        self.recurring_months = recurring_months
        self.roi = [float(c) for c in roi]
        self.beams = beams
        self.selected_vars = selected_vars or {}
        self.filters = filters or {}
        self.merge_how = merge_how
        self.out_gpkg = out_gpkg
        self.out_parquet = out_parquet
        self.persist_login = persist_login
        self.keep_original_file = keep_original_file
        self.cancel_event = cancel_event
        self.roi_path = roi_path
        self.bearer_token = bearer_token
        self.proxy_url = proxy_url
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.proxy_auto = proxy_auto

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
        self._run_gpkg_outputs = []

        # Build proxy dict once — shared by Downloader, Finder, and _derive_url
        from .downloader import _build_proxy_dict
        self._proxies = _build_proxy_dict(
            proxy_url=self.proxy_url,
            proxy_user=self.proxy_user,
            proxy_pass=self.proxy_pass,
            auto_detect=self.proxy_auto,
        )

        # Downloader único — la sesión NASA se reutiliza
        self.downloader = GEDIDownloader(
            persist_login=self.persist_login,
            save_path=self.out_directory,
            bearer_token=self.bearer_token,
            proxy_url=self.proxy_url,
            proxy_user=self.proxy_user,
            proxy_pass=self.proxy_pass,
            proxy_auto=self.proxy_auto,
        )

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

        if self._date_range_has_no_acquisitions():
            return

        # Encontrar granules comunes a todos los productos
        # (mismo período y bbox — el shot_number garantiza alineación)
        all_granule_links = self._find_granules()

        if not all_granule_links:
            print("[Pipeline] No granules found. Check date range, ROI extent, "
                  "and that latitude is within ±51.6°. Note: GEDI was offline "
                  "from March 2023 to April 2024.")
            return

        # Procesar granule por granule
        for granule_url, granule_size in all_granule_links:
            if self._cancelled():
                break

            granule_name = granule_url.split("/")[-1]   # ej: GEDI02_A_2021...h5
            h5_path = os.path.join(self.out_directory, granule_name)
            stem = granule_name.replace('.h5', '')

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
                print(f"[Pipeline] No usable data for {stem} — skipping.")
                continue

            # Merge
            merged = self._merge(gdfs)
            if merged is None or len(merged) == 0:
                print(f"[Pipeline] Empty merge for {stem}. "
                      f"With 'inner join', products must share shot_numbers; "
                      f"try 'outer join' to preserve all footprints.")
                continue

            # Filtro post-merge de sensitivity. No añade columnas auxiliares.
            merged = self._apply_postmerge_filter(merged)
            if merged is None or len(merged) == 0:
                print(f"[Pipeline] No footprints remain for {stem} after "
                      "post-merge filters - skipping export.")
                continue

            # Exportar
            self._export(merged, stem)

        # Merge final — unir todos los granules en un solo archivo
        self._merge_final_outputs()

        print("[Pipeline] Done.")

    # ── Per-product version resolver ─────────────────────────
    def _date_range_has_no_acquisitions(self):
        """Return True when the requested GEDI date range cannot contain data."""
        try:
            import datetime
            start = datetime.datetime.strptime(self.date_start, "%Y.%m.%d").date()
            end = datetime.datetime.strptime(self.date_end, "%Y.%m.%d").date()
        except Exception:
            return False

        mission_start = datetime.date(2019, 4, 4)
        gap_start = datetime.date(2023, 3, 17)
        gap_end = datetime.date(2024, 4, 25)

        if end < mission_start:
            print("[Pipeline] No GEDI acquisitions before 2019-04-04.")
            return True

        if gap_start <= start and end <= gap_end:
            print("[Pipeline] GEDI was not acquiring data from 2023-03-17 "
                  "through 2024-04-25. Choose dates before or after that gap.")
            return True

        if start <= gap_end and end >= gap_start:
            print("[Pipeline] Date range overlaps the GEDI acquisition gap "
                  "(2023-03-17 to 2024-04-25); only dates outside the gap "
                  "can return granules.")

        return False

    # Cache de disponibilidad V003 por producto — verificado una vez por sesión
    _v003_availability_cache: dict = {}

    def _product_version(self, product: str) -> str:
        """Return the effective version for a given product.

        - GEDI04_C: always V002 (V003 not yet released).
        - GEDI04_A: V002 until ORNL publishes V003 granules.
                    Checks CMR once per session; falls back to V002 if 0 granules.
        - L2A, L2B: use self.version (user selection from UI).
        """
        if product == 'GEDI04_C':
            return '002'

        if product == 'GEDI04_A' and self.version == '003':
            return self._resolve_l4a_version()

        return self.version

    def _resolve_l4a_version(self) -> str:
        """Check if GEDI04_A V003 has granules within the pipeline ROI.
        Cached per session (keyed by ROI bbox so different AOIs get independent checks).
        Returns '003' if granules exist for this AOI, '002' otherwise.
        """
        # Cache key includes bbox so different AOIs don't share the result
        ul_lat, ul_lon, lr_lat, lr_lon = self.roi
        bbox_key = f"{ul_lon:.4f},{lr_lat:.4f},{lr_lon:.4f},{ul_lat:.4f}"
        cache_key = f'GEDI04_A.003|{bbox_key}'

        if cache_key in GEDIPipeline._v003_availability_cache:
            return GEDIPipeline._v003_availability_cache[cache_key]

        concept_id = 'C4212593885-ORNL_CLOUD'
        try:
            import requests as _req
            # Query CMR restricted to the pipeline ROI bbox
            url = (
                f"https://cmr.earthdata.nasa.gov/search/granules.json"
                f"?concept_id={concept_id}"
                f"&bounding_box={bbox_key}"
                f"&page_size=1"
            )
            resp = _req.get(url, proxies=self._proxies or None, timeout=(10, 20))
            count = len(resp.json().get('feed', {}).get('entry', []))
            if count > 0:
                print("[Pipeline] GEDI04_A V003 granules found for this AOI — using V003.")
                result = '003'
            else:
                print("[Pipeline] GEDI04_A V003: no granules for this AOI — falling back to V002.")
                result = '002'
        except Exception as exc:
            print(f"[Pipeline] GEDI04_A V003 check failed ({exc}) — using V002.")
            result = '002'

        GEDIPipeline._v003_availability_cache[cache_key] = result
        return result

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

        finder = GEDIFinder(
            product=product_name,
            version=self._product_version(product_name),
            date_start=self.date_start,
            date_end=self.date_end,
            recurring_months=self.recurring_months,
            roi=self.roi,
            proxies=self._proxies,
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
                print("[Pipeline] Skipping after 3 failed attempts")
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
                print(f"[Pipeline] {product} granule not available for this orbit — skipping.")
                continue

            subsetter = GEDISubsetter(
                roi=self.roi,
                product=product,
                version=self._product_version(product),
                selected_vars=self.selected_vars.get(product, []),
                filters=self.filters,
                beams=self.beams,
                roi_gdf=self.roi_gdf,
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
        basename = os.path.basename(base_h5_path)
        ref_product = self.products[0]

        if product == ref_product:
            return base_h5_path

        # Extraer orbit number — se usa para L4A
        orbit_num = None
        for p in basename.replace('.h5', '').split('_'):
            if p.startswith('O') and p[1:].isdigit():
                orbit_num = p[1:]
                break

        # L4A and L4C: filename differs from L2A — look up on disk or CMR.
        # Both products live at ORNL DAAC and are matched to L2A by orbit number.
        if product in ('GEDI04_A', 'GEDI04_C'):
            prefix = product
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
        target_path = os.path.join(self.out_directory, target_basename)

        if os.path.exists(target_path):
            return target_path

        print(f"[Pipeline] Downloading {product} granule: {target_basename}")
        target_url = self._derive_url(target_basename, product)
        if target_url and self._download(target_url):
            return os.path.join(self.out_directory, target_url.split('/')[-1])

        print(f"[Pipeline] Warning: could not obtain {product} for this granule")
        return None

    def _temporal_from_filename(self, filename):
        parts = filename.replace('.h5', '').split('_')
        datetime_field = parts[2] if len(parts) > 2 else ''
        if len(datetime_field) < 7:
            return None
        try:
            import datetime
            year = int(datetime_field[:4])
            doy = int(datetime_field[4:7])
            dt = datetime.datetime(year, 1, 1) + datetime.timedelta(days=doy - 1)
            date_start = dt.strftime('%Y-%m-%dT00:00:00Z')
            date_end = (dt + datetime.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')
            return f"{date_start},{date_end}"
        except Exception:
            return None

    def _entry_h5_url(self, entries_list, orbit_num=None, strict=True):
        for entry in entries_list:
            title = (entry.get('producer_granule_id', '')
                     or entry.get('title', ''))
            if strict and orbit_num and f"O{orbit_num}" not in title:
                continue
            for link in entry.get('links', []):
                href = link.get('href', '')
                if (href.endswith('.h5')
                        and href.startswith('https://')
                        and 's3://' not in href
                        and 'opendap' not in href):
                    return href
        return None

    def _derive_url(self, filename, product):
        """
        Construye la URL de descarga para un producto usando la API CMR de NASA.
        - L2A/L2B: nombre de archivo idéntico salvo el shortname → búsqueda directa
          por producer_granule_id.
        - L4A y L4C: nombre de archivo difiere del L2A → búsqueda por orbit number
          + bbox + temporal en ORNL_CLOUD. L4C usa short_name (más estable que el
          concept_id para productos cuya revisión cambia con frecuencia).
        """
        import requests as req

        # Selector CMR por producto. concept_id para todos los productos.
        # L4C tiene fallback runtime en caso de que ORNL re-ingeste.
        # Use finder's unified resolver — covers V002, V003, and runtime fallback.
        # GEDI04_C is always resolved as .002 (V003 not yet available).
        from .finder import concept_ids as _finder_concept_ids, _resolve_concept_id
        _effective_version = self._product_version(product)
        key = f"{product}.{_effective_version}"

        _cid = _finder_concept_ids.get(key)
        if _cid is None:
            # None means V003 pending — resolve at runtime via DOI
            _cid = _resolve_concept_id(key, proxies=self._proxies)
        if not _cid:
            print(f"[Pipeline] No concept_id available for {key} — cannot derive URL.")
            return None
        cmr_selector = f"concept_id={_cid}"

        # Extraer orbit number del nombre de archivo (ej: O02577)
        # El nombre tiene formato: GEDI02_A_YYYYDDDHHMMSS_OXXXXX_...
        orbit_num = None
        parts = filename.replace('.h5', '').split('_')
        for p in parts:
            if p.startswith('O') and p[1:].isdigit():
                orbit_num = p[1:]   # ej: '02577'
                break

        # ── Productos LP DAAC (L2A, L2B) ──
        # El nombre es casi idéntico al L2A — buscar directo por producer_granule_id
        if product not in ('GEDI04_A', 'GEDI04_C'):
            granule_id = filename.replace('.h5', '')
            cmr_url = (
                f"https://cmr.earthdata.nasa.gov/search/granules.json"
                f"?{cmr_selector}"
                f"&producer_granule_id={granule_id}"
                f"&page_size=1"
            )
            try:
                resp = req.get(cmr_url, proxies=self._proxies or None, timeout=(10, 30))
                entries = resp.json().get('feed', {}).get('entry', [])
                if entries:
                    for link in entries[0].get('links', []):
                        href = link.get('href', '')
                        if href.endswith('.h5'):
                            print(f"[Pipeline] Found URL for {product}: {href}")
                            return href
            except Exception as e:
                print(f"[Pipeline] CMR search error for {product}: {e}")

            # V003 producer_granule_id can differ by product even when the
            # orbit/date/tile match. Fall back to temporal + bbox + orbit.
            try:
                [ul_lat, ul_lon, lr_lat, lr_lon] = self.roi
                bbox = f"{ul_lon},{lr_lat},{lr_lon},{ul_lat}"
                date_str = self._temporal_from_filename(filename)
                queries = []
                if date_str:
                    orbit_param = orbit_num.lstrip('0') if orbit_num else ''
                    if orbit_param:
                        queries.append((
                            "orbit + temporal + bbox",
                            f"https://cmr.earthdata.nasa.gov/search/granules.json"
                            f"?{cmr_selector}&temporal={date_str}"
                            f"&bounding_box={bbox}&orbit_number={orbit_param}"
                            f"&page_size=50"
                        ))
                    queries.append((
                        "temporal + bbox",
                        f"https://cmr.earthdata.nasa.gov/search/granules.json"
                        f"?{cmr_selector}&temporal={date_str}"
                        f"&bounding_box={bbox}&page_size=200"
                    ))

                for label, url in queries:
                    resp = req.get(url, proxies=self._proxies or None,
                                   timeout=(10, 45))
                    entries = resp.json().get('feed', {}).get('entry', [])
                    print(f"[Pipeline] CMR fallback ({product}): "
                          f"{len(entries)} entries for {label}")
                    found = self._entry_h5_url(entries, orbit_num, strict=True)
                    if not found and len(entries) == 1:
                        found = self._entry_h5_url(entries, orbit_num, strict=False)
                    if found:
                        print(f"[Pipeline] Found URL for {product}: {found}")
                        return found
            except Exception as e:
                print(f"[Pipeline] CMR fallback error for {product}: {e}")

            print(f"[Pipeline] Could not find URL for {product} / {filename}")
            return None

        # ── Productos ORNL DAAC (L4A, L4C) ──
        # El nombre difiere del L2A — buscar por orbit number, bbox y fecha.
        # L4A tiene 1 granule por órbita. L4C usa sub-orbits: un orbit L2A puede
        # corresponder a múltiples sub-granules L4C, así que aceptamos cualquiera
        # que cubra nuestro bbox.
        if orbit_num is None:
            print(f"[Pipeline] Could not extract orbit from {filename}")
            return None

        print(f"[Pipeline] Searching {product} granule for orbit O{orbit_num}...")

        def _get_ornl_url(entries_list, orbit, strict_orbit=True):
            """Extrae URL HTTP directa de ORNL de una lista de entries CMR.

            Args:
                strict_orbit: si True, exige que O{orbit} aparezca en el título
                              del granule (para L4A). Si False, acepta cualquier
                              entry con link .h5 (para L4C sub-orbits, donde CMR
                              ya filtró por orbit_number en el request).
            """
            for entry in entries_list:
                title = (entry.get('producer_granule_id', '')
                         or entry.get('title', ''))
                if strict_orbit and f"O{orbit}" not in title:
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

        # L4C sub-orbits: no exigir que el título tenga O{orbit}
        # porque el orbit_number en el request CMR ya lo garantiza
        # L4A V003 uses sub-orbits (like L4C) — relaxed orbit matching.
        # L4A V002 has 1 granule/orbit — strict matching.
        strict_orbit = not (product == 'GEDI04_C' or (product == 'GEDI04_A' and self._product_version(product) == '003'))  # noqa: E501

        try:
            # Búsqueda 1: por bbox + orbit_number (parámetro CMR nativo)
            # orbit_number funciona tanto para L4A (1 granule/orbit) como
            # para L4C (n sub-granules/orbit) — CMR filtra por nosotros.
            [ul_lat, ul_lon, lr_lat, lr_lon] = self.roi
            bbox = f"{ul_lon},{lr_lat},{lr_lon},{ul_lat}"
            r1 = req.get(
                f"https://cmr.earthdata.nasa.gov/search/granules.json"
                f"?{cmr_selector}&bounding_box={bbox}"
                f"&orbit_number={orbit_num}&page_size=50",
                proxies=self._proxies or None, timeout=(10, 30))
            entries = r1.json().get('feed', {}).get('entry', [])
            print(f"[Pipeline] CMR search 1 ({product}): "
                  f"{len(entries)} entries for orbit {orbit_num} + bbox")
            url = _get_ornl_url(entries, orbit_num, strict_orbit)
            if url:
                print(f"[Pipeline] Found URL for {product}: {url}")
                return url

            # Búsqueda 2: por fecha derivada + bbox + orbit_number
            # El basename contiene YYYYDDD (día juliano) — convertir a fecha ISO
            date_str = None
            try:
                parts_fn = filename.replace('.h5', '').split('_')
                datetime_field = parts_fn[2] if len(parts_fn) > 2 else ''
                if len(datetime_field) >= 7:
                    import datetime
                    year = int(datetime_field[:4])
                    doy = int(datetime_field[4:7])
                    dt = datetime.datetime(year, 1, 1) + datetime.timedelta(days=doy - 1)
                    date_start = dt.strftime('%Y-%m-%dT00:00:00Z')
                    date_end = (dt + datetime.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')
                    date_str = f"{date_start},{date_end}"
            except Exception:
                pass

            entries2 = []
            if date_str:
                r2 = req.get(
                    f"https://cmr.earthdata.nasa.gov/search/granules.json"
                    f"?{cmr_selector}"
                    f"&temporal={date_str}"
                    f"&bounding_box={bbox}"
                    f"&orbit_number={orbit_num}"
                    f"&page_size=50",
                    proxies=self._proxies or None, timeout=(10, 30))
                entries2 = r2.json().get('feed', {}).get('entry', [])
                print(f"[Pipeline] CMR search 2 ({product}): "
                      f"{len(entries2)} entries for orbit {orbit_num} + bbox + temporal")
            url2 = _get_ornl_url(entries2, orbit_num, strict_orbit)
            if url2:
                print(f"[Pipeline] Found URL for {product}: {url2}")
                return url2

            # Búsqueda 3: sin orbit_number — solo temporal + bbox, buscar
            # entre todos los resultados el que contenga O{orbit}. Cubre el
            # caso donde CMR no soporta orbit_number para este collection.
            entries3 = []
            if date_str:
                r3 = req.get(
                    f"https://cmr.earthdata.nasa.gov/search/granules.json"
                    f"?{cmr_selector}"
                    f"&temporal={date_str}"
                    f"&bounding_box={bbox}"
                    f"&page_size=200",
                    proxies=self._proxies or None, timeout=(10, 45))
                entries3 = r3.json().get('feed', {}).get('entry', [])
                print(f"[Pipeline] CMR search 3 ({product}): "
                      f"{len(entries3)} entries for temporal + bbox (no orbit filter)")
            # Try strict orbit match first, then relaxed
            url3 = _get_ornl_url(entries3, orbit_num, strict_orbit=True)
            if url3:
                print(f"[Pipeline] Found URL for {product}: {url3}")
                return url3
            # Relaxed: accept any entry with .h5 link
            url3r = _get_ornl_url(entries3, orbit_num, strict_orbit=False)
            if url3r:
                print(f"[Pipeline] Found URL for {product} (relaxed): {url3r}")
                return url3r

            # Búsqueda 4: last-ditch fallback from all earlier results
            for e_list in [entries, entries2, entries3]:
                for entry in e_list:
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

        # Sufijos por producto. Se usa una tabla explícita para evitar errores
        # con los productos cuya conversión por replace() encadenado da un
        # resultado incorrecto (ej. GEDI04_C). Sufijos como _L2A o _L4C son
        # los que verá el usuario en columnas duplicadas (sensitivity_L2A, etc.).
        PRODUCT_SUFFIXES = {
            'GEDI02_A': '_L2A',
            'GEDI02_B': '_L2B',
            'GEDI04_A': '_L4A',
            'GEDI04_C': '_L4C',
        }
        merged = gdfs[products[0]].copy()

        for product in products[1:]:
            right = gdfs[product].copy()

            # Columnas a NO duplicar del lado derecho
            # Se eliminan campos base + campos que se repiten en todos los productos
            drop_cols = [c for c in right.columns
                         if c in _BASE_COLS and c != 'shot_number']
            right = right.drop(columns=drop_cols, errors='ignore')

            # Añadir sufijo al producto para columnas que se repiten
            suffix = PRODUCT_SUFFIXES.get(product, f"_{product}")

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
        Aplica el filtro de sensitivity después del merge.

        No crea la columna auxiliar quality_passed. Si el usuario marcó
        sensitivity para un producto y existe su columna, el shot se elimina
        solo cuando ese producto tiene dato y su sensitivity está bajo el umbral.
        En outer join, los NaN de productos ausentes no penalizan el shot.
        """
        f = self.filters
        sens_cfg = f.get('sensitivity', {})
        min_sens = sens_cfg.get('value', 0.90)
        apply_to = sens_cfg.get('apply_to', {})

        sens_cols = [c for c in gdf.columns if 'sensitivity' in c.lower()]
        if not sens_cols:
            return gdf

        conditions = []
        for col in sens_cols:
            product = self._product_from_sensitivity_column(col)
            if product and apply_to.get(product, False):
                has_data = gdf[col].notna()
                passes = gdf[col].fillna(min_sens + 1) >= min_sens
                conditions.append(~has_data | passes)

        if not conditions:
            return gdf

        import functools
        keep_mask = functools.reduce(lambda a, b: a & b, conditions)
        before = len(gdf)
        gdf = gdf[keep_mask].copy()
        removed = before - len(gdf)
        pct = 0 if before == 0 else len(gdf) / before * 100
        print(f"[Pipeline] sensitivity >= {min_sens}: retained "
              f"{len(gdf)}/{before} shots ({pct:.1f}%), removed {removed}")
        return gdf

    def _product_from_sensitivity_column(self, col: str):
        """Mapea columnas sensitivity/sensitivity_L2B/sensitivity_L4A/sensitivity_L4C a producto."""
        if col == 'sensitivity':
            # La columna sin sufijo pertenece al primer producto del merge.
            return self.products[0] if self.products else None
        if '_L2A' in col:
            return 'GEDI02_A'
        if '_L2B' in col:
            return 'GEDI02_B'
        if '_L4A' in col:
            return 'GEDI04_A'
        if '_L4C' in col:
            return 'GEDI04_C'
        return None

    # ── Limpieza y orden de columnas ──────────────────────────
    def _finalize_output_columns(self, gdf: gp.GeoDataFrame) -> gp.GeoDataFrame:
        """
        Prepara la tabla de salida:
          - elimina columnas auxiliares internas (quality_passed);
          - mueve todos los *_quality_flag al final de la tabla de atributos.
        l2_quality_flag se conserva si aparece (caso L4C) como flag secundario.
        """
        if gdf is None or len(gdf.columns) == 0:
            return gdf

        crs = getattr(gdf, 'crs', None)
        geom_col = None
        try:
            geom_col = gdf.geometry.name
        except Exception:
            if 'geometry' in gdf.columns:
                geom_col = 'geometry'

        drop_cols = [c for c in _OUTPUT_DROP_COLS if c in gdf.columns]
        if drop_cols:
            gdf = gdf.drop(columns=drop_cols, errors='ignore')

        non_geom_cols = [c for c in gdf.columns if c != geom_col]
        quality_cols = [
            c for c in non_geom_cols
            if c.lower() == 'quality_flag' or c.lower().endswith('_quality_flag')
        ]
        quality_cols = sorted(
            quality_cols,
            key=lambda c: (_QUALITY_FLAG_ORDER.get(c.lower(), 99), c.lower())
        )
        regular_cols = [c for c in non_geom_cols if c not in quality_cols]

        ordered_cols = regular_cols + quality_cols
        if geom_col and geom_col in gdf.columns:
            ordered_cols.append(geom_col)

        gdf = gdf.loc[:, ordered_cols].copy()
        if geom_col and geom_col in gdf.columns:
            gdf = gp.GeoDataFrame(gdf, geometry=geom_col, crs=crs)
        return gdf

    # ── Export ────────────────────────────────────────────────
    def _export(self, gdf: gp.GeoDataFrame, stem: str):
        gdf = self._finalize_output_columns(gdf)
        if gdf is None or len(gdf) == 0:
            print(f"[Pipeline] Empty output for {stem} - nothing saved.")
            return

        if self.out_gpkg:
            out_path = os.path.join(self.out_directory, f"{stem}_merged.gpkg")
            try:
                gdf.to_file(out_path, driver='GPKG')
                print(f"[Pipeline] Saved GeoPackage: {os.path.basename(out_path)}")
                self._run_gpkg_outputs.append(out_path)
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
        import glob  # noqa: F401
        from datetime import datetime

        out_dir = self.out_directory

        # Use only files produced in the current run. This avoids merging stale
        # outputs from earlier runs that happened to use the same output folder.
        granule_gpkgs = list(getattr(self, '_run_gpkg_outputs', []))

        if len(granule_gpkgs) == 0:
            print("[Pipeline] No output files produced. All footprints were "
                  "removed by filters. Consider relaxing quality, degrade, "
                  "or surface settings, or using a wider date range.")
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

        merged = self._finalize_output_columns(merged)

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
