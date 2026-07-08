"""
GEDIMetrics v1.0.5 — finder.py

Changes vs v1.0.2:
  - Added V003 concept_ids for GEDI02_A, GEDI02_B, GEDI04_A (LPCLOUD / ORNL_CLOUD)
  - V003 concept_ids resolved at runtime via DOI when not yet hardcoded (future-proof)
  - GEDI04_C is always forced to V002 (V003 not yet available as of June 2026)
  - Generalised runtime resolver: _resolve_concept_id() replaces the L4C-only resolver
  - Per-product version routing in __find_all_granules()
  - Preserved all V002 concept_ids and the L4C-specific short_name candidates
"""

import os
import requests as r
from datetime import datetime

# ── Hardcoded concept_ids ──────────────────────────────────────────────────────
# V002 — confirmed and stable.
# V003 — L2A and L2B at LPCLOUD, L4A at ORNL_CLOUD.
#        IDs marked PENDING are resolved at runtime via DOI (see _resolve_concept_id).
#        Update these once NASA publishes the official IDs.
concept_ids = {
    # ── V002 (complete, stable) ────────────────────────────────────────────────
    'GEDI01_B.002': 'C2142749196-LPCLOUD',
    'GEDI02_A.002': 'C2142771958-LPCLOUD',
    'GEDI02_B.002': 'C2142776747-LPCLOUD',
    'GEDI04_A.002': 'C2237824918-ORNL_CLOUD',
    'GEDI04_C.002': 'C3049900163-ORNL_CLOUD',

    # ── V003 (confirmed June 2026) ───────────────────────────────────────────
    # Concept IDs verified from NASA Earthdata catalog on 2026-06-18.
    'GEDI02_A.003': 'C3974616071-LPCLOUD',
    'GEDI02_B.003': 'C3974616135-LPCLOUD',
    'GEDI04_A.003': 'C4212593885-ORNL_CLOUD',
    # GEDI04_C.003 intentionally absent: V003 not yet released (June 2026).
}

# ── V003 DOIs for runtime resolver ────────────────────────────────────────────
# Used when the concept_id entry above is None.
_V003_DOIS = {
    'GEDI02_A.003': '10.5067/GEDI/GEDI02_A.003',
    'GEDI02_B.003': '10.5067/GEDI/GEDI02_B.003',
    'GEDI04_A.003': '10.3334/ORNLDAAC/2508',
}

# ── L4C V002 short_name candidates (fallback if hardcoded id changes) ─────────
_L4C_SHORT_NAME_CANDIDATES = ['GEDI_L4C_WSCI', 'GEDI04_C']
_L4C_DOI = '10.3334/ORNLDAAC/2338'

# ── Products locked to V002 (V003 does not exist yet) ─────────────────────────
# When the pipeline passes version='003' for any of these, the finder
# silently downgrades to '002' and logs one informational message.
_V002_ONLY_PRODUCTS = {'GEDI04_A', 'GEDI04_C'}

# ── Per-session concept_id cache (keyed by 'PRODUCT.VERSION') ─────────────────
_concept_id_cache: dict = {}

CMR_CONNECT_TIMEOUT = 15
CMR_READ_TIMEOUT = 45


# ── Runtime concept_id resolver ───────────────────────────────────────────────

def _resolve_concept_id(product_key: str, proxies: dict = None):
    """Resolve a CMR concept_id at runtime for a given 'PRODUCT.VERSION' key.

    Resolution order:
      1. Return cached value if already resolved this session.
      2. Try the DOI registered in _V003_DOIS (primary path for V003).
      3. For GEDI04_C.002 only: also try short_name candidates (_L4C_SHORT_NAME_CANDIDATES).

    Args:
        product_key : e.g. 'GEDI02_A.003' or 'GEDI04_C.002'
        proxies     : requests proxy dict

    Returns:
        concept_id string, or None if all strategies fail.
    """

    if product_key in _concept_id_cache:
        return _concept_id_cache[product_key]

    import requests as req

    def _doi_lookup(doi: str):
        try:
            url = (
                f"https://cmr.earthdata.nasa.gov/search/collections.json"
                f"?doi={doi}&page_size=1"
            )
            resp = req.get(url, proxies=proxies or None,
                           timeout=(CMR_CONNECT_TIMEOUT, CMR_READ_TIMEOUT))
            entries = resp.json().get('feed', {}).get('entry', [])
            if entries:
                return entries[0].get('id', '') or None
        except Exception as exc:
            print(f"[Finder] DOI lookup ({doi}) failed: {exc}")
        return None

    def _short_name_lookup(short_name: str, version: str,
                           provider: str):
        try:
            url = (
                f"https://cmr.earthdata.nasa.gov/search/collections.json"
                f"?short_name={short_name}&version={version}"
                f"&provider={provider}&page_size=1"
            )
            resp = req.get(url, proxies=proxies or None,
                           timeout=(CMR_CONNECT_TIMEOUT, CMR_READ_TIMEOUT))
            entries = resp.json().get('feed', {}).get('entry', [])
            if entries:
                return entries[0].get('id', '') or None
        except Exception as exc:
            print(f"[Finder] Short-name lookup ({short_name}) failed: {exc}")
        return None

    cid = None

    # ── Path 1: DOI lookup (V003 products and L4C V002) ───────────────────────
    doi = _V003_DOIS.get(product_key) or (
        _L4C_DOI if product_key == 'GEDI04_C.002' else None
    )
    if doi:
        cid = _doi_lookup(doi)
        if cid:
            print(f"[Finder] concept_id resolved via DOI ({doi}): {cid}")

    # ── Path 2: short_name candidates (L4C only) ──────────────────────────────
    if cid is None and product_key == 'GEDI04_C.002':
        for sn in _L4C_SHORT_NAME_CANDIDATES:
            cid = _short_name_lookup(sn, version='2', provider='ORNL_CLOUD')
            if cid:
                print(f"[Finder] L4C concept_id resolved via short_name={sn}: {cid}")
                break

    if cid:
        _concept_id_cache[product_key] = cid
        return cid

    print(f"[Finder] WARNING: Could not resolve concept_id for {product_key}.")
    return None


# ── Public helper — kept for backward compatibility with pipeline.py ──────────
def _resolve_l4c_concept_id(proxies=None):
    """Backward-compatible wrapper — resolve GEDI04_C.002 concept_id."""
    return _resolve_concept_id('GEDI04_C.002', proxies=proxies)


# ── GEDIFinder ────────────────────────────────────────────────────────────────

class GEDIFinder:
    """
    Queries NASA CMR for GEDI granules overlapping a ROI within a date range.

    Args:
        product          : GEDI shortname, e.g. 'GEDI02_A'
        version          : '002' or '003'
                           Products in _V002_ONLY_PRODUCTS are always queried
                           as '002' regardless of this value.
        date_start       : 'YYYY.MM.DD'
        date_end         : 'YYYY.MM.DD'
        recurring_months : filter to months of date range across all years
        roi              : [UL_lat, UL_lon, LR_lat, LR_lon] EPSG:4326
        proxies          : requests proxy dict, e.g. {'https': 'http://proxy:8080'}
    """

    def __init__(self, product='GEDI02_A', version='002',
                 date_start='', date_end='',
                 recurring_months=False, roi=None,
                 proxies=None):

        self.product = product
        self.proxies = proxies or {}

        # ── Version routing ───────────────────────────────────────────────────
        # GEDI04_C has no V003 yet — always use V002 and inform the user once.
        if product in _V002_ONLY_PRODUCTS and version != '002':
            print(
                f"[Finder] {product}: V003 is not usable yet - "
                f"using V002 for this product."
            )
            self.version = '002'
        else:
            self.version = version

        try:
            self.date_start = datetime.strptime(date_start, "%Y.%m.%d")
            self.date_end = datetime.strptime(date_end, "%Y.%m.%d")
        except ValueError:
            print("[Finder] Invalid date format — expected YYYY.MM.DD")
            raise

        if roi is not None:
            ul_lat, ul_lon, lr_lat, lr_lon = roi
            self.roi = f"{ul_lon} {lr_lat} {lr_lon} {ul_lat}"
        else:
            self.roi = None

        self.recurring_months = recurring_months
        if self.recurring_months:
            print("[Finder] recurring_months=True — filtering by month range.")

    # ── internal ──────────────────────────────────────────────────────────────

    def __find_all_granules(self):
        page = 1
        bbox = self.roi.replace(" ", ",")
        key = f"{self.product}.{self.version}"

        # ── Concept_id resolution ─────────────────────────────────────────────
        # Priority:
        #   1. Hardcoded dict (not None)  → use directly
        #   2. None in dict               → runtime resolver via DOI
        #   3. Key absent                 → runtime resolver (unknown product)
        if key in concept_ids and concept_ids[key] is not None:
            concept_id = concept_ids[key]
            print(f"[Finder] Using hardcoded concept_id for {key}: {concept_id}")
        else:
            # Covers None entries (V003 pending) and any unknown key
            concept_id = _resolve_concept_id(key, proxies=self.proxies)
            if not concept_id:
                print(f"[Finder] No concept_id available for {key} — aborting search.")
                return []

        provider = concept_id.split("-")[-1]
        selector = f"concept_id={concept_id}"

        base_url = (
            f"https://cmr.earthdata.nasa.gov/search/granules.json"
            f"?pretty=true&provider={provider}&page_size=2000"
            f"&{selector}"
        )

        all_entries = []
        while True:
            url = f"{base_url}&bounding_box={bbox}&pageNum={page}"
            try:
                resp = r.get(url,
                             proxies=self.proxies or None,
                             timeout=(CMR_CONNECT_TIMEOUT, CMR_READ_TIMEOUT))
                resp.raise_for_status()
                entries = resp.json().get("feed", {}).get("entry", [])
            except r.exceptions.ConnectTimeout:
                print(
                    "[Finder] Connect timeout reaching CMR.\n"
                    "  Check network/proxy settings."
                )
                return []
            except r.exceptions.ProxyError as exc:
                print(f"[Finder] Proxy error: {exc}")
                return []
            except Exception as exc:
                print(f"[Finder] CMR request failed: {exc}")
                return []

            all_entries.extend(entries)
            if len(entries) < 2000:
                break
            page += 1

        granules = []
        for entry in all_entries:
            links = entry.get("links", [])
            if not links:
                continue
            # Scan all links for the first direct HTTPS .h5 data URL.
            # V003 CMR entries place metadata/HTML links before the data link,
            # so taking links[0] blindly misses the actual file URL.
            href = None
            for link in links:
                candidate = link.get("href", "")
                if (candidate.endswith(".h5")
                        and candidate.startswith("https://")
                        and "s3://" not in candidate):
                    href = candidate
                    break
            # Fallback: accept any non-png, non-html link if no .h5 found
            if href is None:
                for link in links:
                    candidate = link.get("href", "")
                    if ".png" not in candidate and not candidate.endswith(".html"):
                        href = candidate
                        break
            if href is None:
                continue
            granules.append((href, entry.get("granule_size", 0)))

        return granules

    def __date_filter(self, granules):
        # NOTE: No early-exit break — CMR does not guarantee chronological order.
        # V003 collections (and some V002 ORNL collections) return newest-first,
        # which caused the old break to fire on the very first granule and return
        # 0 results even when the date range contained valid data.
        filtered = []
        rec_months = set(range(self.date_start.month, self.date_end.month + 1))
        for g in granules:
            name = g[0].split("/")[-1]
            try:
                date_sec = datetime.strptime(name.split("_")[2][0:7], "%Y%j")
            except (IndexError, ValueError):
                continue
            if self.recurring_months and date_sec.month not in rec_months:
                continue
            if self.date_start <= date_sec <= self.date_end:
                filtered.append(g)
        return filtered

    def __check_download_size(self, link_list):
        return sum(float(lnk[1]) for lnk in link_list) / 1000

    # ── public ────────────────────────────────────────────────────────────────

    def find(self, save_file=True, output_filepath=None) -> list:
        all_granules = self.__find_all_granules()
        print(f"[Finder] Found {len(all_granules)} granules over bbox [{self.roi}]")

        filtered = self.__date_filter(all_granules)
        print(
            f"[Finder] Between dates ({self.date_start}) and ({self.date_end})"
            f" exist {len(filtered)} granules over bbox [{self.roi}]"
        )
        print(
            f"[Finder] Estimated download size: "
            f"{self.__check_download_size(filtered):.2f} GB"
        )

        if save_file and filtered:
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{self.product.replace('.', '_')}_GranuleList_{ts}.txt"
            out_dir = output_filepath or ""
            with open(os.path.join(out_dir, filename), "w") as fh:
                for g in filtered:
                    fh.write(f"{g[0]}\n")
            print(f"[Finder] Saved links to {os.path.join(out_dir, filename)}")

        return filtered
