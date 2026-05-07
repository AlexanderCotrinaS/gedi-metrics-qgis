"""
GEDIMetrics v1.0.2 — finder.py

Changes vs v1.0.1:
  - All CMR requests carry explicit (connect, read) timeouts
  - Proxy support: accepts proxies dict forwarded from pipeline
  - Pagination loop guarded against malformed JSON
  - Cleaner error messages
"""

import os
import requests as r
from datetime import datetime

concept_ids = {
    'GEDI01_B.002': 'C2142749196-LPCLOUD',
    'GEDI02_A.002': 'C2142771958-LPCLOUD',
    'GEDI02_B.002': 'C2142776747-LPCLOUD',
    'GEDI04_A.002': 'C2237824918-ORNL_CLOUD',
    'GEDI04_C.002': 'C3049900163-ORNL_CLOUD',
}

# Fallback candidates if the hardcoded L4C concept_id stops working
# (e.g. after an ORNL DAAC re-ingest). The runtime resolver tries
# these short_names and then the DOI against the CMR collections endpoint.
_L4C_SHORT_NAME_CANDIDATES = ['GEDI_L4C_WSCI', 'GEDI04_C']
_L4C_DOI = '10.3334/ORNLDAAC/2338'

# Cache so we only hit CMR once per session
_l4c_concept_id_cache = None


def _resolve_l4c_concept_id(proxies=None):
    """Discover the CMR concept_id for GEDI L4C at runtime.

    Tries multiple candidate short_names against the CMR collections
    endpoint, then falls back to DOI lookup. Caches the result.
    Returns the concept_id string or None.
    """
    global _l4c_concept_id_cache
    if _l4c_concept_id_cache is not None:
        return _l4c_concept_id_cache

    import requests as req

    # Try each candidate short_name
    for sn in _L4C_SHORT_NAME_CANDIDATES:
        try:
            url = (
                f"https://cmr.earthdata.nasa.gov/search/collections.json"
                f"?short_name={sn}&version=2&provider=ORNL_CLOUD&page_size=1"
            )
            resp = req.get(url, proxies=proxies or None,
                           timeout=(CMR_CONNECT_TIMEOUT, CMR_READ_TIMEOUT))
            entries = resp.json().get('feed', {}).get('entry', [])
            if entries:
                cid = entries[0].get('id', '')
                if cid:
                    _l4c_concept_id_cache = cid
                    print(f"[Finder] L4C concept_id resolved: {cid} "
                          f"(short_name={sn})")
                    return cid
        except Exception as e:
            print(f"[Finder] L4C collection lookup ({sn}) failed: {e}")

    # Fallback: search by DOI
    try:
        url = (
            f"https://cmr.earthdata.nasa.gov/search/collections.json"
            f"?doi={_L4C_DOI}&page_size=1"
        )
        resp = req.get(url, proxies=proxies or None,
                       timeout=(CMR_CONNECT_TIMEOUT, CMR_READ_TIMEOUT))
        entries = resp.json().get('feed', {}).get('entry', [])
        if entries:
            cid = entries[0].get('id', '')
            if cid:
                _l4c_concept_id_cache = cid
                print(f"[Finder] L4C concept_id resolved via DOI: {cid}")
                return cid
    except Exception as e:
        print(f"[Finder] L4C DOI lookup failed: {e}")

    print("[Finder] WARNING: Could not resolve L4C concept_id from CMR. "
          "L4C granules will not be found.")
    return None

CMR_CONNECT_TIMEOUT = 15
CMR_READ_TIMEOUT    = 45


class GEDIFinder:
    """
    Queries NASA CMR for GEDI granules overlapping a ROI within a date range.

    Args:
        product          : GEDI shortname, e.g. 'GEDI02_A'
        version          : '002'
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

        self.product  = product
        self.version  = version
        self.proxies  = proxies or {}

        try:
            self.date_start = datetime.strptime(date_start, "%Y.%m.%d")
            self.date_end   = datetime.strptime(date_end,   "%Y.%m.%d")
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
        page       = 1
        bbox       = self.roi.replace(" ", ",")
        key        = f"{self.product}.{self.version}"

        # Build the query selector. All products use hardcoded concept_ids.
        # The elif branch is a safety net for L4C in case ORNL re-ingests
        # the collection under a new id (the runtime resolver will find it).
        if key in concept_ids:
            concept_id = concept_ids[key]
            provider   = concept_id.split("-")[-1]
            selector   = f"concept_id={concept_id}"
        elif self.product == 'GEDI04_C':
            concept_id = _resolve_l4c_concept_id(proxies=self.proxies)
            if not concept_id:
                return []
            provider = concept_id.split("-")[-1]
            selector = f"concept_id={concept_id}"
        else:
            print(f"[Finder] Unknown product key: {key}")
            return []

        base_url = (
            f"https://cmr.earthdata.nasa.gov/search/granules.json"
            f"?pretty=true&provider={provider}&page_size=2000"
            f"&{selector}"
        )

        all_entries = []
        while True:
            url = f"{base_url}&bounding_box={bbox}&pageNum={page}"
            try:
                resp    = r.get(url,
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
            href = links[0].get("href", "")
            if ".png" in href:
                continue
            granules.append((href, entry.get("granule_size", 0)))

        return granules

    def __date_filter(self, granules):
        filtered   = []
        rec_months = set(range(self.date_start.month, self.date_end.month + 1))
        for g in granules:
            name = g[0].split("/")[-1]
            try:
                date_sec = datetime.strptime(name.split("_")[2][0:7], "%Y%j")
            except (IndexError, ValueError):
                continue
            if date_sec > self.date_end:
                break
            if self.recurring_months and date_sec.month not in rec_months:
                continue
            if self.date_start <= date_sec <= self.date_end:
                filtered.append(g)
        return filtered

    def __check_download_size(self, link_list):
        return sum(float(l[1]) for l in link_list) / 1000

    # ── public ────────────────────────────────────────────────────────────────
    def find(self, save_file=True, output_filepath=None) -> list:
        all_granules = self.__find_all_granules()
        print(f"[Finder] Found {len(all_granules)} granules over bbox [{self.roi}]")

        filtered = self.__date_filter(all_granules)
        print(
            f"[Finder] Between dates ({self.date_start}) and ({self.date_end})"
            f" exist {len(filtered)} granules over bbox [{self.roi}]"
        )
        print(f"[Finder] Estimated download size: "
              f"{self.__check_download_size(filtered):.2f} GB")

        if save_file and filtered:
            ts       = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{self.product.replace('.','_')}_GranuleList_{ts}.txt"
            out_dir  = output_filepath or ""
            with open(os.path.join(out_dir, filename), "w") as fh:
                for g in filtered:
                    fh.write(f"{g[0]}\n")
            print(f"[Finder] Saved links to {os.path.join(out_dir, filename)}")

        return filtered
