# GEDIMetrics — QGIS Plugin

> Download, subset, and merge NASA GEDI LiDAR products directly inside QGIS.

![QGIS](https://img.shields.io/badge/QGIS-3.4%20%7C%204.0-green?logo=qgis)
![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![License](https://img.shields.io/badge/License-GPL--2.0-orange)
![Version](https://img.shields.io/badge/Version-1.0.5-brightgreen)

---

## What is GEDIMetrics?

**GEDIMetrics** is a QGIS plugin that provides a graphical interface to search, download, subset, and merge NASA GEDI spaceborne LiDAR data — without leaving QGIS and without writing a single line of code.

It integrates four GEDI products in a single workflow:

| Product | Description | Default Version |
|---------|-------------|-----------------|
| **L2A** | Canopy height (RH metrics: rh10, rh25, rh50, rh75, rh98, rh100…) | **V003** |
| **L2B** | Vertical structure (canopy cover, PAI, PAVD, FHD, Pgap) | **V003** |
| **L4A** | Aboveground biomass density (AGBD) | **V003** |
| **L4C** | Waveform Structural Complexity Index (WSCI) | V002 ¹ |

> ¹ L4C V003 is not yet released by NASA. The plugin will switch automatically when it becomes available.

Results are merged by `shot_number` and exported as **GeoPackage** (`.gpkg`) and/or **GeoParquet** (`.parquet`), ready to use in QGIS.

---

## Key Features

- 🌍 **Area of Interest** — use any polygon vector layer loaded in QGIS (or load a file directly)
- 📅 **Date range & recurring months** — filter data by date and optionally by specific months
- 🔦 **Beam selection** — choose power beams, coverage beams, or all beams
- ✅ **Variable selection** — pick exactly which GEDI variables you need (L2A, L2B, L4A, L4C)
- 🔧 **Quality filters** — apply quality flags, sensitivity thresholds, degrade exclusion, and surface filters per product
- 💾 **Output formats** — GeoPackage and/or GeoParquet
- 🔐 **NASA EarthData authentication** — Bearer Token mode for reliable connectivity
- 📋 **Live log panel** — follow the pipeline progress inside the QGIS dialog
- ❌ **Cancel button** — stop a running job at any time
- 🔄 **Auto-load results** — output layers load automatically into the QGIS canvas

---
## Installation

### Option 1 — QGIS Plugin Repository (easiest)

1. Open QGIS → **Plugins** → **Manage and Install Plugins…**
2. Go to **Settings** → enable **Show experimental plugins**
3. Search for **GEDIMetrics** → click **Install**

### Option 2 — Install from ZIP

1. Download the latest ZIP from the [Releases](https://github.com/AlexanderCotrinaS/gedi-metrics-qgis/releases) page
2. Open QGIS → **Plugins** → **Manage and Install Plugins…**
3. Click **Install from ZIP** → select the downloaded file
4. Enable the plugin

---

## Python Dependencies

GEDIMetrics **automatically installs missing dependencies** on first run. In most cases no manual action is needed.

If the auto-install fails, follow the steps below for your operating system.

### Windows — OSGeo4W (strongly recommended)

> ⚠️ **Important:** On Windows, install QGIS through **OSGeo4W** — not the standalone installer. The standalone version lacks the binary packages required for `h5py` and `fiona`, which cannot be reliably installed via pip on Windows.

**Step 1 — Install QGIS via OSGeo4W:**

1. Download the **OSGeo4W installer** from [https://trac.osgeo.org/osgeo4w/](https://trac.osgeo.org/osgeo4w/)
2. Run it → choose **Advanced Install**
3. On the **Select Packages** screen, search and mark:
   - `qgis` (or `qgis-ltr` for the long-term release)
   - `python3-h5py`
   - `python3-pandas`
   - `python3-geopandas`
   - `python3-shapely`
   - `python3-fiona`
   - `python3-requests`
4. Complete the installation

**Step 2 — Install GEDIMetrics** using either option above. The plugin auto-installs any remaining packages on first run.

### Linux — Debian / Ubuntu

```bash
sudo apt install python3-h5py python3-pandas python3-geopandas \
                 python3-shapely python3-fiona python3-rtree python3-requests
```

### macOS

```bash
pip install h5py pandas geopandas shapely fiona rtree requests
```

---

## NASA EarthData Authentication

GEDIMetrics uses **Bearer Token** authentication — the safest and most reliable method, compatible with university and corporate firewalls.

1. Register at [NASA EarthData](https://urs.earthdata.nasa.gov/) (free)
2. Log in → go to **My Profile** → **Generate Token**
3. Copy the token and paste it in the **EarthData** tab of the plugin

---

## Usage

1. Open the plugin from the **Plugins** menu or the toolbar button
2. Select your **output folder**
3. Choose **products** (L2A, L2B, L4A, L4C) and **version** (default: V003)
4. Set the **date range** and optionally filter by recurring months
5. Select a **polygon layer** as your area of interest (ROI)
6. Enter your **NASA EarthData Bearer Token** in the EarthData tab
7. Choose **beams**, **variables**, and **quality filters**
8. Select **output formats** (GeoPackage / GeoParquet)
9. Click **Run** — progress appears in the log panel
10. Results load automatically into QGIS when done

---
## Changelog

### v1.0.5 (2026-06-18)
- **GEDI V003 support:** V003 is now the default version for L2A, L2B, and L4A
- **Per-product version routing:** L2A/L2B/L4A use V003 while L4C automatically falls back to V002
- **L4A V003 availability check:** queries CMR at runtime scoped to the pipeline ROI — avoids false positives from granules in other regions
- **CMR link scanning:** fixed granule URL extraction for V003 (CMR places metadata links before data links in V003 entries)
- **Date filter fix:** removed premature loop break that caused 0 results when CMR returns granules in reverse-chronological order
- **L2B V003 geometry paths:** added `_resolve_geo_paths()` fallback — fixes "No intersecting shots" error caused by lat/lon path changes in V003
- **L2A V003 quality flag:** corrected path from `/quality_flag` to `/l2a_quality_flag_rel3` (root beam level, confirmed V003 HDF5 structure)
- **Version lock UI:** version combo defaults to V003; informational tooltip shown when L4C is selected

### v1.0.4 (2026-06-10)
- **Auto-install dependencies:** missing packages (`h5py`, `pandas`, `geopandas`, `numpy`, `shapely`, `requests`) installed automatically via pip on first run
- **Improved error handling:** smart error classification with actionable messages (ImportError, auth, network, permissions, ROI errors)
- **Removed blocking checks:** `fiona`, `rtree`, `pyarrow` no longer block startup — handled internally
- **Concise log messages:** single-line, no bullet points, no tutorial tone

### v1.0.3 (2026-06-06)
- **macOS fix:** `_make_retry()` handles both `urllib3` `allowed_methods` (≥1.26) and `method_whitelist` (<1.26)
- **Degrade flag UI:** label corrected from `= 1` to `≠ 0` (non-binary V002 codes, Table 6 GEDI L2 User Guide)

### v1.0.2 (2026-05-07)
- **New product:** GEDI L4C (Waveform Structural Complexity Index — WSCI)
- **Authentication:** Bearer Token replaces cookie-based login
- **Quality filter defaults:** quality flags default to 1, exclude degraded footprints enabled, surface filter defaults to Land
- **Improved CMR search:** ORNL DAAC products use multi-cascade search for reliable granule discovery

### v1.0.1 (2026-04-26)
- Timeout and metadata fixes

### v1.0.0 (2026-04-24)
- Initial release with L2A, L2B, and L4A support

---

## Contributing & Feedback

This plugin is in active development. Bug reports, feature requests, and pull requests are very welcome!

- 🐛 **Report a bug** → [Open an Issue](https://github.com/AlexanderCotrinaS/gedi-metrics-qgis/issues)
- 💡 **Suggest a feature** → [Open an Issue](https://github.com/AlexanderCotrinaS/gedi-metrics-qgis/issues)
- 🔧 **Contribute code** → Fork the repo and open a Pull Request

---

## Credits

Developed by **Alexander Cotrina-Sanchez**.

This plugin builds on the [GEDI-Pipeline](https://github.com/leonelluiscorado/GEDI-Pipeline) framework by Leonel Corado & Godinho S.

---

## License

This project is licensed under the **GNU General Public License v2.0**.
See the [LICENSE](gedi_metrics/LICENSE) file for details.

---

## Citation

If you use GEDIMetrics in your research, please cite:

```
Cotrina-Sanchez, A. et al (2026). GEDIMetrics: a QGIS plugin for accessing and integrating multi-product GEDI spaceborne LiDAR data.
eartharxiv preprint: https://doi.org/10.31223/X5M48D
```
