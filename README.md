# GEDIMetrics — QGIS Plugin

> Download, subset, and merge NASA GEDI LiDAR products directly inside QGIS.

![QGIS](https://img.shields.io/badge/QGIS-3.4%20%7C%204.0-green?logo=qgis)
![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![License](https://img.shields.io/badge/License-GPL--2.0-orange)
![Status](https://img.shields.io/badge/Status-Experimental-yellow)

---

## What is GEDIMetrics?

**GEDIMetrics** is a QGIS plugin that provides a graphical interface to search, download, subset, and merge NASA GEDI spaceborne LiDAR data — without leaving QGIS and without writing a single line of code.

It integrates four GEDI products in a single workflow:

| Product | Description |
|---------|-------------|
| **L2A** | Canopy height (RH metrics: rh10, rh25, rh50, rh75, rh98, rh100…) |
| **L2B** | Vertical structure (canopy cover, PAI, PAVD, FHD, Pgap) |
| **L4A** | Aboveground biomass density (AGBD) |
| **L4C** | Waveform Structural Complexity Index (WSCI, prediction intervals, horizontal/vertical components) |

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

## Requirements

- QGIS 3.4+ or 4.0+ installed via **OSGeo4W** (recommended for Windows)
- NASA EarthData account → [Register here](https://urs.earthdata.nasa.gov/)
- Python dependencies: `h5py` `pandas` `geopandas` `shapely` `fiona` `rtree` `numpy` `requests`

---

## Installing dependencies

### Windows — OSGeo4W (recommended)

The easiest and most reliable method on Windows is to install QGIS through
**OSGeo4W** and select the Python packages during setup:

1. Download **OSGeo4W installer** from [https://trac.osgeo.org/osgeo4w/](https://trac.osgeo.org/osgeo4w/)
2. Run it → choose **Advanced Install**
3. On the **Select Packages** screen, search and mark for install:
   - `python3-h5py`
   - `python3-pandas`
   - `python3-geopandas`
   - `python3-shapely`
   - `python3-requests`
   - `python3-fiona`
4. Complete the installation — QGIS and all dependencies will be ready

> **Alternative:** From the OSGeo4W Shell (if QGIS is already installed):
> ```cmd
> python -m pip install --user pandas geopandas shapely fiona requests
> ```
> For `h5py`, prefer the OSGeo4W package `python3-h5py` to avoid HDF5 version conflicts.

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

## Installation

1. Download the latest ZIP from the [Releases](https://github.com/AlexanderCotrinaS/gedi-metrics-qgis/releases) page
2. Open QGIS → **Plugins** → **Manage and Install Plugins…**
3. Click **Install from ZIP** → select the downloaded file
4. Enable the plugin and restart QGIS

The plugin is also available from the **QGIS Plugin Repository** (search "GEDIMetrics" — enable *Show experimental plugins* in Settings).

---

## Usage

1. Open the plugin from the **Plugins** menu or the toolbar button
2. Select your **output folder**
3. Choose **products** (L2A, L2B, L4A, L4C) and **version**
4. Set the **date range** and optionally filter by recurring months
5. Select a **polygon layer** as your area of interest (ROI)
6. Enter your **NASA EarthData credentials** (Bearer Token)
7. Choose **beams**, **variables**, and **quality filters**
8. Select **output formats** (GeoPackage / GeoParquet)
9. Click **Run** — progress appears in the log panel
10. Results load automatically into QGIS when done

---

## Changelog

### v1.0.2 (2026-05-07)
- **New product:** GEDI L4C (Waveform Structural Complexity Index — WSCI) supported for download, subsetting, and multi-product merge
- **Authentication fix:** Bearer Token mode resolves connectivity issues with the previous cookie-based login
- **Quality filter defaults:** quality flags default to 1, exclude degraded footprints enabled, surface filter defaults to Land
- **Improved CMR search:** ORNL DAAC products (L4A, L4C) use multi-cascade search for reliable granule discovery

### v1.0.1 (2026-04-26)
- Timeout and metadata fixes
- Email added to metadata

### v1.0.0 (2026-04-24)
- Initial release with L2A, L2B, and L4A support

---

## Roadmap

- [ ] Add support for GEDI L1B waveforms
- [ ] Parallel granule downloads
- [ ] Integration with L4B gridded biomass
- [ ] Advanced visualization tools in QGIS
- [ ] Multi-language support (ES / EN)
- [ ] Batch processing for multiple AOIs

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
Cotrina-Sanchez, A. (2026). GEDIMetrics: A QGIS Plugin for Multi-Product
GEDI LiDAR Data Acquisition, Subsetting, and Integration (v1.0.2).
Available at: https://github.com/AlexanderCotrinaS/gedi-metrics-qgis
```
