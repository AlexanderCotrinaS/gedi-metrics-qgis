# GEDIMetrics — QGIS Plugin

> Download, subset, and merge NASA GEDI LiDAR products directly inside QGIS.

![QGIS](https://img.shields.io/badge/QGIS-3.x%20%7C%204.x-green?logo=qgis)
![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![License](https://img.shields.io/badge/License-GPL--2.0-orange)
![Status](https://img.shields.io/badge/Status-Experimental-yellow)

---

## What is GEDIMetrics?

**GEDIMetrics** is a QGIS plugin that provides a graphical interface to search, download, subset, and merge NASA GEDI spaceborne LiDAR data — without leaving QGIS and without writing a single line of code.

It integrates three GEDI products in a single workflow:

| Product | Description |
|---------|-------------|
| **L2A** | Canopy height (RH metrics: rh10, rh25, rh50, rh75, rh98, rh100…) |
| **L2B** | Canopy cover, PAI, PAVD, FHD, Pgap |
| **L4A** | Aboveground biomass density (AGBD) |

Results are merged by `shot_number` and exported as **GeoPackage** (`.gpkg`) and/or **GeoParquet** (`.parquet`), ready to use in QGIS.

---

## Key Features

- 🌍 **Area of Interest** — use any polygon vector layer loaded in QGIS (or load a file directly).
- 📅 **Date range & recurring months** — filter data by date and optionally by specific months.
- 🔦 **Beam selection** — choose power beams, coverage beams, or all beams.
- ✅ **Variable selection** — pick exactly which GEDI variables you need (L2A, L2B, L4A).
- 🔧 **Quality filters** — apply quality flags before exporting.
- 💾 **Output formats** — GeoPackage and/or GeoParquet.
- 🔐 **NASA EarthData credentials** — enter once with optional persistent login.
- 📋 **Live log panel** — follow the pipeline progress inside the QGIS dialog.
- ❌ **Cancel button** — stop a running job at any time.
- 🔄 **Auto-load results** — output layers are loaded automatically into the QGIS canvas.

---

## Requirements

- QGIS 3.x or 4.x
- NASA EarthData account → [Register here](https://urs.earthdata.nasa.gov/)
- Python dependencies (available to QGIS Python environment):
  - `h5py` `pandas` `geopandas` `shapely` `fiona` `rtree` `numpy` `requests`

### Installing dependencies

**Windows (OSGeo4W Shell):**
```cmd
python -m pip install h5py pandas geopandas shapely fiona rtree requests
```
> If you get HDF5 mismatch errors with `h5py`, use OSGeo4W Setup (Advanced) to install `python3-h5py` instead.

**Linux (Debian/Ubuntu):**
```bash
sudo apt install python3-h5py python3-pandas python3-geopandas \
                 python3-shapely python3-rtree python3-requests
```

**macOS:**
```bash
pip install h5py pandas geopandas shapely fiona rtree requests
```

---

## Installation

1. Download the latest ZIP from the [Releases](https://github.com/AlexanderCotrinaS/gedi-metrics-qgis/releases) page.
2. Open QGIS → **Plugins** → **Manage and Install Plugins…**
3. Click **Install from ZIP** → select the downloaded file.
4. Enable the plugin and restart QGIS.

---

## Usage

1. Open the plugin from the **Plugins** menu or the toolbar button.
2. Select your **output folder**.
3. Choose **products** (L2A, L2B, L4A) and **version**.
4. Set the **date range** and optionally filter by recurring months.
5. Select a **polygon layer** as your area of interest (ROI).
6. Enter your **NASA EarthData credentials** (optionally save them).
7. Choose **beams**, **variables**, and **quality filters**.
8. Select **output formats** (GeoPackage / GeoParquet).
9. Click **Run** — progress appears in the log panel.
10. Results load automatically into QGIS when done.

---

## Screenshots

> *(Coming soon — contributions welcome!)*

---

## Roadmap / Planned improvements

- [ ] Add support for GEDI L1B waveforms
- [ ] Improve dependency auto-installer
- [ ] Add progress bar per granule
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
See the [LICENSE](LICENSE) file for details.

---

## Citation

If you use GEDIMetrics in your research, please cite:

```
Cotrina-Sanchez, A. (2026). GEDIMetrics — QGIS Plugin v1.0.0.
Available at: https://github.com/AlexanderCotrinaS/gedi-metrics-qgis
```
