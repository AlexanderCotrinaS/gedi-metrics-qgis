# GEDI-Pipeline QGIS Plugin
A QGIS plugin to find, download, subset, and clip NASA GEDI data directly to your polygon ROI.
The main **GEDI-Pipeline** project can be accessed here:  
https://github.com/leonelluiscorado/GEDI-Pipeline  

That repository provides a command-line interface (CLI) to interact with the pipeline.  
This QGIS plugin was developed to offer a fast, user-friendly, cross-platform interface
and a simpler installation workflow for GEDI-Pipeline.

## What it does
- Searches GEDI granules for your product and date range.
- Downloads from EarthData (with optional credential caching).
- Subsets and clips to your polygon ROI.
- Loads the resulting GeoPackage layers into QGIS automatically.

## Requirements
- QGIS 3.x
- EarthData login (https://urs.earthdata.nasa.gov/)
- Python deps available to the QGIS Python: `h5py`, `pandas`, `geopandas`, `shapely`, `fiona`, `rtree`, `numpy`, `requests`.

### Windows (OSGeo4W/Standalone QGIS)
- Recommended: use OSGeo4W Setup (Advanced) → on the "Select Packages" menu, search and add `python3-h5py`, `python3-pandas`, `python3-geopandas`, `python3-shapely`, `python3-requests`

- Or from the OSGeo4W Shell:
  ```cmd
  python -m pip install --user h5py
  ```
  If you see HDF5 mismatch errors with `h5py`, uninstall inside the OSGeo4W shell with `python -m pip uninstall h5py` and install the OSGeo4W package `python3-h5py` instead.

### Linux (Debian/Ubuntu)
```bash
sudo apt install python3-h5py python3-pandas python3-geopandas python3-shapely python3-rtree
```

## Installation
1. Download the plugin ZIP (https://github.com/leonelluiscorado/gedi-pipeline-qgis-plugin/releases/tag/v0.1.0). 
2. In QGIS: Plugins → Manage and Install Plugins… → Install from ZIP… → select the ZIP.
3. Restart QGIS.

(This plugin is waiting for approval on the QGIS Plugin Database)

## Usage
1. Open the plugin (Plugins menu or toolbar button).
2. Choose output folder, product/version, date range, and select a polygon layer (or load a polygon file). Optional: use selected features only.
3. Enter your EarthData username/password; optionally “Keep login” to persist.
4. Set optional beams/SDS/flags and run. Progress and logs appear in the dialog; resulting `.gpkg` files load into QGIS automatically.

## Notes
- If dependencies are missing, the plugin shows a guidance dialog; install the packages for your OS as above and restart QGIS.
- The selected Polygon ROI is used both for the search bounding box (derived from its extent) and final clip (exact polygon).

## Citing this Project

This project is currently in development with future improvements, bug fixes and new features.

To cite this plugin: Corado, L., Godinho, S., 2026. GEDI-Pipeline QGIS Plugin. Version 0.1.0, accessed on 08-01-2026, available at: https://github.com/leonelluiscorado/gedi-pipeline-qgis-plugin
