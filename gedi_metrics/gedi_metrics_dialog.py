import os
import sys
import threading
import platform
from pathlib import Path
from .compat import sip, CHECKED, VECTOR_LAYER_TYPE, is_deleted

from qgis.PyQt import uic
from qgis.PyQt import QtCore, QtWidgets
from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsMapLayer,
    QgsProject,
    QgsVectorLayer,
    QgsWkbTypes,
)

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'gedi_metrics_dialog_base.ui'))


# ─────────────────────────────────────────────────────────────
# Catálogo de variables por producto
# (widget_name, path_hdf5, rh_index_o_None)
# ─────────────────────────────────────────────────────────────
L2A_VARS = {
    'chk_l2a_rh10':               ('/rh', 10),
    'chk_l2a_rh20':               ('/rh', 20),
    'chk_l2a_rh25':               ('/rh', 25),
    'chk_l2a_rh30':               ('/rh', 30),
    'chk_l2a_rh40':               ('/rh', 40),
    'chk_l2a_rh50':               ('/rh', 50),
    'chk_l2a_rh60':               ('/rh', 60),
    'chk_l2a_rh70':               ('/rh', 70),
    'chk_l2a_rh75':               ('/rh', 75),
    'chk_l2a_rh80':               ('/rh', 80),
    'chk_l2a_rh95':               ('/rh', 95),
    'chk_l2a_rh98':               ('/rh', 98),
    'chk_l2a_rh100':              ('/rh', 100),
    'chk_l2a_elev_lowestmode':    ('/elev_lowestmode', None),
    'chk_l2a_elev_highestreturn': ('/elev_highestreturn', None),
    'chk_l2a_sensitivity':        ('/sensitivity', None),
    'chk_l2a_solar_elevation':    ('/solar_elevation', None),
    'chk_l2a_dem':                ('/digital_elevation_model', None),
    'chk_l2a_num_detectedmodes':  ('/num_detectedmodes', None),
    'chk_l2a_selected_algorithm': ('/selected_algorithm', None),
}

L2B_VARS = {
    'chk_l2b_cover':              ('/cover', None),
    'chk_l2b_cover_z':            ('/cover_z', None),
    'chk_l2b_pai':                ('/pai', None),
    'chk_l2b_pai_z':              ('/pai_z', None),
    'chk_l2b_fhd_normal':         ('/fhd_normal', None),
    'chk_l2b_rh100':              ('/rh100', None),
    'chk_l2b_pavd_z':             ('/pavd_z', None),
    'chk_l2b_pgap_theta':         ('/pgap_theta', None),
    'chk_l2b_rhov':               ('/rhov', None),
    'chk_l2b_rhog':               ('/rhog', None),
    'chk_l2b_sensitivity':        ('/sensitivity', None),
    'chk_l2b_solar_elevation':    ('/geolocation/solar_elevation', None),
}

L4A_VARS = {
    'chk_l4a_agbd':               ('/agbd', None),
    'chk_l4a_agbd_se':            ('/agbd_se', None),
    'chk_l4a_agbd_t':             ('/agbd_t', None),
    'chk_l4a_agbd_t_se':          ('/agbd_t_se', None),
    'chk_l4a_sensitivity':        ('/sensitivity', None),
    'chk_l4a_solar_elevation':    ('/solar_elevation', None),
    'chk_l4a_elev_lowestmode':    ('/elev_lowestmode', None),
    'chk_l4a_num_detectedmodes':  ('/num_detectedmodes', None),
    'chk_l4a_selected_algorithm': ('/selected_algorithm', None),
}



# ─────────────────────────────────────────────────────────────
# StreamToSignal
# ─────────────────────────────────────────────────────────────
class StreamToSignal:
    def __init__(self, signal):
        self.signal = signal
    def write(self, text):
        text = text.strip()
        if text:
            self.signal.emit(text)
    def flush(self):
        pass


# ─────────────────────────────────────────────────────────────
# PipelineWorker
# ─────────────────────────────────────────────────────────────
class PipelineWorker(QtCore.QObject):
    log      = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(bool, list, str)

    def __init__(self, params, plugin_dir, cancel_event):
        super().__init__()
        self.params       = params
        self.plugin_dir   = plugin_dir
        self.cancel_event = cancel_event

    @QtCore.pyqtSlot()
    def run(self):
        stdout_orig, stderr_orig = sys.stdout, sys.stderr
        sys.stdout = StreamToSignal(self.log)
        sys.stderr = StreamToSignal(self.log)
        try:
            roi      = self._compute_roi()
            self._prepare_netrc()
            pipeline = self._build_pipeline(roi)
            self.log.emit("[GEDIMetrics] Starting pipeline...")
            pipeline.run_pipeline()
            outputs = self._collect_outputs()
            self.finished.emit(True, outputs, "")
        except Exception as e:
            self.finished.emit(False, [], str(e))
        finally:
            sys.stdout = stdout_orig
            sys.stderr = stderr_orig

    def _build_pipeline(self, roi):
        candidates = [
            os.path.join(self.plugin_dir, "GEDI-Pipeline"),
            os.path.join(self.plugin_dir, "pipeline"),
        ]
        framework_path = next(
            (c for c in candidates if os.path.isdir(c)), None)
        if not framework_path:
            raise RuntimeError(
                "Could not locate GEDI framework folder.")
        if framework_path not in sys.path:
            sys.path.insert(0, framework_path)

        from .pipeline.pipeline.pipeline import GEDIPipeline
        return GEDIPipeline(
            out_directory    = self.params["output_dir"],
            products         = self.params["products"],
            version          = self.params["version"],
            date_start       = self.params["start_date"],
            date_end         = self.params["end_date"],
            recurring_months = self.params["recurring_months"],
            roi              = roi,
            beams            = self.params["beams"],  # None = all beams
            selected_vars    = self.params["selected_vars"],
            filters          = self.params["filters"],
            merge_how        = self.params["merge_how"],
            out_gpkg         = self.params["out_gpkg"],
            out_parquet      = self.params["out_parquet"],
            persist_login    = self.params["keep_login"],
            keep_original_file = self.params["keep_original"],
            cancel_event     = self.cancel_event,
            roi_path         = self.params.get("polygon_source") or None,
        )

    def _prepare_netrc(self):
        user = self.params["earthdata_user"]
        pwd  = self.params["earthdata_pass"]
        if not user or not pwd:
            return
        content = f"machine urs.earthdata.nasa.gov login {user} password {pwd}\n"
        netrc_files = [Path.home() / ".netrc"]
        if os.name == "nt":
            netrc_files.append(Path.home() / "_netrc")
        for p in netrc_files:
            try:
                p.write_text(content)
                try: p.chmod(0o600)
                except Exception: pass
                self.log.emit(f"[Auth] Credentials saved to {p}")
            except Exception as e:
                self.log.emit(f"[Auth] Could not write {p}: {e}")

    def _compute_roi(self):
        layer_id = self.params["polygon_layer_id"]
        layer    = QgsProject.instance().mapLayer(layer_id) if layer_id else None
        if not layer:
            raise RuntimeError("No polygon layer selected.")
        if self.params["selected_features_only"] and layer.selectedFeatureCount() > 0:
            extent = layer.boundingBoxOfSelected()
            if not extent.isFinite():
                raise RuntimeError("Selected features have no valid extent.")
        else:
            extent = layer.extent()
        if not extent.isFinite():
            raise RuntimeError("Layer extent is not valid.")
        crs_src  = layer.crs()
        crs_dest = QgsCoordinateReferenceSystem("EPSG:4326")
        if crs_src != crs_dest:
            transform = QgsCoordinateTransform(
                crs_src, crs_dest, QgsProject.instance())
            extent = transform.transformBoundingBox(extent)
        return [extent.yMaximum(), extent.xMinimum(),
                extent.yMinimum(), extent.xMaximum()]

    def _collect_outputs(self):
        out_dir = Path(self.params["output_dir"])
        if self.params["out_gpkg"]:
            # Priorizar el archivo final unificado
            finals = sorted(out_dir.glob("GEDIMetrics_*_final.gpkg"))
            if finals:
                return [str(finals[-1])]  # el más reciente
            # Si no hay final (solo 1 granule), tomar los merged individuales
            return [str(p) for p in sorted(out_dir.glob("*_merged.gpkg"))
                    if '_final' not in p.name]
        return []


# ─────────────────────────────────────────────────────────────
# GEDIMetricsDialog
# ─────────────────────────────────────────────────────────────
class GEDIMetricsDialog(QtWidgets.QDialog, FORM_CLASS):

    def __init__(self, parent=None, plugin_dir=None):
        super().__init__(parent)
        self.setupUi(self)
        self.plugin_dir    = plugin_dir or os.path.dirname(__file__)
        self._worker_thread = None
        self._cancel_event  = threading.Event()

        self._init_polygon_menu()
        self.populate_polygon_layers()
        self._connect_signals()
        self._update_product_tabs()

        # Establecer fecha de fin como hoy por defecto
        from qgis.PyQt.QtCore import QDate
        self.end_date_edit.setDate(QDate.currentDate())

    # ── Señales ───────────────────────────────────────────────
    def _connect_signals(self):
        self.run_pipeline.clicked.connect(self.on_run_clicked)
        self.close_button.clicked.connect(self.on_cancel_close)
        self.browse_output_btn.clicked.connect(self.choose_output_dir)
        self.polygon_layer_combo.currentIndexChanged.connect(
            self.on_polygon_layer_changed)

        # Productos → actualizar pestañas Variables
        self.check_l2a.stateChanged.connect(self._update_product_tabs)
        self.check_l2b.stateChanged.connect(self._update_product_tabs)
        self.check_l4a.stateChanged.connect(self._update_product_tabs)

        # Select all / Clear
        self.btn_l2a_all.clicked.connect(lambda: self._set_all_vars('l2a', True))
        self.btn_l2a_none.clicked.connect(lambda: self._set_all_vars('l2a', False))
        self.btn_l2b_all.clicked.connect(lambda: self._set_all_vars('l2b', True))
        self.btn_l2b_none.clicked.connect(lambda: self._set_all_vars('l2b', False))
        self.btn_l4a_all.clicked.connect(lambda: self._set_all_vars('l4a', True))
        self.btn_l4a_none.clicked.connect(lambda: self._set_all_vars('l4a', False))

        # Surface type — All es exclusivo con Land/Water
        self.chk_surf_all.stateChanged.connect(self._on_surface_all_changed)
        self.chk_surf_land.stateChanged.connect(self._on_surface_specific_changed)
        self.chk_surf_water.stateChanged.connect(self._on_surface_specific_changed)

    def _update_product_tabs(self):
        self.tabWidgetVars.setTabEnabled(0, self.check_l2a.isChecked())
        self.tabWidgetVars.setTabEnabled(1, self.check_l2b.isChecked())
        self.tabWidgetVars.setTabEnabled(2, self.check_l4a.isChecked())

    def _set_all_vars(self, product, state):
        catalog = {'l2a': L2A_VARS, 'l2b': L2B_VARS, 'l4a': L4A_VARS}
        for widget_name in catalog[product]:
            chk = getattr(self, widget_name, None)
            if chk:
                chk.setChecked(state)

    def _on_full_power_changed(self, state):
        """Deshabilita el campo custom beams cuando full power está marcado."""
        # int(state) == 2 es Checked tanto en Qt5 como Qt6
        is_checked = self.chk_full_power_only.isChecked()
        self.beams_lineedit.setEnabled(not is_checked)
        if is_checked:
            self.beams_lineedit.setPlaceholderText(
                "Disabled — using full power beams only")
        else:
            self.beams_lineedit.setPlaceholderText(
                "e.g. BEAM0101,BEAM1000  (empty = all beams)")

    def _on_surface_all_changed(self, state):
        if state == CHECKED:
            for w in (self.chk_surf_land, self.chk_surf_water):
                w.blockSignals(True)
                w.setChecked(False)
                w.blockSignals(False)

    def _on_surface_specific_changed(self, state):
        if state == CHECKED:
            self.chk_surf_all.blockSignals(True)
            self.chk_surf_all.setChecked(False)
            self.chk_surf_all.blockSignals(False)

    # ── Polígono AOI ──────────────────────────────────────────
    def _init_polygon_menu(self):
        menu = QtWidgets.QMenu(self)
        menu.addAction("From computer").triggered.connect(self.on_polygon_from_file)
        menu.addAction("Browse layer").triggered.connect(self.on_polygon_browse_layer)
        menu.addAction("Refresh layers").triggered.connect(self.populate_polygon_layers)
        self.polygon_options_btn.setMenu(menu)

    def choose_output_dir(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(
            self, "Select output folder", os.path.expanduser("~"))
        if d:
            self.output_dir_lineedit.setText(d)

    def populate_polygon_layers(self):
        current_id = self.polygon_layer_combo.currentData()
        self.polygon_layer_combo.blockSignals(True)
        self.polygon_layer_combo.clear()
        layers = [
            l for l in QgsProject.instance().mapLayers().values()
            if l.type() == VECTOR_LAYER_TYPE
            and QgsWkbTypes.geometryType(l.wkbType()) == QgsWkbTypes.PolygonGeometry
        ]
        if not layers:
            self.polygon_layer_combo.addItem("No polygon layers found", None)
            self.polygon_layer_combo.setEnabled(False)
        else:
            self.polygon_layer_combo.setEnabled(True)
            for l in layers:
                self.polygon_layer_combo.addItem(l.name(), l.id())
            if current_id:
                idx = self.polygon_layer_combo.findData(current_id)
                if idx != -1:
                    self.polygon_layer_combo.setCurrentIndex(idx)
        self.polygon_layer_combo.blockSignals(False)
        self.on_polygon_layer_changed()

    def on_polygon_layer_changed(self):
        lid   = self.polygon_layer_combo.currentData()
        layer = QgsProject.instance().mapLayer(lid) if lid else None
        self.polygon_path_lineedit.setText(layer.source() if layer else "")

    def on_polygon_from_file(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select polygon file", os.path.expanduser("~"),
            "Vector files (*.shp *.gpkg);;All files (*)")
        if not path:
            return
        name  = os.path.splitext(os.path.basename(path))[0]
        layer = QgsVectorLayer(path, name, "ogr")
        if not layer.isValid():
            QtWidgets.QMessageBox.critical(
                self, "Invalid layer", "Could not load the selected file.")
            return
        QgsProject.instance().addMapLayer(layer)
        self.populate_polygon_layers()
        idx = self.polygon_layer_combo.findData(layer.id())
        if idx != -1:
            self.polygon_layer_combo.setCurrentIndex(idx)
        self.polygon_path_lineedit.setText(path)

    def on_polygon_browse_layer(self):
        self.populate_polygon_layers()
        self.polygon_layer_combo.showPopup()

    # ── Recolectar parámetros ─────────────────────────────────
    def collect_parameters(self):
        products = []
        if self.check_l2a.isChecked(): products.append('GEDI02_A')
        if self.check_l2b.isChecked(): products.append('GEDI02_B')
        if self.check_l4a.isChecked(): products.append('GEDI04_A')

        selected_vars = {
            'GEDI02_A': self._collect_vars(L2A_VARS),
            'GEDI02_B': self._collect_vars(L2B_VARS),
            'GEDI04_A': self._collect_vars(L4A_VARS),
        }

        surface_flags = []
        if not self.chk_surf_all.isChecked():
            if self.chk_surf_land.isChecked():  surface_flags.append(1)
            if self.chk_surf_water.isChecked(): surface_flags.append(2)

        filters = {
            'quality': {
                'GEDI02_A': self.spin_l2a_quality.value(),
                'GEDI02_B': self.spin_l2b_quality.value(),
                'GEDI04_A': self.spin_l4a_quality.value(),
            },
            'sensitivity': {
                'value':    self.spin_sensitivity.value(),
                'apply_to': {
                    'GEDI02_A': self.chk_sens_l2a.isChecked(),
                    'GEDI02_B': self.chk_sens_l2b.isChecked(),
                    'GEDI04_A': self.chk_sens_l4a.isChecked(),
                }
            },
            'exclude_degrade': self.chk_exclude_degrade.isChecked(),
            'surface_flags':   surface_flags,
        }

        # Beams — devolver lista o None (None = todos los beams)
        FULL_POWER_BEAMS = ['BEAM0101', 'BEAM0110', 'BEAM1000', 'BEAM1011']
        if self.chk_full_power_only.isChecked():
            beams = FULL_POWER_BEAMS          # lista explícita
        elif self.beams_lineedit.text().strip():
            beams = [b.strip() for b in
                     self.beams_lineedit.text().split(',')
                     if b.strip()]
        else:
            beams = None                      # None = todos los beams

        return {
            "output_dir":             self.output_dir_lineedit.text().strip(),
            "products":               products,
            "version":                self.version_combo.currentText(),
            "start_date":             self.start_date_edit.date().toString("yyyy.MM.dd"),
            "end_date":               self.end_date_edit.date().toString("yyyy.MM.dd"),
            "recurring_months":       self.recurring_months_check.isChecked(),
            "polygon_layer_id":       self.polygon_layer_combo.currentData(),
            "polygon_source":         self.polygon_path_lineedit.text().strip(),
            "selected_features_only": self.selected_only_check.isChecked(),
            "selected_vars":          selected_vars,
            "filters":                filters,
            "merge_how":    "outer" if self.radio_merge_outer.isChecked() else "inner",
            "out_gpkg":     self.chk_out_gpkg.isChecked(),
            "out_parquet":  self.chk_out_parquet.isChecked(),
            "beams":        beams,
            "keep_original": self.keep_original_check.isChecked(),
            "earthdata_user": self.earthdata_user_edit.text().strip(),
            "earthdata_pass": self.earthdata_pass_edit.text(),
            "keep_login":     self.keep_login_check.isChecked(),
        }

    def _collect_vars(self, catalog):
        """Devuelve lista de (hdf5_path, rh_idx) para variables marcadas."""
        return [
            (hdf5_path, rh_idx)
            for widget_name, (hdf5_path, rh_idx) in catalog.items()
            if (chk := getattr(self, widget_name, None)) and chk.isChecked()
        ]

    # ── Validación ────────────────────────────────────────────
    def _validate(self, params):
        if not params["output_dir"]:
            return "Please choose an output folder."
        if not params["products"]:
            return "Please select at least one GEDI product (L2A, L2B or L4A)."
        if not params["polygon_layer_id"] and not params["polygon_source"]:
            return "Please select a polygon layer or load a polygon file."
        if not params["out_gpkg"] and not params["out_parquet"]:
            return "Please select at least one output format."
        if not self._has_credentials(params):
            return ("Please enter your EarthData credentials "
                    "or ensure a .netrc file exists.")
        return None

    def _has_credentials(self, params):
        if params["earthdata_user"] and params["earthdata_pass"]:
            return True
        return any(
            (Path.home() / n).exists() and (Path.home() / n).stat().st_size > 0
            for n in [".netrc", "_netrc"])

    # ── Run ───────────────────────────────────────────────────
    def on_run_clicked(self):
        params = self.collect_parameters()
        err = self._validate(params)
        if err:
            QtWidgets.QMessageBox.warning(self, "GEDIMetrics", err)
            return
        if not self.check_dependencies():
            return
        if self._worker_thread and self._worker_thread.isRunning():
            QtWidgets.QMessageBox.information(
                self, "GEDIMetrics", "Pipeline is already running.")
            return

        self.log_text_edit.clear()
        self.progress_bar.setRange(0, 0)
        self._cancel_event.clear()
        self.close_button.setText("Stop")

        self.log_text_edit.append(
            f"[GEDIMetrics] Products : {', '.join(params['products'])}")
        self.log_text_edit.append(
            f"[GEDIMetrics] Merge    : {params['merge_how']} join")
        self.log_text_edit.append(
            f"[GEDIMetrics] Format   : "
            f"{'gpkg ' if params['out_gpkg'] else ''}"
            f"{'parquet' if params['out_parquet'] else ''}")
        beams_log = (', '.join(params['beams'])
                     if params['beams'] else 'all beams')
        self.log_text_edit.append(
            f"[GEDIMetrics] Beams    : {beams_log}")

        self._start_worker(params)

    def _start_worker(self, params):
        self._worker_thread = QtCore.QThread(self)
        self._worker = PipelineWorker(params, self.plugin_dir, self._cancel_event)
        self._worker.moveToThread(self._worker_thread)
        self._worker_thread.started.connect(self._worker.run)
        self._worker.log.connect(self.append_log)
        self._worker.finished.connect(self.on_worker_finished)
        self._worker.finished.connect(self._worker_thread.quit)
        self._worker.finished.connect(self._worker.deleteLater)
        self._worker_thread.finished.connect(self._worker_thread.deleteLater)
        self._worker_thread.start()

    @QtCore.pyqtSlot(str)
    def append_log(self, message):
        self.log_text_edit.append(message)

    @QtCore.pyqtSlot(bool, list, str)
    def on_worker_finished(self, success, outputs, error_message):
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1 if success else 0)
        if success:
            self.log_text_edit.append("[GEDIMetrics] Pipeline finished.")
            self._load_outputs(outputs)
        else:
            self.log_text_edit.append(f"[Error] {error_message}")
        self._worker_thread = None
        self.close_button.setText("Close")

    def _load_outputs(self, outputs):
        if not outputs:
            self.log_text_edit.append("[GEDIMetrics] No output files produced.")
            return
        added = 0
        for path in outputs:
            layer = QgsVectorLayer(path, os.path.basename(path), "ogr")
            if layer.isValid():
                QgsProject.instance().addMapLayer(layer)
                added += 1
                self.log_text_edit.append(f"[GEDIMetrics] Loaded: {path}")
            else:
                self.log_text_edit.append(f"[GEDIMetrics] Could not load: {path}")
        if added:
            self.log_text_edit.append(
                f"[GEDIMetrics] {added} layer(s) added to QGIS.")

    def on_cancel_close(self):
        try:
            if (self._worker_thread
                    and not is_deleted(self._worker_thread)
                    and self._worker_thread.isRunning()):
                self._cancel_event.set()
                self.log_text_edit.append(
                    "[GEDIMetrics] Cancellation requested...")
                self.close_button.setText("Stopping...")
                return
        except RuntimeError:
            pass
        self.reject()

    # ── Dependencias ──────────────────────────────────────────
    def check_dependencies(self):
        missing, hdf5_mismatch = [], None
        try:
            import h5py
            built   = getattr(h5py.version, "hdf5_built_version", None)
            runtime = getattr(h5py.version, "hdf5_version", None)
            if built and runtime and built.split(".")[:2] != runtime.split(".")[:2]:
                hdf5_mismatch = (built, runtime)
        except ImportError:
            missing.append("h5py")
        for mod in ["pandas", "geopandas", "numpy", "shapely", "requests"]:
            try: __import__(mod)
            except ImportError: missing.append(mod)

        if not missing and not hdf5_mismatch:
            return True

        lines = []
        if missing:
            lines.append("Missing packages:\n  " + ", ".join(sorted(set(missing))))
        if hdf5_mismatch:
            lines.append(f"h5py/HDF5 mismatch: built={hdf5_mismatch[0]}, "
                         f"runtime={hdf5_mismatch[1]}")
        os_name = platform.system()
        if os_name == "Windows":
            lines.append("OSGeo4W Shell:\n  python -m pip install --user " +
                         " ".join(sorted(set(missing))))
        elif os_name == "Darwin":
            lines.append("macOS:\n  python -m pip install --user " +
                         " ".join(sorted(set(missing))))
        else:
            lines.append("Linux:\n  sudo apt install python3-h5py "
                         "python3-pandas python3-geopandas python3-shapely")

        QtWidgets.QMessageBox.critical(
            self, "Missing dependencies", "\n\n".join(lines))
        return False
