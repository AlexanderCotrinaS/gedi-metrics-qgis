import os
import sys
import threading
import platform
from pathlib import Path
from .compat import CHECKED, VECTOR_LAYER_TYPE, is_deleted

from qgis.PyQt import uic
from qgis.PyQt import QtCore, QtWidgets
from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
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
    'chk_l2a_rh10': ('/rh', 10),
    'chk_l2a_rh20': ('/rh', 20),
    'chk_l2a_rh25': ('/rh', 25),
    'chk_l2a_rh30': ('/rh', 30),
    'chk_l2a_rh40': ('/rh', 40),
    'chk_l2a_rh50': ('/rh', 50),
    'chk_l2a_rh60': ('/rh', 60),
    'chk_l2a_rh70': ('/rh', 70),
    'chk_l2a_rh75': ('/rh', 75),
    'chk_l2a_rh80': ('/rh', 80),
    'chk_l2a_rh90': ('/rh', 90),
    'chk_l2a_rh95': ('/rh', 95),
    'chk_l2a_rh98': ('/rh', 98),
    'chk_l2a_rh100': ('/rh', 100),
    'chk_l2a_elev_lowestmode': ('/elev_lowestmode', None),
    'chk_l2a_elev_highestreturn': ('/elev_highestreturn', None),
    'chk_l2a_sensitivity': ('/sensitivity', None),
    'chk_l2a_solar_elevation': ('/solar_elevation', None),
    'chk_l2a_dem': ('/digital_elevation_model', None),
    'chk_l2a_num_detectedmodes': ('/num_detectedmodes', None),
    'chk_l2a_selected_algorithm': ('/selected_algorithm', None),
    'chk_l2a_phenology_phase': ('/land_cover_data/phenology_phase', None),
    'chk_l2a_phenology_year': ('/land_cover_data/phenology_year', None),
    'chk_l2a_worldcover_class': ('/land_cover_data/worldcover_class', None),
}

L2B_VARS = {
    'chk_l2b_cover': ('/cover', None),
    'chk_l2b_cover_z': ('/cover_z', None),
    'chk_l2b_pai': ('/pai', None),
    'chk_l2b_pai_z': ('/pai_z', None),
    'chk_l2b_fhd_normal': ('/fhd_normal', None),
    'chk_l2b_rh100': ('/rh100', None),
    'chk_l2b_pavd_z': ('/pavd_z', None),
    'chk_l2b_pgap_theta': ('/pgap_theta', None),
    'chk_l2b_rhov': ('/rhov', None),
    'chk_l2b_rhog': ('/rhog', None),
    'chk_l2b_sensitivity': ('/sensitivity', None),
    'chk_l2b_solar_elevation': ('/geolocation/solar_elevation', None),
    'chk_l2b_rch25': ('/rch', 25),
    'chk_l2b_rch50': ('/rch', 50),
    'chk_l2b_rch75': ('/rch', 75),
    'chk_l2b_rch98': ('/rch', 98),
    'chk_l2b_rch100': ('/rch', 100),
    'chk_l2b_phenology_phase': ('/land_cover_data/phenology_phase', None),
    'chk_l2b_phenology_year': ('/land_cover_data/phenology_year', None),
    'chk_l2b_worldcover_class': ('/land_cover_data/worldcover_class', None),
}

L4A_VARS = {
    'chk_l4a_agbd': ('/agbd', None),
    'chk_l4a_agbd_se': ('/agbd_se', None),
    'chk_l4a_agbd_t': ('/agbd_t', None),
    'chk_l4a_agbd_t_se': ('/agbd_t_se', None),
    'chk_l4a_sensitivity': ('/sensitivity', None),
    'chk_l4a_solar_elevation': ('/solar_elevation', None),
    'chk_l4a_elev_lowestmode': ('/elev_lowestmode', None),
    'chk_l4a_num_detectedmodes': ('/num_detectedmodes', None),
    'chk_l4a_selected_algorithm': ('/selected_algorithm', None),
}

# L4C — Waveform Structural Complexity Index (WSCI).
# Set mínimo de variables: la métrica WSCI con sus intervalos de predicción al
# 95%, las componentes horizontal/vertical, y la flag de calidad estricta del
# producto. wsci_quality_flag se filtra automáticamente como columna de calidad
# principal (BASE_FIELDS), pero se mantiene aquí también por si el usuario quiere
# verla en la tabla aunque el filtro esté en 0.
L4C_VARS = {
    'chk_l4c_wsci': ('/wsci', None),
    'chk_l4c_wsci_pi_lower': ('/wsci_pi_lower', None),
    'chk_l4c_wsci_pi_upper': ('/wsci_pi_upper', None),
    'chk_l4c_wsci_xy': ('/wsci_xy', None),
    'chk_l4c_wsci_z': ('/wsci_z', None),
    'chk_l4c_wsci_quality_flag': ('/wsci_quality_flag', None),
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
    log = QtCore.pyqtSignal(str)
    finished = QtCore.pyqtSignal(bool, list, str)

    def __init__(self, params, plugin_dir, cancel_event):
        super().__init__()
        self.params = params
        self.plugin_dir = plugin_dir
        self.cancel_event = cancel_event

    @QtCore.pyqtSlot()
    def run(self):
        stdout_orig, stderr_orig = sys.stdout, sys.stderr
        sys.stdout = StreamToSignal(self.log)
        sys.stderr = StreamToSignal(self.log)
        try:
            roi = self._compute_roi()
            self._prepare_netrc()
            proxy_desc = self._describe_proxy()
            self.log.emit(f"[GEDIMetrics] Network  : {proxy_desc}")
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
            out_directory=self.params["output_dir"],
            products=self.params["products"],
            version=self.params["version"],
            date_start=self.params["start_date"],
            date_end=self.params["end_date"],
            recurring_months=self.params["recurring_months"],
            roi=roi,
            beams=self.params["beams"],
            selected_vars=self.params["selected_vars"],
            filters=self.params["filters"],
            merge_how=self.params["merge_how"],
            out_gpkg=self.params["out_gpkg"],
            out_parquet=self.params["out_parquet"],
            persist_login=self.params["keep_login"],
            keep_original_file=self.params["keep_original"],
            cancel_event=self.cancel_event,
            roi_path=self.params.get("polygon_source") or None,
            bearer_token=self.params.get("earthdata_token") or None,
            proxy_url=self.params.get("proxy_url") or None,
            proxy_user=self.params.get("proxy_user") or None,
            proxy_pass=self.params.get("proxy_pass") or None,
            proxy_auto=self.params.get("proxy_auto", True),
        )

    def _describe_proxy(self):
        """Human-readable proxy status for the log."""
        if self.params.get("proxy_manual") and self.params.get("proxy_url"):
            return f"manual proxy → {self.params['proxy_url']}"
        if self.params.get("proxy_auto"):
            import urllib.request
            detected = urllib.request.getproxies()
            if detected:
                return f"auto proxy → {list(detected.values())[0]}"
            return "auto-detect (no proxy found — direct connection)"
        return "direct (no proxy)"

    def _prepare_netrc(self):
        user = self.params["earthdata_user"]
        pwd = self.params["earthdata_pass"]
        if not user or not pwd:
            return
        content = f"machine urs.earthdata.nasa.gov login {user} password {pwd}\n"
        netrc_files = [Path.home() / ".netrc"]
        if os.name == "nt":
            netrc_files.append(Path.home() / "_netrc")
        for p in netrc_files:
            try:
                p.write_text(content)
                try:
                    p.chmod(0o600)
                except Exception:
                    pass
                self.log.emit(f"[Auth] Credentials saved to {p}")
            except Exception as e:
                self.log.emit(f"[Auth] Could not write {p}: {e}")

    def _compute_roi(self):
        layer_id = self.params["polygon_layer_id"]
        layer = QgsProject.instance().mapLayer(layer_id) if layer_id else None
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
        crs_src = layer.crs()
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
        self.plugin_dir = plugin_dir or os.path.dirname(__file__)
        self._worker_thread = None
        self._cancel_event = threading.Event()

        self._init_polygon_menu()
        self.populate_polygon_layers()
        self._connect_signals()
        self._update_product_tabs()
        self._update_version_lock()   # sync version combo with initial product state

        # Establecer fecha de fin como hoy por defecto
        from qgis.PyQt.QtCore import QDate
        self.end_date_edit.setDate(QDate.currentDate())

        # Restore saved settings
        self._load_settings()
        self._update_version_lock()   # re-sync after settings restore
        # Auto-detect network → update status label
        self._detect_and_update_status()

    def _load_settings(self):
        from qgis.PyQt.QtCore import QSettings
        s = QSettings()
        s.beginGroup("GEDIMetrics")
        token = s.value("earthdata_token", "")
        if token:
            self.earthdata_token_edit.setText(token)
        user = s.value("earthdata_user", "")
        if user:
            self.earthdata_user_edit.setText(user)
        out_dir = s.value("output_dir", "")
        if out_dir:
            self.output_dir_lineedit.setText(out_dir)
        proxy_url = s.value("proxy_url", "")
        if proxy_url:
            self.proxy_url_edit.setText(proxy_url)
        s.endGroup()

    def _save_settings(self, params):
        from qgis.PyQt.QtCore import QSettings
        s = QSettings()
        s.beginGroup("GEDIMetrics")
        if params.get("earthdata_token"):
            s.setValue("earthdata_token", params["earthdata_token"])
        if params.get("earthdata_user"):
            s.setValue("earthdata_user", params["earthdata_user"])
        if params.get("output_dir"):
            s.setValue("output_dir", params["output_dir"])
        if params.get("proxy_url"):
            s.setValue("proxy_url", params["proxy_url"])
        s.endGroup()

    def _detect_and_update_status(self):
        """Silent TCP probe — updates lbl_auth_status with one line."""
        import socket  # noqa: F401
        from qgis.PyQt.QtCore import QTimer
        QTimer.singleShot(300, self._run_probe)

    def _run_probe(self):
        import socket  # noqa: F401
        token = self.earthdata_token_edit.text().strip()
        if token:
            self.lbl_auth_status.setText(
                "✓  Bearer Token detected — ready. "
                "Token auth works on university networks.")
            return
        try:
            sock = socket.create_connection(
                ("urs.earthdata.nasa.gov", 443), timeout=6)
            sock.close()
            self.lbl_auth_status.setText(
                "✓  Direct connection — use username and password.")
        except (socket.timeout, OSError):
            self.lbl_auth_status.setText(
                "⚠  University network detected — paste a Bearer Token above. "
                "Get one free at: urs.earthdata.nasa.gov/user_tokens")

    # ── Señales ───────────────────────────────────────────────
    def _connect_signals(self):
        self.run_pipeline.clicked.connect(self.on_run_clicked)
        self.close_button.clicked.connect(self.on_cancel_close)
        self.browse_output_btn.clicked.connect(self.choose_output_dir)
        # Proxy controls
        self.chk_proxy_manual.stateChanged.connect(self._on_proxy_manual_toggled)
        self.btn_test_connection.clicked.connect(self._on_test_connection)
        self.polygon_layer_combo.currentIndexChanged.connect(
            self.on_polygon_layer_changed)

        # Productos → actualizar pestañas Variables + version lock
        self.check_l2a.stateChanged.connect(self._update_product_tabs)
        self.check_l2b.stateChanged.connect(self._update_product_tabs)
        self.check_l4a.stateChanged.connect(self._update_product_tabs)
        self.check_l4c.stateChanged.connect(self._update_product_tabs)
        self.check_l2a.stateChanged.connect(self._update_version_lock)
        self.check_l2b.stateChanged.connect(self._update_version_lock)
        self.check_l4a.stateChanged.connect(self._update_version_lock)
        self.check_l4c.stateChanged.connect(self._update_version_lock)

        # Select all / Clear
        self.btn_l2a_all.clicked.connect(lambda: self._set_all_vars('l2a', True))
        self.btn_l2a_none.clicked.connect(lambda: self._set_all_vars('l2a', False))
        self.btn_l2b_all.clicked.connect(lambda: self._set_all_vars('l2b', True))
        self.btn_l2b_none.clicked.connect(lambda: self._set_all_vars('l2b', False))
        self.btn_l4a_all.clicked.connect(lambda: self._set_all_vars('l4a', True))
        self.btn_l4a_none.clicked.connect(lambda: self._set_all_vars('l4a', False))
        self.btn_l4c_all.clicked.connect(lambda: self._set_all_vars('l4c', True))
        self.btn_l4c_none.clicked.connect(lambda: self._set_all_vars('l4c', False))

        # Surface type — All es exclusivo con Land/Water
        self.chk_surf_all.stateChanged.connect(self._on_surface_all_changed)
        self.chk_surf_land.stateChanged.connect(self._on_surface_specific_changed)
        self.chk_surf_water.stateChanged.connect(self._on_surface_specific_changed)

        # Defaults orientados a calidad: quality_flag>=1, exclude degraded
        # footprints, y restringir a superficie terrestre (Land=1). El usuario
        # puede relajar estos filtros si necesita máxima cobertura.
        # Version combo: populate V003 and V002, default to V003
        self.version_combo.clear()
        self.version_combo.addItem('003')
        self.version_combo.addItem('002')
        self.version_combo.setCurrentText('003')

        self.spin_l2a_quality.setValue(1)
        self.spin_l2b_quality.setValue(1)
        self.spin_l4a_quality.setValue(1)
        self.spin_l4c_quality.setValue(1)
        self.chk_exclude_degrade.setChecked(True)
        self.chk_surf_all.setChecked(False)
        self.chk_surf_water.setChecked(False)
        self.chk_surf_land.setChecked(True)

    def _update_product_tabs(self):
        self.tabWidgetVars.setTabEnabled(0, self.check_l2a.isChecked())
        self.tabWidgetVars.setTabEnabled(1, self.check_l2b.isChecked())
        self.tabWidgetVars.setTabEnabled(2, self.check_l4a.isChecked())
        self.tabWidgetVars.setTabEnabled(3, self.check_l4c.isChecked())

    def _update_version_lock(self):
        """Inform the user when products are routed to V002 regardless of the
        version selector. The version combo stays enabled; the pipeline resolves
        the effective version per product.
        """
        if self.check_l4a.isChecked() or self.check_l4c.isChecked():
            self.label_version.setToolTip(
                'L4A and L4C use V002 for now. '
                'L2A and L2B use the selected version.')
            self.version_combo.setToolTip(
                'L4A/L4C will use V002 regardless of this setting.')
        else:
            self.label_version.setToolTip('')
            self.version_combo.setToolTip('')

    def _set_all_vars(self, product, state):
        catalog = {'l2a': L2A_VARS, 'l2b': L2B_VARS,
                   'l4a': L4A_VARS, 'l4c': L4C_VARS}
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
            lyr for lyr in QgsProject.instance().mapLayers().values()
            if lyr.type() == VECTOR_LAYER_TYPE
            and QgsWkbTypes.geometryType(lyr.wkbType()) == QgsWkbTypes.PolygonGeometry
        ]
        if not layers:
            self.polygon_layer_combo.addItem("No polygon layers found", None)
            self.polygon_layer_combo.setEnabled(False)
        else:
            self.polygon_layer_combo.setEnabled(True)
            for lyr in layers:
                self.polygon_layer_combo.addItem(lyr.name(), lyr.id())
            if current_id:
                idx = self.polygon_layer_combo.findData(current_id)
                if idx != -1:
                    self.polygon_layer_combo.setCurrentIndex(idx)
        self.polygon_layer_combo.blockSignals(False)
        self.on_polygon_layer_changed()

    def on_polygon_layer_changed(self):
        lid = self.polygon_layer_combo.currentData()
        layer = QgsProject.instance().mapLayer(lid) if lid else None
        self.polygon_path_lineedit.setText(layer.source() if layer else "")

    def on_polygon_from_file(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select polygon file", os.path.expanduser("~"),
            "Vector files (*.shp *.gpkg);;All files (*)")
        if not path:
            return
        name = os.path.splitext(os.path.basename(path))[0]
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

    # ── Proxy helpers ─────────────────────────────────────────
    def _on_proxy_manual_toggled(self, state):
        enabled = state == CHECKED
        for w in [self.proxy_url_edit, self.proxy_user_edit, self.proxy_pass_edit]:
            w.setEnabled(enabled)
        if enabled:
            self.chk_proxy_auto.setChecked(False)

    def _on_test_connection(self):
        """Quick TCP probe to urs.earthdata.nasa.gov — runs in the UI thread
        (it's fast: 8 s max).  Updates the status label with the result."""
        import socket  # noqa: F401
        status_label = getattr(self, "lbl_connection_status", self.lbl_auth_status)
        status_label.setText("Testing...")
        QtWidgets.QApplication.processEvents()
        try:
            sock = socket.create_connection(("urs.earthdata.nasa.gov", 443), timeout=8)
            sock.close()
            status_label.setText(
                "✓  Connected — direct access works.")
        except (socket.timeout, OSError):
            # Try a basic requests GET through the configured proxy
            try:
                import requests
                proxies = self._collect_proxy_dict()
                r = requests.get(
                    "https://urs.earthdata.nasa.gov",
                    proxies=proxies if proxies else None,
                    timeout=(8, 10))
                status_label.setText(
                    f"✓  Connected via proxy (HTTP {r.status_code}).")
            except Exception as exc:
                status_label.setText(
                    f"✗  Failed: {exc}\n"
                    "  Try enabling Auto-detect or enter proxy manually.")

    def _collect_proxy_dict(self):
        """Build proxy dict from current GUI state (mirrors downloader logic)."""
        if self.chk_proxy_manual.isChecked():
            url = self.proxy_url_edit.text().strip()
            user = self.proxy_user_edit.text().strip()
            pwd = self.proxy_pass_edit.text()
            if url:
                if user and pwd:
                    from urllib.parse import urlparse
                    p = urlparse(url)
                    url = f"{p.scheme}://{user}:{pwd}@{p.netloc}{p.path}"
                return {"https": url, "http": url}
        if self.chk_proxy_auto.isChecked():
            import urllib.request
            return urllib.request.getproxies()
        return {}

    # ── Recolectar parámetros ─────────────────────────────────
    def collect_parameters(self):
        products = []
        if self.check_l2a.isChecked():
            products.append('GEDI02_A')
        if self.check_l2b.isChecked():
            products.append('GEDI02_B')
        if self.check_l4a.isChecked():
            products.append('GEDI04_A')
        if self.check_l4c.isChecked():
            products.append('GEDI04_C')

        selected_vars = {
            'GEDI02_A': self._collect_vars(L2A_VARS),
            'GEDI02_B': self._collect_vars(L2B_VARS),
            'GEDI04_A': self._collect_vars(L4A_VARS),
            'GEDI04_C': self._collect_vars(L4C_VARS),
        }

        surface_flags = []
        if not self.chk_surf_all.isChecked():
            if self.chk_surf_land.isChecked():
                surface_flags.append(1)
            if self.chk_surf_water.isChecked():
                surface_flags.append(2)

        filters = {
            'quality': {
                'GEDI02_A': self.spin_l2a_quality.value(),
                'GEDI02_B': self.spin_l2b_quality.value(),
                'GEDI04_A': self.spin_l4a_quality.value(),
                'GEDI04_C': self.spin_l4c_quality.value(),
            },
            'sensitivity': {
                'value': self.spin_sensitivity.value(),
                'apply_to': {
                    'GEDI02_A': self.chk_sens_l2a.isChecked(),
                    'GEDI02_B': self.chk_sens_l2b.isChecked(),
                    'GEDI04_A': self.chk_sens_l4a.isChecked(),
                    'GEDI04_C': self.chk_sens_l4c.isChecked(),
                }
            },
            'exclude_degrade': self.chk_exclude_degrade.isChecked(),
            'surface_flags': surface_flags,
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
            "output_dir": self.output_dir_lineedit.text().strip(),
            "products": products,
            "version": self.version_combo.currentText(),
            "start_date": self.start_date_edit.date().toString("yyyy.MM.dd"),
            "end_date": self.end_date_edit.date().toString("yyyy.MM.dd"),
            "recurring_months": self.recurring_months_check.isChecked(),
            "polygon_layer_id": self.polygon_layer_combo.currentData(),
            "polygon_source": self.polygon_path_lineedit.text().strip(),
            "selected_features_only": self.selected_only_check.isChecked(),
            "selected_vars": selected_vars,
            "filters": filters,
            "merge_how": "outer" if self.radio_merge_outer.isChecked() else "inner",
            "out_gpkg": self.chk_out_gpkg.isChecked(),
            "out_parquet": self.chk_out_parquet.isChecked(),
            "beams": beams,
            "keep_original": self.keep_original_check.isChecked(),
            "earthdata_user": self.earthdata_user_edit.text().strip(),
            "earthdata_pass": self.earthdata_pass_edit.text(),
            "earthdata_token": self.earthdata_token_edit.text().strip(),
            "keep_login": self.keep_login_check.isChecked(),
            "proxy_auto": self.chk_proxy_auto.isChecked(),
            "proxy_manual": self.chk_proxy_manual.isChecked(),
            "proxy_url": self.proxy_url_edit.text().strip(),
            "proxy_user": self.proxy_user_edit.text().strip(),
            "proxy_pass": self.proxy_pass_edit.text(),
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
        if params.get("earthdata_token"):
            return True
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

        self._save_settings(params)

        auth_mode = "Bearer Token" if params.get("earthdata_token") else "username/password"
        self.log_text_edit.append(
            f"[GEDIMetrics] Products : {', '.join(params['products'])}")
        self.log_text_edit.append(
            f"[GEDIMetrics] Auth     : {auth_mode}")
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
            self._show_error_guidance(error_message)
        self._worker_thread = None
        self.close_button.setText("Close")

    def _show_error_guidance(self, error_message):
        """Classify the error and provide actionable guidance to the user."""
        err = error_message.lower()
        guidance = []

        # ── Import / DLL errors ──
        if "no module named" in err or "importerror" in err:
            mod = error_message.split("'")[-2] if "'" in error_message else "unknown"
            guidance.append(
                f"A required Python package is not available: {mod}\n\n"
                "How to fix:\n"
                "• Windows (OSGeo4W): Open OSGeo4W Shell →\n"
                f"  pip install {mod}\n"
                "• Windows (standard QGIS): Reinstall QGIS via OSGeo4W\n"
                "  (https://trac.osgeo.org/osgeo4w/) — this is the most\n"
                "  reliable way to get all dependencies working.\n"
                f"• macOS: pip3 install {mod}\n"
                f"• Linux: sudo apt install python3-{mod}")

        elif "dll" in err or "library" in err or "hdf5" in err.lower():
            guidance.append(
                "A shared library conflict was detected (likely HDF5/GDAL).\n\n"
                "This usually happens when QGIS is installed via the standard\n"
                "installer instead of OSGeo4W.\n\n"
                "How to fix:\n"
                "1. Uninstall your current QGIS\n"
                "2. Download OSGeo4W: https://trac.osgeo.org/osgeo4w/\n"
                "3. Run → Advanced Install → select python3-h5py,\n"
                "   python3-geopandas, python3-fiona\n"
                "4. Restart QGIS and try again")

        # ── Authentication errors ──
        elif "401" in err or "unauthorized" in err or "forbidden" in err:
            guidance.append(
                "Authentication failed.\n\n"
                "How to fix:\n"
                "• Your Bearer Token may have expired.\n"
                "  1. Go to https://urs.earthdata.nasa.gov/user_tokens\n"
                "  2. Click 'Generate Token' (or revoke and regenerate)\n"
                "  3. Copy the new token into GEDIMetrics → EarthData tab\n"
                "\n"
                "• If using username/password:\n"
                "  Verify your credentials at https://urs.earthdata.nasa.gov")

        # ── Network / connection errors ──
        elif ("timeout" in err or "connection" in err
              or "urlopen" in err or "proxy" in err or "ssl" in err):
            guidance.append(
                "A network error occurred during data download.\n\n"
                "Possible causes and fixes:\n"
                "• Firewall blocking NASA servers → use Bearer Token auth\n"
                "  (EarthData tab → paste your token)\n"
                "• Slow connection → try again (the plugin will resume\n"
                "  from where it stopped)\n"
                "• Proxy needed → configure proxy in the plugin settings\n"
                "• VPN interference → try disconnecting your VPN")

        # ── Permission errors ──
        elif "permission" in err or "access" in err or "errno 13" in err:
            guidance.append(
                "Permission denied when writing output files.\n\n"
                "How to fix:\n"
                "• Choose a different output folder (e.g., your Desktop\n"
                "  or Documents folder)\n"
                "• Avoid writing to C:\\Program Files or system directories\n"
                "• On macOS/Linux: check folder permissions with\n"
                "  ls -la <your_output_folder>")

        # ── Disk space ──
        elif "no space" in err or "disk" in err or "errno 28" in err:
            guidance.append(
                "Not enough disk space to save the output.\n\n"
                "GEDI granules can be large (1-2 GB each).\n"
                "Free up disk space or choose an output folder on\n"
                "a drive with more available space.")

        # ── ROI / geometry errors ──
        elif "polygon" in err or "roi" in err or "extent" in err:
            guidance.append(
                "There was a problem with your Region of Interest (ROI).\n\n"
                "How to fix:\n"
                "• Make sure the polygon layer is loaded in QGIS\n"
                "• Verify the polygon has a valid geometry (no self-\n"
                "  intersections, not empty)\n"
                "• Try using a simpler polygon (e.g., a bounding box)")

        # ── Generic fallback ──
        if not guidance:
            guidance.append(
                f"An unexpected error occurred:\n{error_message}\n\n"
                "Suggestions:\n"
                "• Make sure QGIS is installed via OSGeo4W (Windows)\n"
                "• Verify your EarthData credentials and internet connection\n"
                "• Try with a smaller date range or fewer products\n"
                "• Report this error at:\n"
                "  https://github.com/AlexanderCotrinaS/gedi-metrics-qgis/issues")

        self.log_text_edit.append("")
        self.log_text_edit.append("═══ How to fix this error ═══")
        for g in guidance:
            for line in g.split("\n"):
                self.log_text_edit.append(f"  {line}")

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
    def _is_osgeo4w(self):
        """Detect if QGIS is running under OSGeo4W (recommended on Windows)."""
        osgeo4w_root = os.environ.get("OSGEO4W_ROOT", "")
        qgis_prefix = os.environ.get("QGIS_PREFIX_PATH", "")
        return bool(osgeo4w_root) or "osgeo4w" in qgis_prefix.lower()

    def _try_auto_install(self, packages):
        """Attempt to install missing packages via pip.

        Returns list of packages that could NOT be installed.
        """
        if not packages:
            return []

        import subprocess

        pip_names = {
            "h5py": "h5py",
            "pandas": "pandas",
            "geopandas": "geopandas",
            "numpy": "numpy",
            "shapely": "shapely",
            "requests": "requests",
            "fiona": "fiona",
            "rtree": "rtree",
            "pyarrow": "pyarrow",
        }

        to_install = [pip_names.get(p, p) for p in packages]
        print(f"[GEDIMetrics] Attempting to install: {', '.join(to_install)}")

        # Show a wait dialog
        progress = QtWidgets.QProgressDialog(
            f"Installing dependencies: {', '.join(to_install)}...\n"
            "This may take a minute on first run.",
            None, 0, 0, self)
        progress.setWindowTitle("GEDIMetrics — Installing Dependencies")
        progress.setMinimumDuration(0)
        progress.show()
        QtWidgets.QApplication.processEvents()

        try:
            cmd = [sys.executable, "-m", "pip", "install",
                   "--break-system-packages"] + to_install
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                # Retry without --break-system-packages (older pip)
                cmd = [sys.executable, "-m", "pip", "install"] + to_install
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                print(f"[GEDIMetrics] ✓ Successfully installed: {', '.join(to_install)}")
            else:
                print(f"[GEDIMetrics] pip install failed:\n{result.stderr[-500:]}")
        except Exception as e:
            print(f"[GEDIMetrics] Auto-install error: {e}")
        finally:
            progress.close()

        # Re-check what's still missing
        still_missing = []
        for mod in packages:
            try:
                __import__(mod)
            except ImportError:
                still_missing.append(mod)

        if still_missing:
            print(f"[GEDIMetrics] Still missing after install attempt: "
                  f"{', '.join(still_missing)}")
        return still_missing

    def check_dependencies(self):
        # ── Critical packages (block execution if missing) ──
        critical_missing = []
        for mod in ["h5py", "pandas", "geopandas", "numpy", "shapely", "requests"]:
            try:
                __import__(mod)
            except ImportError:
                critical_missing.append(mod)

        # ── h5py HDF5 version check ──
        warnings_list = []
        try:
            import h5py
            built = getattr(h5py.version, "hdf5_built_version", None)
            runtime = getattr(h5py.version, "hdf5_version", None)
            if built and runtime and built.split(".")[:2] != runtime.split(".")[:2]:
                warnings_list.append(
                    f"⚠ h5py HDF5 version mismatch:\n"
                    f"  Built with HDF5 {built}, but runtime has {runtime}.\n"
                    f"  This may cause crashes when reading GEDI files.\n"
                    f"  Fix: uninstall h5py and reinstall via OSGeo4W Setup\n"
                    f"  (search 'python3-h5py' in the package list).")
        except ImportError:
            pass  # already in critical_missing

        # ── Optional packages (fiona, rtree, pyarrow) ──
        # NOT checked here. They are used internally by geopandas or only
        # needed for specific output formats. If they fail at runtime,
        # the error handler (_show_error_guidance) provides clear guidance.

        # ── Auto-install critical packages if missing ──
        if critical_missing:
            reply = QtWidgets.QMessageBox.question(
                self, "GEDIMetrics — Missing Dependencies",
                f"The following required packages are missing:\n"
                f"  {', '.join(critical_missing)}\n\n"
                f"Would you like GEDIMetrics to install them automatically?\n"
                f"(This requires an internet connection)",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.Yes)

            if reply == QtWidgets.QMessageBox.Yes:
                still_missing = self._try_auto_install(critical_missing)
            else:
                still_missing = critical_missing

            if still_missing:
                self._show_manual_install_guide(still_missing, warnings_list)
                return False

        # ── Show warnings if any (non-blocking) ──
        if warnings_list:
            msg = "\n\n".join(warnings_list)
            reply = QtWidgets.QMessageBox.information(
                self, "GEDIMetrics — Dependency Notes",
                f"{msg}\n\n"
                f"These are informational only. The plugin should work normally.\n"
                f"Click OK to continue.")

        return True

    def _show_manual_install_guide(self, missing, warnings_list):
        """Show OS-specific installation instructions when auto-install fails."""
        lines = []
        lines.append(
            "❌ Could not install the following packages:\n  "
            + ", ".join(sorted(set(missing))))
        lines.append(
            "Automatic installation was not possible.\n"
            "Please install them manually using the instructions below.")

        for w in warnings_list:
            lines.append(w)

        os_name = platform.system()
        if os_name == "Windows":
            if self._is_osgeo4w():
                lines.append(
                    "═══ Manual install (OSGeo4W) ═══\n"
                    "1. Close QGIS\n"
                    "2. Open the OSGeo4W Shell\n"
                    "   (Start Menu → OSGeo4W Shell)\n"
                    "3. Run:\n"
                    f"   pip install {' '.join(sorted(set(missing)))}\n"
                    "4. Restart QGIS\n"
                    "\n"
                    "For h5py issues, prefer the OSGeo4W package:\n"
                    "  Re-run OSGeo4W Setup → Advanced Install →\n"
                    "  search 'python3-h5py' and mark it for installation.")
            else:
                lines.append(
                    "═══ Manual install (Windows — standard QGIS) ═══\n"
                    "Your QGIS is NOT using OSGeo4W. This is the most\n"
                    "common cause of dependency problems on Windows.\n"
                    "\n"
                    "RECOMMENDED: Reinstall QGIS via OSGeo4W:\n"
                    "  1. Download from https://trac.osgeo.org/osgeo4w/\n"
                    "  2. Run → Advanced Install\n"
                    "  3. Select these packages:\n"
                    "     python3-h5py, python3-pandas,\n"
                    "     python3-geopandas, python3-shapely,\n"
                    "     python3-fiona, python3-requests\n"
                    "  4. Complete installation and restart QGIS")
        elif os_name == "Darwin":
            lines.append(
                "═══ Manual install (macOS) ═══\n"
                "Open Terminal and run:\n"
                f"  pip3 install {' '.join(sorted(set(missing)))}\n"
                "\n"
                "If you get permission errors:\n"
                f"  pip3 install --user {' '.join(sorted(set(missing)))}\n"
                "\n"
                "Then restart QGIS.")
        else:
            apt_names = {
                "h5py": "python3-h5py", "pandas": "python3-pandas",
                "geopandas": "python3-geopandas", "shapely": "python3-shapely",
                "fiona": "python3-fiona", "rtree": "python3-rtree",
                "requests": "python3-requests", "numpy": "python3-numpy",
            }
            apt_pkgs = " ".join(
                apt_names.get(m, f"python3-{m}")
                for m in sorted(set(missing)))
            lines.append(
                "═══ Manual install (Linux) ═══\n"
                f"  sudo apt install {apt_pkgs}\n"
                "\n"
                "Then restart QGIS.")

        QtWidgets.QMessageBox.critical(
            self, "GEDIMetrics — Manual Installation Required",
            "\n\n".join(lines))
