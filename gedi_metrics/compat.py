# -*- coding: utf-8 -*-
"""
GEDIMetrics — compat.py
Capa de compatibilidad entre QGIS 3.x (PyQt5) y QGIS 4.x (PyQt6).
Importar desde aquí en lugar de usar Qt directamente.
"""
from qgis.PyQt import QtCore

# ── Versión Qt ────────────────────────────────────────────────
QT_MAJOR = int(QtCore.QT_VERSION_STR.split('.')[0])  # 5 o 6

# ── sip ───────────────────────────────────────────────────────
# PyQt5: import sip  (módulo independiente)
# PyQt6: from PyQt6 import sip
try:
    from PyQt6 import sip
except ImportError:
    try:
        import sip
    except ImportError:
        sip = None          # fallback — isdeleted no disponible

# ── Qt.Checked ────────────────────────────────────────────────
# PyQt5: QtCore.Qt.Checked
# PyQt6: QtCore.Qt.CheckState.Checked
try:
    CHECKED = QtCore.Qt.CheckState.Checked      # PyQt6
except AttributeError:
    CHECKED = QtCore.Qt.Checked                 # PyQt5

# ── QgsMapLayer.VectorLayer ───────────────────────────────────
# QGIS 3.x: QgsMapLayer.VectorLayer
# QGIS 4.x: Qgis.LayerType.Vector
try:
    from qgis.core import Qgis
    VECTOR_LAYER_TYPE = Qgis.LayerType.Vector
except AttributeError:
    from qgis.core import QgsMapLayer
    VECTOR_LAYER_TYPE = QgsMapLayer.VectorLayer

# ── exec_dialog ───────────────────────────────────────────────
# PyQt5: dlg.exec_()
# PyQt6: dlg.exec()   (exec_() fue eliminado)
def exec_dialog(dlg):
    try:
        return dlg.exec()
    except AttributeError:
        return dlg.exec_()

# ── isdeleted ─────────────────────────────────────────────────
# Wrapper seguro para sip.isdeleted
def is_deleted(obj):
    if sip is None:
        return False
    try:
        return sip.isdeleted(obj)
    except Exception:
        return False
