# -*- coding: utf-8 -*-
"""
GEDIMetrics — QGIS Plugin
Author: Alexander Cotrina-Sanchez
"""
import sys
import os

# Limpiar paths espurios que el script de prueba pudo haber añadido
# Esto evita el conflicto con imports relativos del paquete
_plugin_dir = os.path.dirname(__file__)
_spurious = [
    os.path.join(_plugin_dir, "pipeline", "pipeline"),
    os.path.join(_plugin_dir, "pipeline", "utils"),
    os.path.join(_plugin_dir, "pipeline"),
]
for _p in _spurious:
    _p_norm = os.path.normpath(_p)
    sys.path[:] = [p for p in sys.path
                   if os.path.normpath(p) != _p_norm]


def classFactory(iface):
    from .gedi_metrics import GEDIMetrics
    return GEDIMetrics(iface)
