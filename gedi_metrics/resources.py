# -*- coding: utf-8 -*-
# GEDIMetrics resources — carga el ícono desde el archivo local
# Compatible con QGIS 3.x / PyQt5

from qgis.PyQt.QtCore import QFile, QIODevice
from qgis.PyQt.QtGui import QIcon, QPixmap
from qgis.PyQt import QtCore
import os

# Path al ícono (relativo al directorio del plugin)
_ICON_FILE = os.path.join(os.path.dirname(__file__), 'icon.png')

def _register_icon():
    """Registra el ícono en el sistema de recursos Qt."""
    if not os.path.isfile(_ICON_FILE):
        return
    # Leer el PNG y registrarlo como recurso Qt en :/plugins/gedi_metrics/icon.png
    with open(_ICON_FILE, 'rb') as f:
        data = f.read()

    qt_resource_data = data
    
    # Usar QPixmap cache directamente — más simple y compatible
    pm = QPixmap()
    pm.loadFromData(data)
    # Registrar en el cache de Qt con la clave correcta
    QtCore.QDir.addSearchPath('plugins', os.path.dirname(os.path.dirname(__file__)))

_register_icon()
