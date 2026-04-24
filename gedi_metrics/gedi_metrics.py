# -*- coding: utf-8 -*-
"""
GEDIMetrics — plugin principal
Author: Alexander Cotrina-Sanchez
"""
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction

import os
import sys
import platform


def add_vendor_paths(plugin_dir):
    tag = {"Windows": "win", "Darwin": "macos", "Linux": "linux"}.get(
        platform.system())
    vendor_paths = []
    if tag:
        vendor_paths.append(os.path.join(plugin_dir, "vendor", tag))
    vendor_paths.append(os.path.join(plugin_dir, "vendor", "common"))
    for vp in vendor_paths:
        if os.path.isdir(vp) and vp not in sys.path:
            sys.path.insert(0, vp)


from .gedi_metrics_dialog import GEDIMetricsDialog


class GEDIMetrics:
    """QGIS Plugin Implementation — GEDIMetrics."""

    def __init__(self, iface):
        self.iface      = iface
        self.plugin_dir = os.path.dirname(__file__)
        add_vendor_paths(self.plugin_dir)

        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            self.plugin_dir, 'i18n',
            'GEDIMetrics_{}.qm'.format(locale))
        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

        self.actions     = []
        self.menu        = self.tr(u'&GEDIMetrics')
        self.first_start = None

    def tr(self, message):
        return QCoreApplication.translate('GEDIMetrics', message)

    def add_action(self, icon_path, text, callback,
                   enabled_flag=True, add_to_menu=True,
                   add_to_toolbar=True, status_tip=None,
                   whats_this=None, parent=None):
        icon   = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)
        if status_tip:
            action.setStatusTip(status_tip)
        if whats_this:
            action.setWhatsThis(whats_this)
        if add_to_toolbar:
            self.iface.addToolBarIcon(action)
        if add_to_menu:
            self.iface.addPluginToMenu(self.menu, action)
        self.actions.append(action)
        return action

    def initGui(self):
        # Cargar ícono directo desde archivo — evita dependencia de Qt resources
        icon_path = os.path.join(self.plugin_dir, 'icon.png')
        self.add_action(
            icon_path,
            text       = self.tr(u'GEDIMetrics'),
            callback   = self.run,
            status_tip = self.tr(u'Download and merge GEDI products'),
            parent     = self.iface.mainWindow())
        self.first_start = True

    def unload(self):
        for action in self.actions:
            self.iface.removePluginMenu(self.tr(u'&GEDIMetrics'), action)
            self.iface.removeToolBarIcon(action)

    def run(self):
        if self.first_start:
            self.first_start = False
            self.dlg = GEDIMetricsDialog(plugin_dir=self.plugin_dir)
        self.dlg.show()
        self.dlg.exec()
