#!/usr/bin/env python3
"""VBZ MQTT Live - Meldungsanzeige + Anlagen-Übersicht mit Excel-Import"""

import sys
import json
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QHeaderView, QTextEdit, QSplitter, QCheckBox,
    QSpinBox, QMessageBox, QAbstractItemView, QTabWidget, QComboBox,
    QFileDialog
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QRect
from PyQt5.QtGui import QColor, QFont, QIcon, QPixmap, QPainter, QBrush
import paho.mqtt.client as mqtt


def create_vbz_icon() -> QIcon:
    """Erstellt das VBZ-Logo als Icon (blau, weisser Text)."""
    size = 64
    px = QPixmap(size, size)
    px.fill(QColor("#1888b8"))
    p = QPainter(px)
    p.setPen(Qt.white)
    font = QFont("Arial", 22, QFont.Bold)
    p.setFont(font)
    p.drawText(QRect(0, 0, size, size), Qt.AlignCenter, "VBZ")
    p.end()
    return QIcon(px)


class MQTTWorker(QThread):
    message_received = pyqtSignal(str, str)
    connection_changed = pyqtSignal(bool, str)

    def __init__(self):
        super().__init__()
        self.client = None

    def connect_broker(self, host, port, username, password, topic):
        self._topic = topic
        self._host = host
        self._port = port

        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass

        self.client = mqtt.Client()
        if username:
            self.client.username_pw_set(username, password)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        try:
            self.client.connect(host, port, keepalive=60)
            self.client.loop_start()
        except Exception as e:
            self.connection_changed.emit(False, f"Verbindungsfehler: {e}")

    def disconnect_broker(self):
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(self._topic)
            self.connection_changed.emit(True, f"Verbunden mit {self._host}:{self._port}, Topic: {self._topic}")
        else:
            codes = {
                1: "Ungültige Protokollversion",
                2: "Ungültige Client-ID",
                3: "Broker nicht verfügbar",
                4: "Falscher Benutzername/Passwort",
                5: "Nicht autorisiert",
            }
            self.connection_changed.emit(False, f"Fehler: {codes.get(rc, f'RC={rc}')}")

    def _on_disconnect(self, client, userdata, rc):
        msg = "Verbindung getrennt" if rc == 0 else f"Verbindung verloren (RC={rc})"
        self.connection_changed.emit(False, msg)

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
        except Exception:
            payload = str(msg.payload)
        self.message_received.emit(msg.topic, payload)


# ---------------------------------------------------------------------------

def make_lamp(status: str) -> QLabel:
    """Erstellt eine farbige Statuslampe (●)."""
    label = QLabel("●")
    label.setAlignment(Qt.AlignCenter)
    label.setFont(QFont("Arial", 16))
    color = {"ok": "#22c55e", "error": "#f97316", "offline": "#ef4444"}.get(status, "#ef4444")
    label.setStyleSheet(f"color: {color};")
    tooltip = {"ok": "HEALTH_OK", "error": "Health-Fehler", "offline": "Keine Meldung"}.get(status, "")
    label.setToolTip(tooltip)
    return label


class AnlagenTab(QWidget):
    def __init__(self, device_status: dict):
        super().__init__()
        self.device_status = device_status
        self.anlagen = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(6)

        toolbar = QHBoxLayout()
        btn_import = QPushButton("Excel importieren…")
        btn_import.clicked.connect(self.import_excel)
        toolbar.addWidget(btn_import)

        self.lbl_file = QLabel("Keine Datei geladen")
        self.lbl_file.setStyleSheet("color: grey;")
        toolbar.addWidget(self.lbl_file)

        toolbar.addSpacing(20)
        toolbar.addWidget(QLabel("Besitzer (MVU):"))
        self.combo_mvu = QComboBox()
        self.combo_mvu.setMinimumWidth(160)
        self.combo_mvu.addItem("Alle")
        self.combo_mvu.currentTextChanged.connect(self.refresh_table)
        toolbar.addWidget(self.combo_mvu)

        toolbar.addStretch()
        self.lbl_count = QLabel("")
        toolbar.addWidget(self.lbl_count)
        layout.addLayout(toolbar)

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Status", "MVU", "Tech Nr", "Haltestelle"])
        hh = self.table.horizontalHeader()
        hh.setSectionResizeMode(QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.Stretch)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)

    def import_excel(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Excel-Datei öffnen", "", "Excel-Dateien (*.xlsx *.xls)"
        )
        if not path:
            return
        try:
            import openpyxl
        except ImportError:
            QMessageBox.critical(self, "Fehler", "Bitte 'openpyxl' installieren:\n  pip install openpyxl")
            return

        try:
            wb = openpyxl.load_workbook(path, data_only=True)
            ws = wb.active

            # Header-Zeile suchen
            col_mvu = col_tech = col_halt = None
            header_row = 0
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                cells = [str(c).strip().lower() if c is not None else "" for c in row]
                if "mvu" in cells:
                    col_mvu  = cells.index("mvu")
                    col_tech = next((j for j, c in enumerate(cells) if "tech" in c), None)
                    col_halt = next((j for j, c in enumerate(cells) if "halt" in c), None)
                    header_row = i + 1
                    break

            # Fallback: erste drei Spalten
            if col_mvu is None:
                col_mvu, col_tech, col_halt = 0, 1, 2
                header_row = 1

            self.anlagen = []
            for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
                def cell(idx):
                    return row[idx] if idx is not None and idx < len(row) else None

                mvu_val   = cell(col_mvu)
                tech_val  = cell(col_tech)
                halt_val  = cell(col_halt)

                # Zeilen ohne Tech Nr überspringen
                if tech_val is None:
                    continue
                tech_str = str(tech_val).strip().split(".")[0]  # "42190.0" → "42190"
                if not tech_str.isdigit():
                    continue

                # Summenzeilen überspringen (MVU enthält "TOTAL")
                mvu_str = str(mvu_val).strip() if mvu_val else ""
                if "total" in mvu_str.lower():
                    continue

                self.anlagen.append({
                    "mvu":        mvu_str,
                    "tech_nr":    tech_str,
                    "haltestelle": str(halt_val).strip() if halt_val else "",
                })

            # MVU-Filter befüllen
            mvus = sorted(set(a["mvu"] for a in self.anlagen if a["mvu"]))
            self.combo_mvu.blockSignals(True)
            self.combo_mvu.clear()
            self.combo_mvu.addItem("Alle")
            for mvu in mvus:
                self.combo_mvu.addItem(mvu)
            self.combo_mvu.blockSignals(False)

            import os
            self.lbl_file.setText(os.path.basename(path))
            self.lbl_file.setStyleSheet("color: black;")
            self.refresh_table()

        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Excel konnte nicht gelesen werden:\n{e}")

    def refresh_table(self):
        selected = self.combo_mvu.currentText()
        filtered = [
            a for a in self.anlagen
            if selected == "Alle" or a["mvu"] == selected
        ]

        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        for anlage in filtered:
            row = self.table.rowCount()
            self.table.insertRow(row)

            status = self.device_status.get(anlage["tech_nr"], "offline")
            self.table.setCellWidget(row, 0, make_lamp(status))

            for col, key in enumerate(["mvu", "tech_nr", "haltestelle"], start=1):
                item = QTableWidgetItem(anlage[key])
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                # Tech Nr als Zahl sortierbar machen
                if key == "tech_nr":
                    item.setData(Qt.UserRole, int(anlage[key]))
                self.table.setItem(row, col, item)

        self.table.setSortingEnabled(True)
        self.lbl_count.setText(f"{len(filtered)} Anlagen")

    def update_lamps(self):
        """Aktualisiert nur die Statuslampen (ohne Tabelle neu aufzubauen)."""
        selected = self.combo_mvu.currentText()
        filtered = [
            a for a in self.anlagen
            if selected == "Alle" or a["mvu"] == selected
        ]
        for row, anlage in enumerate(filtered):
            status = self.device_status.get(anlage["tech_nr"], "offline")
            self.table.setCellWidget(row, 0, make_lamp(status))


# ---------------------------------------------------------------------------

class MeldungenTab(QWidget):
    """Bestehender Meldungs-Tab (ausgelagert in eigenes Widget)."""
    def __init__(self):
        super().__init__()
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(6)
        layout.addWidget(self._make_filter_panel())

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._make_table())
        splitter.addWidget(self._make_detail_panel())
        splitter.setSizes([520, 180])
        layout.addWidget(splitter, stretch=1)

    def _make_filter_panel(self):
        box = QGroupBox("Filter")
        outer = QVBoxLayout(box)
        outer.setSpacing(4)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("SboId:"))
        self.inp_sboid = QLineEdit()
        self.inp_sboid.setPlaceholderText("z.B. 100648")
        self.inp_sboid.setFixedWidth(120)
        self.inp_sboid.setToolTip(
            "Filtert Meldungen, deren Topic diese SboId enthält.\n"
            "Leer lassen = alle Topics zeigen."
        )
        row1.addWidget(self.inp_sboid)

        self.chk_health = QCheckBox("Nur Fehler (HEALTH_OK ausblenden)")
        self.chk_health.setChecked(True)
        row1.addWidget(self.chk_health)

        btn_clear = QPushButton("Leeren")
        btn_clear.setFixedWidth(75)
        btn_clear.clicked.connect(self.clear_table)
        row1.addWidget(btn_clear)
        outer.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Gerätetyp:"))
        self.chk_dcu   = QCheckBox("dcu");   self.chk_dcu.setChecked(True)
        self.chk_du    = QCheckBox("du");    self.chk_du.setChecked(True)
        self.chk_pau   = QCheckBox("pau");   self.chk_pau.setChecked(True)
        self.chk_other = QCheckBox("andere"); self.chk_other.setChecked(False)
        for chk in (self.chk_dcu, self.chk_du, self.chk_pau, self.chk_other):
            row2.addWidget(chk)
        row2.addStretch()
        outer.addLayout(row2)

        return box

    def _make_table(self):
        self.table = QTableWidget()
        self.table.setColumnCount(11)
        self.table.setHorizontalHeaderLabels([
            "Zeitstempel", "Typ", "Topic", "Beschreibung",
            "Health", "Erreichbarkeit", "Aktivierung",
            "Grund", "CPU %", "RAM %", "Disk %",
        ])
        hh = self.table.horizontalHeader()
        hh.setSectionResizeMode(QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.Stretch)
        hh.setSectionResizeMode(3, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.setSortingEnabled(True)
        self.table.itemSelectionChanged.connect(self._show_detail)
        return self.table

    def _make_detail_panel(self):
        box = QGroupBox("Details – vollständige Meldung (JSON)")
        lay = QVBoxLayout(box)
        self.detail = QTextEdit()
        self.detail.setReadOnly(True)
        self.detail.setFont(QFont("Courier New", 9))
        lay.addWidget(self.detail)
        return box

    def _find_row(self, topic: str) -> int:
        """Gibt die Zeile für dieses Topic zurück, oder -1 wenn nicht vorhanden."""
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item and item.data(Qt.UserRole + 1) == topic:
                return r
        return -1

    def add_row(self, topic: str, device_type: str, data: dict, raw: str):
        self.table.setSortingEnabled(False)

        existing = self._find_row(topic)
        row = existing if existing >= 0 else self.table.rowCount()
        if existing < 0:
            self.table.insertRow(row)

        header = data.get("msg_header", {})
        usage  = data.get("usage", {})
        health = data.get("health", "")

        cells = [
            header.get("timestamp", ""),
            device_type,
            topic,
            data.get("description", ""),
            health,
            data.get("reachability", ""),
            data.get("activation", ""),
            data.get("reason", ""),
            str(usage.get("cpu", "")),
            str(usage.get("ram", "")),
            str(usage.get("disk", "")),
        ]

        for col, val in enumerate(cells):
            item = QTableWidgetItem(val)
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            if col == 0:
                item.setData(Qt.UserRole, raw)        # vollständiges JSON
                item.setData(Qt.UserRole + 1, topic)  # Topic als Schlüssel
            self.table.setItem(row, col, item)

        if health in ("HEALTH_OK", ""):
            color = QColor(255, 255, 200)
        elif "WARN" in health or "DEGRADED" in health:
            color = QColor(255, 220, 150)
        else:
            color = QColor(255, 180, 180)
        for col in range(self.table.columnCount()):
            self.table.item(row, col).setBackground(color)

        self.table.setSortingEnabled(True)
        if existing < 0:
            self.table.scrollToBottom()

    def clear_table(self):
        self.table.setRowCount(0)
        self.detail.clear()

    def _show_detail(self):
        row = self.table.currentRow()
        if row < 0:
            return
        item = self.table.item(row, 0)
        if item:
            raw = item.data(Qt.UserRole)
            if raw:
                try:
                    formatted = json.dumps(json.loads(raw), indent=2, ensure_ascii=False)
                except Exception:
                    formatted = raw
                self.detail.setPlainText(formatted)


# ---------------------------------------------------------------------------

class MQTTViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.worker = MQTTWorker()
        self.worker.message_received.connect(self.handle_message)
        self.worker.connection_changed.connect(self.handle_connection)
        self._total = 0
        self._shown = 0
        self._is_connected = False
        self.device_status = {}  # tech_nr (str) -> "ok" | "error" | "offline"
        self._setup_ui()

    def _setup_ui(self):
        self.setWindowTitle("VBZ MQTT Live")
        self.setWindowIcon(create_vbz_icon())
        self.setMinimumSize(1300, 750)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(6)

        main_layout.addWidget(self._make_connection_panel())

        self.tabs = QTabWidget()
        self.meldungen_tab = MeldungenTab()
        self.anlagen_tab   = AnlagenTab(self.device_status)
        self.tabs.addTab(self.meldungen_tab, "Meldungen")
        self.tabs.addTab(self.anlagen_tab,   "Anlagen")
        main_layout.addWidget(self.tabs, stretch=1)

        self.statusBar().showMessage("Nicht verbunden")

    def _make_connection_panel(self):
        box = QGroupBox("Broker-Verbindung")
        row = QHBoxLayout(box)

        row.addWidget(QLabel("Host:"))
        self.inp_host = QLineEdit("localhost")
        self.inp_host.setFixedWidth(160)
        row.addWidget(self.inp_host)

        row.addWidget(QLabel("Port:"))
        self.inp_port = QSpinBox()
        self.inp_port.setRange(1, 65535)
        self.inp_port.setValue(1883)
        self.inp_port.setFixedWidth(75)
        row.addWidget(self.inp_port)

        row.addWidget(QLabel("Benutzer:"))
        self.inp_user = QLineEdit()
        self.inp_user.setFixedWidth(110)
        row.addWidget(self.inp_user)

        row.addWidget(QLabel("Passwort:"))
        self.inp_pass = QLineEdit()
        self.inp_pass.setEchoMode(QLineEdit.Password)
        self.inp_pass.setFixedWidth(110)
        row.addWidget(self.inp_pass)

        row.addWidget(QLabel("Topic:"))
        self.inp_topic = QLineEdit("#")
        self.inp_topic.setFixedWidth(130)
        row.addWidget(self.inp_topic)

        self.btn_connect = QPushButton("Verbinden")
        self.btn_connect.setFixedWidth(110)
        self.btn_connect.clicked.connect(self._toggle_connection)
        row.addWidget(self.btn_connect)

        return box

    # ------------------------------------------------------------------ logic

    def _toggle_connection(self):
        if not self._is_connected:
            host = self.inp_host.text().strip()
            if not host:
                QMessageBox.warning(self, "Fehler", "Bitte Host angeben.")
                return
            self.btn_connect.setEnabled(False)
            self.btn_connect.setText("Verbinde…")
            self.worker.connect_broker(
                host,
                self.inp_port.value(),
                self.inp_user.text().strip(),
                self.inp_pass.text(),
                self.inp_topic.text().strip() or "#",
            )
        else:
            self.worker.disconnect_broker()

    def handle_connection(self, connected: bool, message: str):
        self._is_connected = connected
        self.btn_connect.setEnabled(True)
        if connected:
            self.btn_connect.setText("Trennen")
            self.statusBar().showMessage(f"✓  {message}  |  Empfangen: 0  |  Angezeigt: 0")
        else:
            self.btn_connect.setText("Verbinden")
            self.statusBar().showMessage(f"✗  {message}")

    @staticmethod
    def _device_type_from_topic(topic: str) -> str:
        known = {"dcu", "du", "pau"}
        for seg in topic.split("/"):
            if seg in known:
                return seg
        return ""

    @staticmethod
    def _tech_nr_from_topic(topic: str) -> str | None:
        """Extrahiert die Tech-Nr aus dem Topic (Segment nach dcu/du/pau)."""
        known = {"dcu", "du", "pau"}
        parts = topic.split("/")
        for i, seg in enumerate(parts):
            if seg in known and i + 1 < len(parts):
                tech = parts[i + 1].split(":")[0]  # "42094:01" → "42094"
                if tech.isdigit():
                    return tech
        return None

    def handle_message(self, topic: str, payload: str):
        self._total += 1

        mt = self.meldungen_tab

        # SboId-Filter
        sboid = mt.inp_sboid.text().strip()
        if sboid and sboid not in topic:
            self._update_status()
            return

        # Gerätetyp-Filter
        device_type = self._device_type_from_topic(topic)
        allowed = set()
        if mt.chk_dcu.isChecked():   allowed.add("dcu")
        if mt.chk_du.isChecked():    allowed.add("du")
        if mt.chk_pau.isChecked():   allowed.add("pau")

        if device_type in {"dcu", "du", "pau"}:
            if device_type not in allowed:
                self._update_status()
                return
        else:
            if not mt.chk_other.isChecked():
                self._update_status()
                return

        # JSON parsen
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            self._update_status()
            return

        health = data.get("health", "")

        # Anlagen-Status aktualisieren
        tech_nr = self._tech_nr_from_topic(topic)
        if tech_nr:
            new_status = "ok" if health == "HEALTH_OK" else "error"
            if self.device_status.get(tech_nr) != new_status:
                self.device_status[tech_nr] = new_status
                self.anlagen_tab.update_lamps()

        # Health-Filter für Meldungs-Tab
        if mt.chk_health.isChecked() and health == "HEALTH_OK":
            self._update_status()
            return

        is_new = mt._find_row(topic) < 0
        if is_new:
            self._shown += 1
        mt.add_row(topic, device_type, data, payload)
        self._update_status()

    def _update_status(self):
        state = "Verbunden" if self._is_connected else "Getrennt"
        self.statusBar().showMessage(
            f"{state}  |  Empfangen: {self._total}  |  Angezeigt: {self._shown}"
        )

    def closeEvent(self, event):
        self.worker.disconnect_broker()
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName("VBZ MQTT Live")
    icon = create_vbz_icon()
    app.setWindowIcon(icon)
    win = MQTTViewer()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
