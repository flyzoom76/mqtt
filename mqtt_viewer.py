#!/usr/bin/env python3
"""MQTT Viewer - Zeigt nur fehlerhafte Meldungen mit sboid-Filterung"""

import sys
import json
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QHeaderView, QTextEdit, QSplitter, QCheckBox,
    QSpinBox, QMessageBox, QAbstractItemView
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QMetaObject, Q_ARG
from PyQt5.QtGui import QColor, QFont
import paho.mqtt.client as mqtt


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


class MQTTViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.worker = MQTTWorker()
        self.worker.message_received.connect(self.handle_message)
        self.worker.connection_changed.connect(self.handle_connection)
        self._total = 0
        self._shown = 0
        self._is_connected = False
        self._setup_ui()

    def _setup_ui(self):
        self.setWindowTitle("MQTT Viewer")
        self.setMinimumSize(1300, 750)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setSpacing(6)

        top = QHBoxLayout()
        top.addWidget(self._make_connection_panel(), stretch=3)
        top.addWidget(self._make_filter_panel(), stretch=2)
        layout.addLayout(top)

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._make_table())
        splitter.addWidget(self._make_detail_panel())
        splitter.setSizes([520, 180])
        layout.addWidget(splitter, stretch=1)

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

    def _make_filter_panel(self):
        box = QGroupBox("Filter")
        outer = QVBoxLayout(box)
        outer.setSpacing(4)

        # Zeile 1: SboId + Health-Filter + Leeren
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
        btn_clear.clicked.connect(self._clear_table)
        row1.addWidget(btn_clear)
        outer.addLayout(row1)

        # Zeile 2: Gerätetyp-Checkboxen
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Gerätetyp:"))
        self.chk_dcu = QCheckBox("dcu")
        self.chk_dcu.setChecked(True)
        self.chk_du = QCheckBox("du")
        self.chk_du.setChecked(True)
        self.chk_pau = QCheckBox("pau")
        self.chk_pau.setChecked(True)
        self.chk_other = QCheckBox("andere")
        self.chk_other.setChecked(False)
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
        """Extrahiert den Gerätetyp (dcu/du/pau) aus dem Topic-Pfad."""
        known = {"dcu", "du", "pau"}
        for segment in topic.split("/"):
            if segment in known:
                return segment
        return ""

    def handle_message(self, topic: str, payload: str):
        self._total += 1

        # SboId-Filter (topic-basiert)
        sboid = self.inp_sboid.text().strip()
        if sboid and sboid not in topic:
            self._update_status()
            return

        # Gerätetyp-Filter
        device_type = self._device_type_from_topic(topic)
        allowed = set()
        if self.chk_dcu.isChecked():
            allowed.add("dcu")
        if self.chk_du.isChecked():
            allowed.add("du")
        if self.chk_pau.isChecked():
            allowed.add("pau")
        if device_type in {"dcu", "du", "pau"}:
            if device_type not in allowed:
                self._update_status()
                return
        else:
            # unbekannter Typ: nur anzeigen wenn "andere" aktiviert
            if not self.chk_other.isChecked():
                self._update_status()
                return

        # JSON parsen
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            self._update_status()
            return

        # Health-Filter
        health = data.get("health", "")
        if self.chk_health.isChecked() and health == "HEALTH_OK":
            self._update_status()
            return

        self._shown += 1
        self._add_row(topic, device_type, data, payload)
        self._update_status()

    def _add_row(self, topic: str, device_type: str, data: dict, raw: str):
        self.table.setSortingEnabled(False)

        row = self.table.rowCount()
        self.table.insertRow(row)

        header = data.get("msg_header", {})
        usage = data.get("usage", {})
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
                item.setData(Qt.UserRole, raw)
            self.table.setItem(row, col, item)

        # Zeilenfarbe nach Health-Status
        if health in ("HEALTH_OK", ""):
            color = QColor(255, 255, 200)   # gelb (nur wenn Filter aus)
        elif "WARN" in health or "DEGRADED" in health:
            color = QColor(255, 220, 150)   # orange
        else:
            color = QColor(255, 180, 180)   # rot
        for col in range(self.table.columnCount()):
            self.table.item(row, col).setBackground(color)

        self.table.setSortingEnabled(True)
        self.table.scrollToBottom()

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

    def _clear_table(self):
        self.table.setRowCount(0)
        self.detail.clear()
        self._total = 0
        self._shown = 0
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
    win = MQTTViewer()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
