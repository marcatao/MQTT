"""Ponto de entrada: `python -m app.main` abre a janela MQTT → SQL Server."""
from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler

from PySide6.QtCore import Qt
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import QApplication

from app.bridge import PipelineBridge
from app.config import carregar, pasta_base
from app.gui import estilo
from app.gui.janela import APLICACAO, ORGANIZACAO, JanelaPrincipal
from app.gui.recursos import icone_app


def _preparar_windows() -> None:
    """Ícone próprio na barra de tarefas (sem isso o Windows agrupa com o python.exe)."""
    if sys.platform != "win32":
        return
    try:
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("Alumbra.MqttSqlServer")
    except Exception:  # noqa: BLE001 - só cosmético
        pass


def _configurar_log_arquivo() -> None:
    """Arquivo rotativo em logs/. A caixa de logs da tela recebe SÓ o sinal bridge.log, por isso nenhum
    handler do logging escreve na tela (senão cada linha apareceria duas vezes)."""
    try:
        pasta = pasta_base() / "logs"
        pasta.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(pasta / "mqtt-sqlserver.log", maxBytes=2_000_000, backupCount=5,
                                      encoding="utf-8")
    except OSError:
        return  # pasta sem permissão de escrita: segue sem arquivo de log
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    raiz = logging.getLogger()
    raiz.addHandler(handler)
    raiz.setLevel(logging.INFO)


def main(argv: list[str] | None = None) -> int:
    _preparar_windows()
    _configurar_log_arquivo()
    QGuiApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    app = QApplication(argv if argv is not None else sys.argv)
    app.setOrganizationName(ORGANIZACAO)
    app.setApplicationName(APLICACAO)
    app.setWindowIcon(icone_app())
    app.setStyle("Fusion")
    app.setFont(estilo.fonte_interface())

    config = carregar()
    bridge = PipelineBridge(config)
    janela = JanelaPrincipal(bridge, config)
    janela.show()
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
