"""Ponto de entrada: `python -m app.main` abre a janela MQTT → SQL Server.

    --auto-conectar   conecta sozinho com os valores salvos (usado pelo vigia ao reiniciar o programa)
"""
from __future__ import annotations

import argparse
import logging
import sys
from logging.handlers import RotatingFileHandler

from PySide6.QtCore import QSettings, QTimer, Qt
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import QApplication

from app.bridge import PipelineBridge
from app.config import Config, carregar, pasta_base
from app.gui import estilo
from app.gui.janela import APLICACAO, ORGANIZACAO, JanelaPrincipal
from app.gui.recursos import icone_app
from app.gui.vigia import ARG_AUTO


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


def ler_argumentos(argv: list[str]) -> argparse.Namespace:
    """Só as opções do app; o resto (opções do Qt, como -platform) fica para o QApplication."""
    p = argparse.ArgumentParser(prog="MQTT-SQLServer", add_help=False)
    p.add_argument(ARG_AUTO, dest="auto_conectar", action="store_true")
    args, _ = p.parse_known_args(argv[1:])
    return args


def montar_janela(argv: list[str], config: Config | None = None, bridge_cls=PipelineBridge,
                  settings: QSettings | None = None, **kw_janela) -> JanelaPrincipal:
    """Cria bridge e janela, mostra e, com --auto-conectar, agenda a conexão automática.

    A janela já chama bridge.definir_sql no construtor; auto_conectar envia o SQL de novo e só então conecta.
    """
    args = ler_argumentos(argv)
    config = config or carregar()
    bridge = bridge_cls(config)
    janela = JanelaPrincipal(bridge, config, settings=settings, **kw_janela)
    janela.show()
    if args.auto_conectar:
        logging.getLogger("app.main").info("Início com %s: conectando com os valores salvos.", ARG_AUTO)
        QTimer.singleShot(0, janela.auto_conectar)
    return janela


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv)
    _preparar_windows()
    _configurar_log_arquivo()
    QGuiApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)
    app = QApplication(argv)
    app.setOrganizationName(ORGANIZACAO)
    app.setApplicationName(APLICACAO)
    app.setWindowIcon(icone_app())
    app.setStyle("Fusion")
    app.setFont(estilo.fonte_interface())

    janela = montar_janela(argv)  # noqa: F841 - mantém a referência viva até o fim do exec()
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
