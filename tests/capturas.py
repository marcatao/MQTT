"""Gera capturas PNG da janela (claro e escuro, conectada, com logs) usando o bridge falso.

    QT_QPA_PLATFORM=offscreen python -m tests.capturas <pasta_destino>
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QCoreApplication, QSettings  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui import estilo  # noqa: E402
from app.gui.janela import JanelaPrincipal  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_janela import CofreFalso  # noqa: E402


def _rodar(segundos: float) -> None:
    fim = time.monotonic() + segundos
    while time.monotonic() < fim:
        QCoreApplication.processEvents()
        time.sleep(0.01)


def capturar(destino: Path, tema: str, conectar: bool = True, sql_ok: bool = True,
             tamanho: tuple[int, int] = (1200, 860)) -> Path:
    pasta = tempfile.mkdtemp()
    settings = QSettings(str(Path(pasta) / "gui.ini"), QSettings.Format.IniFormat)
    bridge = FakePipelineBridge(Config(), intervalo=0.12, sql_ok=sql_ok)
    janela = JanelaPrincipal(bridge, Config(), settings=settings, tema=tema, cofre=CofreFalso())
    janela.resize(*tamanho)
    janela.campo_senha.setText("fictícia")  # valor fictício, aparece mascarado
    janela.show()
    _rodar(0.3)
    janela.botao_testar.click()
    _rodar(0.5)
    if conectar:
        janela.botao_conectar.click()
        _rodar(2.5)
    nome = f"janela-{tema}{'-conectada' if conectar else ''}{'' if sql_ok else '-falha-sql'}.png"
    caminho = destino / nome
    janela.grab().save(str(caminho))
    janela.close()
    return caminho


def main() -> None:
    destino = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    destino.mkdir(parents=True, exist_ok=True)
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    app.setFont(estilo.fonte_interface())
    for caminho in (
        capturar(destino, "claro"),
        capturar(destino, "escuro"),
        capturar(destino, "claro", conectar=False, sql_ok=False, tamanho=(980, 640)),
    ):
        print(caminho)


if __name__ == "__main__":
    main()
