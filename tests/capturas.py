"""Gera capturas PNG da janela (claro e escuro, conectada, com logs, aba Mosquitto) usando o bridge falso.

    QT_QPA_PLATFORM=offscreen python -m tests.capturas <pasta_destino>
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
from datetime import datetime
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
             tamanho: tuple[int, int] = (1200, 860), mosquitto: bool = False) -> Path:
    pasta = tempfile.mkdtemp()
    settings = QSettings(str(Path(pasta) / "gui.ini"), QSettings.Format.IniFormat)
    config = Config()
    config.mosquitto.iniciar_automaticamente = False  # não tenta rodar o mosquitto.exe
    bridge = FakePipelineBridge(config, intervalo=0.12, sql_ok=sql_ok)
    janela = JanelaPrincipal(bridge, config, settings=settings, tema=tema, cofre=CofreFalso())
    janela.resize(*tamanho)
    janela.campo_senha.setText("fictícia")  # valor fictício, aparece mascarado
    janela.show()
    _rodar(0.3)
    janela.botao_testar.click()
    _rodar(0.5)
    if conectar:
        janela.botao_conectar.click()
        _rodar(2.0)
        # o ciclo real é de 1 min: simula a última gravação ok (há alguns segundos) para ela aparecer na captura
        bridge.ultima_gravacao_ok.emit(datetime.fromtimestamp(time.time() - 18).strftime("%Y-%m-%dT%H:%M:%S"))
        _rodar(0.5)
    if mosquitto:
        _simular_mosquitto(janela)
        janela.abas.setCurrentIndex(1)
        _rodar(0.4)
    nome = (f"janela-{tema}{'-conectada' if conectar else ''}{'' if sql_ok else '-falha-sql'}"
            f"{'-mosquitto' if mosquitto else ''}{f'-{tamanho[0]}' if tamanho[0] < 1200 else ''}.png")
    caminho = destino / nome
    janela.grab().save(str(caminho))
    janela.close()
    return caminho


SAIDA_MOSQUITTO = """1730650000: mosquitto version 2.0.18 starting
1730650000: Using default config.
1730650000: Starting in local only mode. Connections will only be possible from clients running on this machine.
1730650000: Create a configuration file which defines a listener to allow remote access.
1730650000: Opening ipv4 listen socket on port 1883.
1730650000: Opening ipv6 listen socket on port 1883.
1730650000: mosquitto version 2.0.18 running
1730650012: New connection from 192.168.100.50:51544 on port 1883.
1730650012: New client connected from 192.168.100.50:51544 as NOVUS_Teste_Alumbra (p2, c1, k60).
1730650012: No will message specified.
1730650012: Sending CONNACK to NOVUS_Teste_Alumbra (0, 0)
1730650014: New connection from 127.0.0.1:51551 on port 1883.
1730650014: New client connected from 127.0.0.1:51551 as mqtt-sqlserver-4f2a (p5, c1, k60).
1730650014: Received SUBSCRIBE from mqtt-sqlserver-4f2a
1730650014: 	NOVUS/Teste_Alumbra/# (QoS 2)
1730650014: Sending SUBACK to mqtt-sqlserver-4f2a
1730650020: Received PUBLISH from NOVUS_Teste_Alumbra (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/events', ... (128 bytes))
1730650020: Sending PUBLISH to mqtt-sqlserver-4f2a (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/events', ... (128 bytes))
1730650030: Received PUBLISH from NOVUS_Teste_Alumbra (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/events', ... (128 bytes))
1730650030: Sending PUBLISH to mqtt-sqlserver-4f2a (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/events', ... (128 bytes))
1730650074: Received PINGREQ from NOVUS_Teste_Alumbra
1730650074: Sending PINGRESP to NOVUS_Teste_Alumbra"""


def _simular_mosquitto(janela) -> None:
    """Saída simulada no estilo do `mosquitto -v` (o .exe real só existe no Windows do usuário)."""
    console = janela.mosquitto
    console.config.pasta = "C:\\Program Files\\mosquitto168"
    janela.painel_mosquitto.caixa.limpar()
    console.saida.emit("prompt", console.prompt())
    console.estado_alterado.emit("rodando", "Rodando (PID 4812)")
    console.estado = "rodando"
    console.saida.emit("saida", SAIDA_MOSQUITTO)
    comando = 'mosquitto_sub -h localhost -t "#" -v'
    janela.painel_mosquitto.campo_comando.setText(comando)
    console.saida.emit("prompt", console.prompt(comando))
    console.saida.emit("saida", 'NOVUS/Teste_Alumbra/events {"pid":51387408,"device_id":"Teste_Alumbra",'
                                '"channels":{"timestamp":1793386748,"chd1_value":67,"chd2_value":119,"chd3_value":0}}')
    console.saida.emit("info", "^C")
    console.saida.emit("info", "Comando interrompido.")


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
        capturar(destino, "claro", mosquitto=True),
        capturar(destino, "escuro", mosquitto=True),
        capturar(destino, "escuro", mosquitto=True, tamanho=(980, 640)),
    ):
        print(caminho)


if __name__ == "__main__":
    main()
