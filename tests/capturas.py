"""Gera capturas PNG da janela (claro e escuro, conectada, com logs, abas Mosquitto e Clientes, janela de erros)
usando o bridge falso.

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


ARP_SIMULADO = """
Interface: 192.168.100.162 --- 0x7
  Endereço IP           Endereço físico       Tipo
  192.168.100.1         00-1a-2b-3c-4d-5e     dinâmico
  192.168.100.50        a4-cf-12-8e-3b-71     dinâmico
  192.168.100.57        a4-cf-12-8e-40-0c     dinâmico
  192.168.100.255       ff-ff-ff-ff-ff-ff     estático
"""


def _saida_clientes(agora: float) -> str:
    """Três dispositivos NOVUS (um parado há 3 min), o próprio app e um mosquitto_sub, no formato do mosquitto -v."""
    t = int(agora)
    linhas = [
        f"{t - 900}: New connection from 192.168.100.50:51544 on port 1883.",
        f"{t - 900}: New client connected from 192.168.100.50:51544 as NOVUS_A1B2C3 (p2, c1, k60, u'novus').",
        f"{t - 880}: New client connected from 192.168.100.57:49152 as NOVUS_D4E5F6 (p2, c1, k60, u'novus').",
        f"{t - 870}: New client connected from 192.168.100.71:50210 as fieldlogger-07 (p2, c1, k60).",
        f"{t - 860}: New client connected from 127.0.0.1:51551 as mqtt-sqlserver-4f2a (p5, c1, k60).",
        f"{t - 860}: Received SUBSCRIBE from mqtt-sqlserver-4f2a",
        f"{t - 300}: New client connected from 127.0.0.1:51600 as mosq-Zx81kPq (p2, c1, k60).",
        f"{t - 300}: Received SUBSCRIBE from mosq-Zx81kPq",
    ]
    for k in range(86):  # um envio a cada ~10 s
        linhas.append(f"{t - 860 + k * 10}: Received PUBLISH from NOVUS_A1B2C3 (d0, q0, r0, m0, "
                      f"'NOVUS/Teste_Alumbra/events', ... (128 bytes))")
    for k in range(40):
        linhas.append(f"{t - 600 + k * 10 + 3}: Received PUBLISH from NOVUS_D4E5F6 (d0, q0, r0, m0, "
                      f"'NOVUS/Linha2/events', ... (131 bytes))")
    for k in range(3):  # caso real: confirmação de comando num tópico que o NXperience gravou com "Ï"
        linhas.append(f"{t - 400 + k * 120}: Received PUBLISH from NOVUS_A1B2C3 (d0, q1, r0, m{k + 20}, "
                      f"'NOÏUS/Teste_Alumbra/ack/command', ... (212 bytes))")
    for k in range(9):  # parou há 3 min
        linhas.append(f"{t - 830 + k * 70}: Received PUBLISH from fieldlogger-07 (d0, q1, r0, m{k + 1}, "
                      f"'NOVUS/Teste_Alumbra/fieldlogger', ... (96 bytes))")
    return "\n".join(sorted(linhas, key=lambda l: int(l.split(":", 1)[0])))


def capturar_clientes(destino: Path, tema: str) -> Path:
    pasta = tempfile.mkdtemp()
    settings = QSettings(str(Path(pasta) / "gui.ini"), QSettings.Format.IniFormat)
    config = Config()
    config.mosquitto.iniciar_automaticamente = False
    config.mqtt.topico = "NOVUS/#"  # assinatura do usuário: o NOÏUS/… fica de fora
    bridge = FakePipelineBridge(config)
    janela = JanelaPrincipal(bridge, config, settings=settings, tema=tema, cofre=CofreFalso(),
                             executor_arp=lambda: ARP_SIMULADO, enderecos_locais={"192.168.100.162"})
    janela.resize(1440, 900)  # tamanho inicial padrão numa tela Full HD
    janela.show()
    _rodar(0.3)
    agora = time.time()
    console = janela.mosquitto
    console.estado_alterado.emit("rodando", "Rodando (PID 4812)")
    console.saida.emit("saida", _saida_clientes(agora))
    iso = lambda t: datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")  # noqa: E731
    bridge.emitir_dispositivo("Teste_Alumbra", "NOVUS/Teste_Alumbra/events", iso(agora - 10))
    bridge.emitir_dispositivo("Linha2_Forno", "NOVUS/Linha2/events", iso(agora - 7))
    bridge.emitir_dispositivo("Caldeira_03", "NOVUS/Teste_Alumbra/fieldlogger", iso(agora - 180))
    bridge.status_alterado.emit("conectado", "192.168.100.162:1883 · NOVUS/#")
    for device, sn, fw, rssi, fila, ip, deriva in (
            ("Teste_Alumbra", "21004417", "1.23", "55", 0, [192, 168, 100, 50], 2),     # rssi do manual: "55"
            ("Linha2_Forno", "21004502", "1.23", "-71", 37, [192, 168, 100, 57], 194),  # fila no aparelho
            ("Caldeira_03", "20117730", "1.18", "-82", 0, [192, 168, 100, 71], -41)):
        bridge.emitir_canais(device, ["chd1", "chd2", "chd3"])
        bridge.emitir_diag(device, {"error": 0, "title": f"DigiRail {device}", "location": "Fábrica — galpão 2",
                                    "curr_timestamp": int(agora) + deriva, "fw_v": fw, "mqtt_queue": fila,
                                    "sn": sn, "curr_rssi": rssi, "min_rssi": "-46", "max_rssi": "-87",
                                    "avg_rssi": "-58", "ipv4": ip})
    bridge.emitir_resposta("Teste_Alumbra", "reset_counters", True, "Contadores zerados (chd2).")
    bridge.emitir_resposta("Teste_Alumbra", "diag", False,
                           "Sem resposta do dispositivo em 15 s (esperada em NOVUS/Teste_Alumbra/ack/command).")
    bridge.contadores.emit(1284, 21, 3)
    janela.abas.setCurrentIndex(2)
    _rodar(1.5)  # consulta ARP (thread) e tique
    janela.painel_clientes.tique()
    _rodar(0.2)
    caminho = destino / f"janela-{tema}-clientes.png"
    janela.grab().save(str(caminho))
    if tema == "claro":
        _capturar_dialogo_zerar(janela, destino)
    _capturar_dialogo_topicos(janela, destino, tema)
    janela.close()
    return caminho


def _capturar_dialogo_topicos(janela, destino: Path, tema: str) -> None:
    """Diálogo "Tópicos" do Teste_Alumbra: o NOÏUS listado em "não ouvidos" e já usado na confirmação de comando
    (indicador vermelho "não ouvido" até salvar)."""
    painel = janela.painel_clientes

    def executar(dlg):
        _topico, botao = dlg.itens_nao_ouvidos[0]
        acao = next(a for a in botao.menu().actions() if a.data() == "ack_command")
        acao.trigger()
        dlg.show()
        dlg.adjustSize()
        _rodar(0.3)
        dlg.campos["ack_command"].deselect()  # a ativação da janela seleciona o texto do campo com foco
        _rodar(0.1)
        dlg.grab().save(str(destino / f"dialogo-topicos-{tema}.png"))
        dlg.close()
        return 0  # não salva

    painel.executar_dialogo = executar
    painel.abrir_topicos("Teste_Alumbra")


def _capturar_dialogo_zerar(janela, destino: Path) -> None:
    """Diálogo "Zerar contadores" com chd2 marcado (os canais vistos vêm primeiro e destacados)."""
    painel = janela.painel_clientes
    capturas: list[Path] = []

    def executar(dlg):
        dlg.caixas[2].setChecked(True)
        dlg.show()
        _rodar(0.3)
        caminho = destino / "dialogo-zerar-contadores.png"
        dlg.grab().save(str(caminho))
        capturas.append(caminho)
        dlg.close()
        return 0  # não envia

    painel.executar_dialogo = executar
    painel.zerar("Linha2_Forno")


ERROS_SIMULADOS = [
    ("payload", "Leitura incompleta em NOVUS/Teste_Alumbra/events: falta ts_unix. Enviada para a contingência.",
     "NOVUS/Teste_Alumbra/events", "Campos obrigatórios: pid, device_id, ts_unix.",
     '{"pid":51387408,"device_id":"Teste_Alumbra","channels":{"chd1_value":67,"chd2_value":119,"chd3_value":0}}'),
    ("sql", "Falha ao gravar 3 leituras no SQL Server: tempo de login esgotado.", None,
     "[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired (servidor TERRA\\SOLO)", None),
    ("contingencia", "Leituras guardadas na contingência: contingencia/20261103.jsonl (3 linhas).", None,
     "O SQL Server não respondeu; serão gravadas quando o reprocessamento existir.", None),
    ("payload", "Payload inválido: JSON truncado (posição 118).", "NOVUS/Linha2/events",
     "Expecting ',' delimiter: line 1 column 118 (char 117)",
     '{"pid":51387409,"device_id":"Linha2_Forno","channels":{"timestamp":17933867'),
    ("fila", "Fila de processamento cheia: 1 mensagem descartada.", "NOVUS/Teste_Alumbra/events", None, None),
    ("mqtt", "Assinatura recusada pelo broker para NOVUS/Restrito/#.", "NOVUS/Restrito/#",
     "SUBACK com código 0x87 (não autorizado)", None),
    ("comando", "Falha ao zerar contadores chd2 em Linha2_Forno: sem resposta do dispositivo.",
     "NOVUS/Linha2_Forno/command", "Nenhuma resposta em NOVUS/Linha2_Forno/ack/command em 15 s.",
     '{"timestamp":1793386748,"desired":{"reset_counters":{"reset_chd2":1}}}'),
]


def capturar_erros(destino: Path, tema: str) -> Path:
    pasta = tempfile.mkdtemp()
    settings = QSettings(str(Path(pasta) / "gui.ini"), QSettings.Format.IniFormat)
    config = Config()
    config.mosquitto.iniciar_automaticamente = False
    config.gravacao.pasta_logs = pasta  # arquivo do dia vazio: só os erros da sessão
    bridge = FakePipelineBridge(config)
    janela = JanelaPrincipal(bridge, config, settings=settings, tema=tema, cofre=CofreFalso(),
                             executor_arp=lambda: "", enderecos_locais=set())
    janela.show()
    agora = time.time()
    for i, (cat, msg, topico, detalhe, payload) in enumerate(ERROS_SIMULADOS):
        quando = datetime.fromtimestamp(agora - 3600 + i * 517).strftime("%Y-%m-%dT%H:%M:%S")
        bridge.emitir_erro(cat, msg, topico, detalhe, payload, quando=quando)
    bridge.contadores.emit(1284, 21, len(ERROS_SIMULADOS))
    _rodar(0.4)
    janela.abrir_erros()
    _rodar(0.5)
    dlg = janela.janela_erros
    # seleciona o primeiro (mais antigo) erro de payload, que tem JSON completo
    for r in range(dlg.filtro.rowCount()):
        reg = dlg.modelo.registro(dlg.filtro.mapToSource(dlg.filtro.index(r, 0)).row())
        if reg.payload and reg.payload.endswith("}}"):
            dlg.tabela.selectRow(r)
            break
    _rodar(0.3)
    caminho = destino / f"erros-{tema}.png"
    dlg.grab().save(str(caminho))
    caminho_card = destino / f"janela-{tema}-card-erros.png"
    janela.grab().save(str(caminho_card))
    dlg.close()
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
        capturar(destino, "claro", mosquitto=True),
        capturar(destino, "escuro", mosquitto=True),
        capturar(destino, "escuro", mosquitto=True, tamanho=(980, 640)),
        capturar_clientes(destino, "claro"),
        capturar_clientes(destino, "escuro"),
        capturar_erros(destino, "claro"),
        capturar_erros(destino, "escuro"),
    ):
        print(caminho)


if __name__ == "__main__":
    main()
