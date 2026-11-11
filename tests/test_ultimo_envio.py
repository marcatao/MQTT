"""Aba Clientes: o Último envio só muda com leituras de contagem (`dispositivo_visto`), decidido com o usuário em
11/11/2026. PUBLISH do mosquitto -v (diag em ack/command, eventos) e diagnóstico não mexem nele
(QT_QPA_PLATFORM=offscreen)."""
from __future__ import annotations

import json
import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import Qt  # noqa: E402

from app.gui import estilo  # noqa: E402
from app.gui.clientes import COL_HA, COL_NOME, COL_ULTIMO, SEM_VALOR, RegistroClientes  # noqa: E402
from app.config import Config  # noqa: E402
from tests.fake_bridge import FakePipelineBridge, diag_exemplo  # noqa: E402
from tests.test_clientes import T0, Relogio, novo_painel  # noqa: E402,F401
from tests.test_janela import qapp  # noqa: E402,F401

DEV = "Teste_Alumbra"
CLIENTE = "NOVUS_A"
T_DADOS = f"NOVUS/{DEV}/events"
T_ACK = f"NOVUS/{DEV}/ack/command"


def publish(t: float, topico: str, cliente: str = CLIENTE) -> str:
    return f"{int(t)}: Received PUBLISH from {cliente} (d0, q1, r0, m1, '{topico}', ... (120 bytes))"


def iso(t: float) -> str:
    return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def conectado(t: float = T0, cliente: str = CLIENTE, ip: str = "192.168.100.50") -> str:
    return f"{int(t)}: New client connected from {ip}:40000 as {cliente} (p2, c1, k60, u'novus')."


def _painel(novo_painel, relogio):
    p = novo_painel(relogio=relogio, executor_arp=lambda: "", locais=set())
    p.definir_paleta(estilo.CLARA)
    return p


def _celula(painel, col, linha=0, papel=Qt.ItemDataRole.DisplayRole):
    return painel.modelo.data(painel.modelo.index(linha, col), papel)


# ---------------------------------------------------------------------- lógica pura
def test_publish_sem_dispositivo_visto_fica_sem_ultimo_envio():
    r = RegistroClientes()
    r.processar_texto("\n".join([conectado(), publish(T0 + 5, T_DADOS), publish(T0 + 6, T_ACK)]), T0)
    c = r.cliente(CLIENTE)
    assert c.mensagens == 2 and c.status == "Conectado" and c.ip == "192.168.100.50"
    assert c.topico == T_ACK            # o PUBLISH continua definindo o tópico
    assert c.ultimo_envio is None


def test_publish_de_diag_em_ack_command_nao_altera_ultimo_envio():
    r = RegistroClientes()
    r.processar_texto(publish(T0, T_DADOS), T0)
    r.dispositivo_visto(DEV, T_DADOS, T0 + 1)
    c = r.cliente(CLIENTE)
    assert c.ultimo_envio == T0 + 1
    r.processar_texto(publish(T0 + 100, T_ACK), T0 + 100)   # resposta de diag
    assert c.ultimo_envio == T0 + 1
    assert c.mensagens == 2


def test_dispositivo_visto_altera_ultimo_envio():
    r = RegistroClientes()
    r.processar_texto(publish(T0, T_DADOS), T0)
    r.dispositivo_visto(DEV, T_DADOS, T0 + 1)
    r.processar_texto(publish(T0 + 60, T_DADOS), T0 + 60)
    r.dispositivo_visto(DEV, T_DADOS, T0 + 61)
    assert r.cliente(CLIENTE).ultimo_envio == T0 + 61
    assert len(r.linhas) == 1


# ---------------------------------------------------------------------- painel (tela)
def test_painel_publish_ack_command_nao_altera_ultimo_envio(novo_painel):
    relogio = Relogio(T0 + 10)
    painel = _painel(novo_painel, relogio)
    painel.ao_saida_mosquitto("saida", "\n".join([conectado(), publish(T0 + 1, T_DADOS)]))
    painel.ao_dispositivo_visto(DEV, T_DADOS, iso(T0 + 2))
    antes = _celula(painel, COL_ULTIMO)
    assert antes != SEM_VALOR and _celula(painel, COL_HA) == "há 8 s"

    relogio.t = T0 + 200
    painel.ao_saida_mosquitto("saida", publish(T0 + 199, T_ACK))
    painel.tique()
    assert _celula(painel, COL_ULTIMO) == antes
    assert _celula(painel, COL_HA).startswith("há 3 min")      # continua atrasado: diag não conta
    assert _celula(painel, COL_NOME, papel=Qt.ItemDataRole.ForegroundRole) is not None


def test_painel_publish_em_events_sem_dispositivo_visto_nao_altera(novo_painel):
    painel = _painel(novo_painel, Relogio(T0 + 10))
    painel.ao_saida_mosquitto("saida", "\n".join([conectado(), publish(T0 + 5, T_DADOS)]))
    assert _celula(painel, COL_ULTIMO) == SEM_VALOR
    assert _celula(painel, COL_HA) == SEM_VALOR
    assert _celula(painel, COL_NOME, papel=Qt.ItemDataRole.ForegroundRole) is None
    assert painel.chip_enviando.texto() == "0 dispositivos enviando"
    dica = _celula(painel, COL_ULTIMO, papel=Qt.ItemDataRole.ToolTipRole)
    assert dica == "Última leitura de contagem recebida (diagnóstico e eventos não contam)."


def test_painel_dispositivo_visto_altera(novo_painel):
    relogio = Relogio(T0 + 10)
    painel = _painel(novo_painel, relogio)
    painel.ao_saida_mosquitto("saida", "\n".join([conectado(), publish(T0 + 5, T_DADOS)]))
    painel.ao_dispositivo_visto(DEV, T_DADOS, iso(T0 + 5))
    assert _celula(painel, COL_HA) == "há 5 s"
    assert _celula(painel, COL_ULTIMO) != SEM_VALOR
    assert painel.chip_enviando.texto() == "1 dispositivo enviando"


def test_painel_diagnostico_nao_altera_ultimo_envio(novo_painel):
    relogio = Relogio(T0 + 10)
    painel = _painel(novo_painel, relogio)
    painel.ao_saida_mosquitto("saida", "\n".join([conectado(), publish(T0 + 5, T_DADOS)]))
    painel.ao_dispositivo_visto(DEV, T_DADOS, iso(T0 + 5))
    antes = _celula(painel, COL_ULTIMO)

    relogio.t = T0 + 300
    painel.ao_diagnostico(DEV, json.dumps(diag_exemplo(DEV)))
    painel.tique()
    assert _celula(painel, COL_ULTIMO) == antes
    assert _celula(painel, COL_HA).startswith("há 4 min")


def test_diagnostico_de_device_novo_fica_sem_ultimo_envio(novo_painel):
    painel = _painel(novo_painel, Relogio(T0 + 10))
    painel.ao_diagnostico("Linha2", json.dumps(diag_exemplo("Linha2")))
    assert _celula(painel, COL_ULTIMO) == SEM_VALOR
    assert painel.chip_enviando.texto() == "0 dispositivos enviando"


def test_chip_conta_so_contagem_nos_ultimos_2_min(novo_painel):
    relogio = Relogio(T0 + 200)
    painel = _painel(novo_painel, relogio)
    linhas = [conectado(T0, "A", "192.168.100.50"), conectado(T0, "B", "192.168.100.51"),
              conectado(T0, "C", "192.168.100.52"),
              publish(T0 + 10, "NOVUS/A/events", "A"),       # contagem antiga (> 2 min)
              publish(T0 + 150, "NOVUS/B/events", "B"),      # contagem recente
              publish(T0 + 190, "NOVUS/C/ack/command", "C"),  # só diag, recente
              publish(T0 + 195, "NOVUS/A/ack/command", "A")]  # diag recente de A não renova
    painel.ao_saida_mosquitto("saida", "\n".join(linhas))
    painel.ao_dispositivo_visto("A", "NOVUS/A/events", iso(T0 + 10))
    painel.ao_dispositivo_visto("B", "NOVUS/B/events", iso(T0 + 150))
    painel.ao_diagnostico("C", json.dumps(diag_exemplo("C")))
    painel.tique()
    assert painel.enviando() == 1
    assert painel.chip_enviando.texto() == "1 dispositivo enviando"

    relogio.t = T0 + 280   # B passa de 2 min
    painel.tique()
    assert painel.chip_enviando.texto() == "0 dispositivos enviando"


# ---------------------------------------------------------------------- fake bridge
def test_fake_bridge_so_emite_dispositivo_visto_para_contagem(qapp):
    bridge = FakePipelineBridge(Config())
    vistos = []
    bridge.dispositivo_visto.connect(lambda d, t, i: vistos.append((d, t)))
    assert bridge.emitir_mensagem(DEV, T_DADOS, "contagem") is True
    for tipo in ("diag", "resposta", "evento"):
        assert bridge.emitir_mensagem(DEV, T_ACK, tipo) is False
    assert vistos == [(DEV, T_DADOS)]
    bridge.deleteLater()
