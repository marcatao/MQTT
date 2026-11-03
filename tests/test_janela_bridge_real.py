"""Fumaça da janela com o PipelineBridge REAL: broker amqtt local → JSON publicado → contadores e logs na tela.

Sem SQL Server configurado (servidor vazio): o pipeline grava em contingência numa pasta temporária.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
pytest.importorskip("amqtt.broker")

import json  # noqa: E402

from PySide6.QtCore import QSettings  # noqa: E402

from app.bridge import PipelineBridge  # noqa: E402
from app.config import Config  # noqa: E402
from app.gui.janela import JanelaPrincipal  # noqa: E402
from tests.test_integracao_mqtt import _publicar, broker  # noqa: E402,F401 - fixture do broker amqtt
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401


def test_janela_com_bridge_real(qapp, broker, tmp_path):  # noqa: F811
    porta = broker
    config = Config()
    config.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    config.gravacao.pasta_logs = str(tmp_path / "logs")
    config.mosquitto.iniciar_automaticamente = False  # não tenta rodar o mosquitto.exe nos testes
    bridge = PipelineBridge(config)
    settings = QSettings(str(tmp_path / "gui.ini"), QSettings.Format.IniFormat)
    janela = JanelaPrincipal(bridge, config, settings=settings, tema="claro", cofre=CofreFalso())
    janela.show()
    try:
        janela.campo_broker.setText("127.0.0.1")
        janela.campo_porta.setValue(porta)
        janela.campo_servidor.setText("")  # sem SQL Server: vai para contingência
        janela.botao_conectar.click()

        assert esperar(lambda: janela.estado == "conectado", 10), janela.logs.texto()
        assert janela.status.texto() == "Conectado"
        assert janela.botao_conectar.text() == "Desconectar"

        payload = json.dumps({"dispositivo": "logbox01", "temperatura": 23.5, "umidade": 61})
        _publicar(porta, [("NOVUS/Teste_Alumbra/logbox01", payload)])

        assert esperar(lambda: janela.contador_recebidas.valor >= 1, 10), janela.logs.texto()
        assert esperar(lambda: janela.logs.linhas >= 2, 5)

        janela.botao_conectar.click()
        assert esperar(lambda: janela.estado == "desconectado", 10), janela.logs.texto()
    finally:
        janela.close()
    texto = janela.logs.texto()
    assert "NOVUS/Teste_Alumbra/logbox01" in texto
    assert list((tmp_path / "contingencia").glob("*.jsonl")), "registro não foi para a contingência"


def test_clientes_e_erros_com_bridge_real(qapp, broker, tmp_path):  # noqa: F811
    """dispositivo_visto → aba Clientes; erro_registrado → janela de erros (e o arquivo do dia sem duplicar)."""
    import time

    porta = broker
    config = Config()
    config.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    config.gravacao.pasta_logs = str(tmp_path / "logs")
    config.mosquitto.iniciar_automaticamente = False
    bridge = PipelineBridge(config)
    settings = QSettings(str(tmp_path / "gui.ini"), QSettings.Format.IniFormat)
    janela = JanelaPrincipal(bridge, config, settings=settings, tema="claro", cofre=CofreFalso(),
                             executor_arp=lambda: "", enderecos_locais=set())
    janela.show()
    try:
        janela.campo_broker.setText("127.0.0.1")
        janela.campo_porta.setValue(porta)
        janela.campo_servidor.setText("")
        janela.botao_conectar.click()
        assert esperar(lambda: janela.estado == "conectado", 10), janela.logs.texto()

        completo = json.dumps({"pid": 51387408, "device_id": "Teste_Alumbra",
                               "channels": {"timestamp": int(time.time()), "chd1_value": 67,
                                            "chd2_value": 119, "chd3_value": 0}})
        incompleto = json.dumps({"device_id": "Teste_Alumbra", "channels": {"chd1_value": 1}})  # sem pid e ts
        _publicar(porta, [("NOVUS/Teste_Alumbra/events", completo), ("NOVUS/Teste_Alumbra/events", incompleto)])

        painel = janela.painel_clientes
        assert esperar(lambda: any(l[0] == "Teste_Alumbra" for l in painel.linhas_visiveis()), 10), \
            janela.logs.texto()
        linha = next(l for l in painel.linhas_visiveis() if l[0] == "Teste_Alumbra")
        assert linha[5] == "NOVUS/Teste_Alumbra/events"

        dlg = janela.janela_erros
        assert esperar(lambda: janela.contador_erros.valor >= 1 and dlg.total >= 1, 10), janela.logs.texto()
        erros_sessao = dlg.total
        assert erros_sessao == janela.contador_erros.valor  # um registro para cada +1 no contador
        janela.contador_erros.clicado.emit()
        assert dlg.isVisible()
        assert esperar(lambda: not dlg._carregando, 5)
        assert list((tmp_path / "logs").glob("erros_*.jsonl")), "o core não gravou o arquivo do dia"
        assert dlg.total == erros_sessao  # o arquivo do dia tem os mesmos: nada duplicado
        r = dlg.lista.itens[-1]
        assert r.topico == "NOVUS/Teste_Alumbra/events" and r.payload and "chd1_value" in r.payload

        janela.botao_conectar.click()
        assert esperar(lambda: janela.estado == "desconectado", 10)
    finally:
        janela.close()
