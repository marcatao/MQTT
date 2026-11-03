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
