"""Testes do adaptador Qt (sinais do PipelineBridge)."""
from __future__ import annotations

import os
import time

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
QtWidgets = pytest.importorskip("PySide6.QtWidgets")

from app.config import Config, SqlServerConfig  # noqa: E402
from app.bridge import PipelineBridge  # noqa: E402
from app import bridge as modulo_bridge  # noqa: E402


@pytest.fixture(scope="module")
def app():
    # QApplication (não QCoreApplication): os testes da GUI no mesmo processo precisam dela
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication([])


def _esperar(app, cond, timeout=5.0):
    fim = time.monotonic() + timeout
    while time.monotonic() < fim:
        app.processEvents()
        if cond():
            return True
        time.sleep(0.02)
    return False


def test_sinais_de_erro_de_validacao(app, tmp_path):
    cfg = Config()
    cfg.sqlserver = SqlServerConfig(servidor="", banco="")
    cfg.gravacao.pasta_contingencia = str(tmp_path)
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    b = PipelineBridge(cfg)
    status, logs = [], []
    b.status_alterado.connect(lambda e, d: status.append((e, d)))
    b.log.connect(lambda h, n, m: logs.append((h, n, m)))
    b.conectar("", 1883, "NOVUS/#", 2)
    assert _esperar(app, lambda: status)
    assert status[0] == ("erro", "Informe o endereço do broker.")
    hora, nivel, msg = logs[0]
    assert len(hora) == 8 and hora[2] == ":" and nivel == "ERRO"
    b.encerrar(2)


def test_testar_sql_nao_bloqueia_e_emite_resultado(app, monkeypatch):
    def lento(sql):
        time.sleep(0.2)
        return True, f"✓ Conectado.\n  Banco: {sql.banco}"
    monkeypatch.setattr(modulo_bridge.sql_conexao, "testar_conexao", lento)
    b = PipelineBridge(Config())
    resultados = []
    b.teste_sql_concluido.connect(lambda ok, m: resultados.append((ok, m)))
    inicio = time.monotonic()
    b.testar_sql(SqlServerConfig(servidor="X", banco="MQTT", senha="segredo"))
    assert time.monotonic() - inicio < 0.1
    assert _esperar(app, lambda: resultados)
    assert resultados == [(True, "✓ Conectado.\n  Banco: MQTT")]
    b.encerrar(1)


def test_testar_sql_excecao_vira_falso(app, monkeypatch):
    def quebra(sql):
        raise RuntimeError("boom")
    monkeypatch.setattr(modulo_bridge.sql_conexao, "testar_conexao", quebra)
    b = PipelineBridge(Config())
    resultados = []
    b.teste_sql_concluido.connect(lambda ok, m: resultados.append((ok, m)))
    b.testar_sql(SqlServerConfig())
    assert _esperar(app, lambda: resultados)
    assert resultados[0][0] is False and "boom" in resultados[0][1]
    b.encerrar(1)


def test_definir_sql_repassa_copia(app):
    cfg = Config()
    b = PipelineBridge(cfg)
    novo = SqlServerConfig(servidor="S", banco="B")
    b.definir_sql(novo)
    assert cfg.sqlserver == novo and cfg.sqlserver is not novo
    b.encerrar(1)
