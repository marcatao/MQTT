"""Teste de fumaça da janela com o bridge falso (roda sem tela: QT_QPA_PLATFORM=offscreen)."""
from __future__ import annotations

import os
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QCoreApplication, QSettings  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui.janela import JanelaPrincipal, validar_broker, validar_topico  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402

SENHA_FICTICIA = "senha-de-teste"


class CofreFalso:
    def __init__(self, inicial: dict | None = None):
        self.dados = dict(inicial or {})

    def ler(self, usuario):
        return self.dados.get(usuario), None

    def salvar(self, usuario, senha):
        self.dados[usuario] = senha
        return None

    def apagar(self, usuario):
        self.dados.pop(usuario, None)
        return None


@pytest.fixture(scope="session")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


def esperar(condicao, timeout=3.0):
    fim = time.monotonic() + timeout
    while time.monotonic() < fim:
        QCoreApplication.processEvents()
        if condicao():
            return True
        time.sleep(0.01)
    QCoreApplication.processEvents()
    return condicao()


@pytest.fixture
def montar(qapp, tmp_path):
    criadas = []

    def _montar(cofre=None, **kw):
        settings = QSettings(str(tmp_path / "gui.ini"), QSettings.Format.IniFormat)
        config = Config()
        config.mosquitto.iniciar_automaticamente = False  # não tenta rodar o mosquitto.exe nos testes
        bridge = FakePipelineBridge(config, **kw)
        janela = JanelaPrincipal(bridge, config, settings=settings, tema="claro", cofre=cofre or CofreFalso())
        janela.show()
        criadas.append(janela)
        return janela, bridge, settings

    yield _montar
    for j in criadas:
        j.close()


def test_padroes_preenchidos(montar):
    janela, bridge, _ = montar()
    assert janela.windowTitle() == "Integrador Alumbra MQTT + SQL + Mosquitto"
    assert janela.campo_broker.text() == "192.168.100.162"
    assert janela.campo_porta.value() == 1883
    assert janela.campo_topico.text() == "NOVUS/Teste_Alumbra/#"
    assert janela.seletor_qos.valor() == 2
    assert janela.campo_servidor.text() == "TERRA\\SOLO"
    assert janela.campo_banco.text() == "MQTT"
    assert janela.campo_usuario.text() == "mqtt"
    assert janela.seletor_cripto.valor() == "Optional"
    assert not janela.opcao_confiar.isChecked()
    assert bridge.chamadas[0][0] == "definir_sql"


def test_conectar_logs_contadores_desconectar(montar):
    janela, bridge, settings = montar()
    assert janela.status.texto() == "Desconectado"
    janela.seletor_qos.definir(1)
    janela.botao_conectar.click()

    assert ("definir_sql",) == bridge.chamadas[-2][:1]
    assert bridge.chamadas[-1] == ("conectar", "192.168.100.162", 1883, "NOVUS/Teste_Alumbra/#", 1)
    assert not janela.botao_conectar.isEnabled()
    assert not janela.campo_broker.isEnabled()

    assert esperar(lambda: janela.estado == "conectado")
    assert janela.status.texto() == "Conectado"
    assert janela.botao_conectar.text() == "Desconectar"
    assert janela.botao_conectar.isEnabled()

    assert esperar(lambda: janela.contador_recebidas.valor >= 5)
    assert esperar(lambda: janela.logs.linhas >= 5)
    texto = janela.logs.texto()
    assert "Conectado. Assinando" in texto
    assert janela.contador_gravadas.valor > 0
    assert janela.contador_recebidas._valor.text() != "0"

    janela.botao_conectar.click()
    assert esperar(lambda: janela.estado == "desconectado")
    assert janela.botao_conectar.text() == "Conectar"
    assert janela.campo_broker.isEnabled()
    assert int(settings.value("mqtt/qos")) == 1


def test_falha_de_conexao_mostra_erro(montar):
    janela, _, _ = montar(falhar_conexao=True)
    janela.botao_conectar.click()
    assert esperar(lambda: janela.estado == "erro")
    assert janela.status.texto() == "Erro"
    assert janela.botao_conectar.text() == "Conectar"
    assert esperar(lambda: "conexão recusada" in janela.logs.texto())


def test_validacao_impede_conectar(montar):
    janela, bridge, _ = montar()
    janela.campo_topico.setText("NOVUS/#/x")
    janela.botao_conectar.click()
    assert not any(c[0] == "conectar" for c in bridge.chamadas)
    assert janela.erro_validacao.isVisible()
    assert janela.estado == "desconectado"
    assert validar_broker("mqtt://x") and validar_broker("") and not validar_broker("192.168.100.162")
    assert not validar_topico("NOVUS/Teste_Alumbra/#") and not validar_topico("a/+/b")
    assert validar_topico("a/b#") and validar_topico("a+/b")


@pytest.mark.parametrize("sucesso", [True, False])
def test_testar_sql(montar, sucesso):
    janela, bridge, settings = montar(sql_ok=sucesso)
    janela.campo_senha.setText(SENHA_FICTICIA)
    janela.botao_testar.click()
    assert bridge.chamadas[-1][0] == "testar_sql"
    cfg = bridge.chamadas[-1][1]
    assert (cfg.servidor, cfg.banco, cfg.usuario, cfg.senha) == ("TERRA\\SOLO", "MQTT", "mqtt", SENHA_FICTICIA)
    assert cfg.criptografar == "Optional" and cfg.trust_server_certificate is False
    assert not janela.botao_testar.isEnabled()
    assert janela.botao_testar.text() == "Testando…"

    assert esperar(lambda: janela.botao_testar.isEnabled())
    assert janela.resultado_sql.estado == ("ok" if sucesso else "falha")
    assert janela.resultado_sql.isVisible()
    log = janela.logs.texto()
    assert ("Conectado a TERRA\\SOLO" in log) if sucesso else ("Falha no login" in log and " ERRO " in log)
    assert SENHA_FICTICIA not in log
    # A senha nunca vai para o QSettings.
    settings.sync()
    assert all(SENHA_FICTICIA not in str(settings.value(k)) for k in settings.allKeys())


def test_autenticacao_windows_desabilita_usuario_senha(montar):
    janela, bridge, _ = montar()
    janela.seletor_autenticacao.botoes[1].click()  # Windows
    assert not janela.campo_usuario.isEnabled() and not janela.campo_senha.isEnabled()
    cfg = janela.sql_config()
    assert cfg.autenticacao_windows and cfg.usuario == "" and cfg.senha == ""
    janela.seletor_autenticacao.botoes[0].click()  # SQL Server
    assert janela.campo_usuario.isEnabled() and janela.campo_senha.isEnabled()


def test_lembrar_senha_usa_cofre(montar, tmp_path):
    cofre = CofreFalso()
    janela, bridge, settings = montar(cofre=cofre)
    janela.campo_senha.setText(SENHA_FICTICIA)
    janela.opcao_lembrar.setChecked(True)
    janela.botao_testar.click()
    assert cofre.dados == {"mqtt": SENHA_FICTICIA}
    assert esperar(lambda: janela.botao_testar.isEnabled())
    janela.close()

    # Reabrindo: a senha volta do cofre, não do QSettings.
    janela2, _, _ = montar(cofre=cofre)
    assert janela2.opcao_lembrar.isChecked()
    assert janela2.campo_senha.text() == SENHA_FICTICIA
    janela2.opcao_lembrar.setChecked(False)
    janela2.close()
    assert cofre.dados == {}


def test_logs_lote_limite_e_rolagem(montar):
    janela, _, _ = montar()
    painel = janela.logs
    for i in range(6000):
        painel.adicionar("12:00:00", "INFO", f"linha {i}")
    QCoreApplication.processEvents()
    painel.descarregar()
    assert painel.caixa.blockCount() <= 5000
    assert painel.caixa.toPlainText().splitlines()[-1].endswith("linha 5999")
    # Usuário sobe a rolagem: pausa e mostra o aviso; novas linhas não puxam para o fim.
    barra = painel.caixa.verticalScrollBar()
    barra.setValue(0)
    assert not painel.seguindo and painel.chip_pausa.isVisibleTo(janela)
    painel.adicionar("12:00:01", "ERRO", "nova")
    painel.descarregar()
    assert barra.value() == 0
    painel.ir_para_fim()
    assert painel.seguindo and barra.value() == barra.maximum()
    painel.limpar()
    assert painel.linhas == 0


def test_fechar_chama_encerrar(montar):
    janela, bridge, _ = montar()
    janela.close()
    assert ("encerrar", 8.0) in bridge.chamadas
