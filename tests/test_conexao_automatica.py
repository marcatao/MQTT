"""Conexão automática ao abrir o programa (`[inicio] conectar_automaticamente`) e com --auto-conectar.

Roda sem tela (QT_QPA_PLATFORM=offscreen) com o bridge falso. Nenhuma senha real: só a fictícia dos testes.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings  # noqa: E402

from app.config import Config, carregar  # noqa: E402
from app.gui.janela import AVISO_SEM_SENHA, MSG_AUTO_ABERTURA, MSG_AUTO_REINICIO  # noqa: E402
from app.gui.vigia import ARG_AUTO  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_janela import SENHA_FICTICIA, CofreFalso, esperar, qapp  # noqa: E402,F401


@pytest.fixture
def abrir(qapp, tmp_path):  # noqa: F811
    """Abre o programa como o app/main.py faz (montar_janela), com settings e cofre isolados."""
    from app.main import montar_janela

    criadas = []

    def _abrir(argv=("app",), auto=True, lembrar=True, cofre=None, servidor="TERRA\\SOLO",
               broker="10.0.0.5", topico="NOVUS/x/#", config=None):
        config = config or Config()
        config.inicio.conectar_automaticamente = auto
        config.mosquitto.iniciar_automaticamente = False  # não tenta rodar o mosquitto.exe nos testes
        settings = QSettings(str(tmp_path / f"gui{len(criadas)}.ini"), QSettings.Format.IniFormat)
        settings.setValue("mqtt/broker", broker)
        settings.setValue("mqtt/porta", 1884)
        settings.setValue("mqtt/topico", topico)
        settings.setValue("mqtt/qos", 1)
        settings.setValue("sql/servidor", servidor)
        settings.setValue("sql/lembrar_senha", lembrar)
        settings.sync()
        if cofre is None:
            cofre = CofreFalso({"mqtt": SENHA_FICTICIA} if lembrar else None)
        janela = montar_janela(list(argv), config=config, bridge_cls=FakePipelineBridge, settings=settings,
                               tema="claro", cofre=cofre)
        criadas.append(janela)
        return janela, janela.bridge

    yield _abrir
    for j in criadas:
        j.close()


def _nomes(bridge) -> list[str]:
    return [c[0] for c in bridge.chamadas]


def _conectou(bridge) -> bool:
    return "conectar" in _nomes(bridge)


def test_config_inicio(tmp_path):
    assert Config().inicio.conectar_automaticamente is True
    arq = tmp_path / "config.toml"
    arq.write_text("[inicio]\nconectar_automaticamente = false\n", encoding="utf-8")
    assert carregar(arq).inicio.conectar_automaticamente is False


def test_abre_testa_banco_e_conecta(abrir):
    janela, bridge = abrir()
    # nada antes de a janela aparecer: a conexão vem por QTimer, não no construtor
    assert not _conectou(bridge)
    assert esperar(lambda: _conectou(bridge))
    nomes = _nomes(bridge)
    i_teste, i_con = nomes.index("testar_sql"), nomes.index("conectar")
    assert "definir_sql" in nomes[:i_teste] and i_teste < i_con
    assert bridge.chamadas[i_con] == ("conectar", "10.0.0.5", 1884, "NOVUS/x/#", 1)
    cfg = bridge.chamadas[i_teste][1]
    assert (cfg.servidor, cfg.senha) == ("TERRA\\SOLO", SENHA_FICTICIA)  # senha veio do cofre
    assert janela.vigia.ativo
    assert esperar(lambda: janela.estado == "conectado")
    assert esperar(lambda: janela.resultado_sql.isVisible() and "Conectado a" in janela.logs.texto())
    janela.logs.descarregar()
    texto = janela.logs.texto()
    assert MSG_AUTO_ABERTURA in texto and AVISO_SEM_SENHA not in texto

    # Desconectar continua normal e para o vigia
    janela.botao_conectar.click()
    assert esperar(lambda: janela.estado == "desconectado")
    assert not janela.vigia.ativo


def test_opcao_desligada_nao_faz_nada(abrir):
    janela, bridge = abrir(auto=False)
    esperar(lambda: False, timeout=0.6)
    nomes = _nomes(bridge)
    assert "conectar" not in nomes and "testar_sql" not in nomes
    # só o envio de sempre do SQL (e a periodicidade dos apontamentos reais), sem teste nem conexão
    assert set(nomes) <= {"definir_sql", "definir_periodicidade_apontamentos"} and "definir_sql" in nomes
    assert janela.estado == "desconectado" and not janela.vigia.ativo


def test_opcao_desligada_com_auto_conectar_conecta(abrir):
    janela, bridge = abrir(argv=("app", ARG_AUTO), auto=False)
    assert esperar(lambda: _conectou(bridge))
    assert "testar_sql" in _nomes(bridge)
    assert janela.vigia.ativo
    janela.logs.descarregar()
    assert MSG_AUTO_REINICIO in janela.logs.texto()


@pytest.mark.parametrize("broker, topico", [("", "NOVUS/x/#"), ("mqtt://10.0.0.5", "NOVUS/x/#"),
                                            ("10.0.0.5", "")])
def test_campo_invalido_nao_conecta(abrir, broker, topico):
    janela, bridge = abrir()
    # campo vazio no QSettings volta ao padrão do config; por isso o valor inválido vai direto no campo,
    # antes de a conexão automática disparar (ela espera a janela aparecer)
    janela.campo_broker.setText(broker)
    janela.campo_topico.setText(topico)
    assert not _nomes(bridge).count("testar_sql")
    assert esperar(lambda: "testar_sql" in _nomes(bridge))
    esperar(lambda: False, timeout=0.3)
    assert not _conectou(bridge)
    assert janela.erro_validacao.isVisibleTo(janela) and janela.erro_validacao.text()
    assert janela.estado == "desconectado" and not janela.vigia.ativo
    janela.logs.descarregar()
    assert "Conexão automática não iniciada" in janela.logs.texto()


def test_sql_sem_senha_conecta_mqtt_sem_testar(abrir):
    janela, bridge = abrir(lembrar=False, cofre=CofreFalso())
    assert esperar(lambda: _conectou(bridge))
    assert "testar_sql" not in _nomes(bridge)
    assert janela.vigia.ativo
    janela.logs.descarregar()
    texto = janela.logs.texto()
    assert AVISO_SEM_SENHA in texto
    assert texto.count("Lembrar senha") == 1  # sem o aviso duplicado do vigia


def test_sql_nao_configurado_conecta_sem_testar(abrir):
    janela, bridge = abrir(servidor="")
    assert esperar(lambda: _conectou(bridge))
    assert "testar_sql" not in _nomes(bridge)
    janela.logs.descarregar()
    assert AVISO_SEM_SENHA not in janela.logs.texto()
