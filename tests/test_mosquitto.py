"""Aba Mosquitto: console do broker local como processo filho (QProcess), com um mosquitto.exe falso.

Em Linux o "mosquitto.exe" é um script Python executável (shebang) numa pasta temporária; o comando extra roda
com /bin/sh -c (no Windows, cmd.exe /c).
"""
from __future__ import annotations

import os
import stat
import sys
import textwrap
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt  # noqa: E402
from PySide6.QtTest import QTest  # noqa: E402

from app.config import Config, MosquittoConfig, carregar  # noqa: E402
from app.gui.mosquitto import (  # noqa: E402
    DICA_SERVICO,
    ConsoleMosquitto,
    _Leitor,
    decodificar,
    separar_comando,
)
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="mosquitto.exe falso usa shebang (Linux/macOS)")

RODANDO = """
    import os, sys, time
    print("1730650000: mosquitto version 2.0.18 starting", file=sys.stderr, flush=True)
    print("1730650000: Opening ipv4 listen socket on port 1883.", flush=True)
    print("args=" + " ".join(sys.argv[1:]), flush=True)
    print("cwd=" + os.getcwd(), flush=True)
    while True:
        time.sleep(0.05)
"""
SAI_COM_ERRO = """
    import sys
    print("1730650000: mosquitto version 2.0.18 starting", file=sys.stderr, flush=True)
    print("1730650000: Error: Only one usage of each socket address is normally permitted.", file=sys.stderr,
          flush=True)
    sys.exit(1)
"""
IGNORA_SIGTERM = """
    import signal, sys, time
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    print("teimoso", flush=True)
    while True:
        time.sleep(0.05)
"""


def _falso(pasta, corpo: str, nome: str = "mosquitto.exe"):
    arq = pasta / nome
    arq.write_text(f"#!{sys.executable} -u\n" + textwrap.dedent(corpo), encoding="utf-8")
    arq.chmod(arq.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return arq


def _vivo(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:  # zumbi (já saiu, ainda não colhido) conta como morto
        with open(f"/proc/{pid}/stat", encoding="ascii") as f:
            return f.read().split(") ", 1)[1][0] != "Z"
    except OSError:
        return True


@pytest.fixture
def abrir(qapp, tmp_path):  # noqa: F811
    """Abre o programa como o app/main.py (montar_janela), com o Mosquitto apontando para a pasta temporária."""
    from app.main import montar_janela

    criadas = []

    def _abrir(corpo: str | None = RODANDO, auto: bool = True, pasta=None, comando: str = "mosquitto -v"):
        pasta = pasta or tmp_path
        if corpo is not None:
            _falso(pasta, corpo)
        config = Config()
        config.inicio.conectar_automaticamente = False
        config.mosquitto = MosquittoConfig(pasta=str(pasta), comando=comando, iniciar_automaticamente=auto)
        settings = QSettings(str(tmp_path / f"gui{len(criadas)}.ini"), QSettings.Format.IniFormat)
        janela = montar_janela(["app"], config=config, bridge_cls=FakePipelineBridge, settings=settings,
                               tema="claro", cofre=CofreFalso())
        criadas.append(janela)
        return janela, janela.mosquitto, janela.painel_mosquitto

    yield _abrir
    for j in criadas:
        j.close()
        j.deleteLater()  # janelas fechadas não ficam vivas (com timers) atrapalhando os testes seguintes
    esperar(lambda: False, 0.05)


def _texto(painel) -> str:
    return painel.caixa.texto()


# ---------------------------------------------------------------------- funções puras
def test_prompt_padrao_exato():
    console = ConsoleMosquitto(MosquittoConfig())
    assert console.prompt() == \
        r'C:\Program Files\Mosquitto>mosquitto -c "C:\Program Files\Mosquitto\mosquitto.conf" -v'
    assert console.programa == r"C:\Program Files\Mosquitto\mosquitto.exe"
    assert console.prompt('mosquitto_sub -h localhost -t "#" -v') == \
        r'C:\Program Files\Mosquitto>mosquitto_sub -h localhost -t "#" -v'


def test_separar_comando():
    assert separar_comando("mosquitto -v") == ("mosquitto.exe", ["-v"])
    assert separar_comando("mosquitto.exe -v -c mosquitto.conf") == ("mosquitto.exe", ["-v", "-c", "mosquitto.conf"])
    assert separar_comando(r'mosquitto -c "C:\Program Files\m\a b.conf"') == \
        ("mosquitto.exe", ["-c", r"C:\Program Files\m\a b.conf"])
    assert separar_comando("  ") == ("", [])


def test_decodificacao_utf8_e_codificacao_local():
    assert decodificar("ação".encode("utf-8")) == "ação"
    assert decodificar("ação".encode("cp850"), "cp850") == "ação"  # não é UTF-8 válido: cai para a local
    assert decodificar(b"\xff\xfe ok", "nao-existe") == "\ufffd\ufffd ok"
    leitor = _Leitor()
    dados = "linha com ç\nmeia".encode("utf-8")
    corte = dados.index("ç".encode()) + 1  # parte o "ç" ao meio
    assert leitor.ler(dados[:corte]) is None
    assert leitor.ler(dados[corte:]) == "linha com ç"
    assert leitor.ler(b" linha\r\n") == "meia linha"
    assert leitor.final() is None


def test_comando_troca_a_variavel_pasta():
    console = ConsoleMosquitto(MosquittoConfig(pasta="D:\\Mosq\\"))
    assert console.comando == r'mosquitto -c "D:\Mosq\mosquitto.conf" -v'
    nome, args = separar_comando(console.comando)
    assert (nome, args) == ("mosquitto.exe", ["-c", r"D:\Mosq\mosquitto.conf", "-v"])


def test_config_mosquitto(tmp_path):
    padrao = Config().mosquitto
    assert padrao.pasta == r"C:\Program Files\Mosquitto"
    assert padrao.comando == r'mosquitto -c "{pasta}\mosquitto.conf" -v'
    assert padrao.iniciar_automaticamente is True
    arq = tmp_path / "config.toml"
    arq.write_text('[mosquitto]\npasta = "D:\\\\mqtt"\ncomando = "mosquitto -v -p 1884"\n'
                   "iniciar_automaticamente = false\n", encoding="utf-8")
    cfg = carregar(arq).mosquitto
    assert (cfg.pasta, cfg.comando, cfg.iniciar_automaticamente) == ("D:\\mqtt", "mosquitto -v -p 1884", False)


# ---------------------------------------------------------------------- janela
def test_abas_atividade_e_mosquitto(abrir):
    janela, _, painel = abrir(auto=False)
    assert janela.abas.count() == 4  # + Clientes (terceira) e Apontamentos reais (quarta)
    assert janela.abas.tabText(0) == "Atividade"
    assert janela.abas.tabText(1) == "Mosquitto"
    assert janela.abas.tabText(2) == "Clientes"
    assert janela.abas.tabText(3) == "Apontamentos reais"
    assert janela.abas.widget(0) is janela.logs
    assert janela.abas.widget(1) is painel
    assert janela.abas.currentIndex() == 0  # abre na Atividade, como antes
    # contadores continuam fora das abas
    assert not janela.abas.isAncestorOf(janela.contador_recebidas)


def test_inicio_automatico_mostra_prompt_e_saida(abrir, tmp_path):
    janela, console, painel = abrir()
    assert esperar(lambda: "Opening ipv4 listen socket on port 1883." in _texto(painel), 5), _texto(painel)
    linhas = painel.caixa.linhas_texto()
    assert linhas[0] == f"{tmp_path}>mosquitto -v"
    assert "1730650000: mosquitto version 2.0.18 starting" in linhas  # stderr mesclado
    assert "args=-v" in linhas
    assert f"cwd={tmp_path}" in linhas
    assert esperar(lambda: console.estado == "rodando", 3)
    assert painel.chip_estado.valor() == f"Rodando (PID {console.pid})"
    assert painel.chip_estado.estado == "ok"
    assert not painel.botao_iniciar.isEnabled() and painel.botao_parar.isEnabled()


def test_inicio_automatico_vem_antes_da_conexao(abrir, tmp_path, qapp):  # noqa: F811
    from app.main import montar_janela

    _falso(tmp_path, RODANDO)
    config = Config()
    config.mosquitto = MosquittoConfig(pasta=str(tmp_path))
    settings = QSettings(str(tmp_path / "ordem.ini"), QSettings.Format.IniFormat)
    janela = montar_janela(["app"], config=config, bridge_cls=FakePipelineBridge, settings=settings,
                           tema="claro", cofre=CofreFalso())
    try:
        # logo depois de abrir: o Mosquitto já foi pedido e a conexão ainda não (vem ~300 ms depois)
        assert janela.mosquitto.estado in ("iniciando", "rodando")
        assert not any(c[0] == "conectar" for c in janela.bridge.chamadas)
        assert esperar(lambda: any(c[0] == "conectar" for c in janela.bridge.chamadas), 3)
    finally:
        janela.close()


def test_executavel_ausente(abrir, tmp_path):
    vazia = tmp_path / "vazia"
    vazia.mkdir()
    janela, console, painel = abrir(corpo=None, pasta=vazia)
    assert esperar(lambda: "não encontrado" in _texto(painel), 3)
    linhas = painel.caixa.linhas_texto()
    assert linhas[0] == f"{vazia}>mosquitto -v"
    assert f"mosquitto.exe não encontrado em {vazia} — confira o Endereço Mosquitto (botão …)." in linhas
    assert console.estado == "erro"
    assert painel.chip_estado.estado == "erro"
    assert painel.chip_estado.valor().startswith("Erro:")
    assert janela.isVisible() and painel.botao_iniciar.isEnabled()


def test_pasta_padrao_inexistente_no_linux_nao_quebra(qapp):  # noqa: F811
    console = ConsoleMosquitto(MosquittoConfig())
    saidas = []
    console.saida.connect(lambda t, s: saidas.append((t, s)))
    assert console.iniciar() is False
    assert saidas[0] == ("prompt",
                         r'C:\Program Files\Mosquitto>mosquitto -c "C:\Program Files\Mosquitto\mosquitto.conf" -v')
    assert saidas[1] == ("erro", "mosquitto.exe não encontrado em C:\\Program Files\\Mosquitto — confira "
                                 "o Endereço Mosquitto (botão …).")
    assert console.estado == "erro"


def test_processo_que_sai_mostra_codigo_e_dica_sem_reiniciar(abrir):
    janela, console, painel = abrir(corpo=SAI_COM_ERRO)
    assert esperar(lambda: console.estado == "erro", 5), _texto(painel)
    texto = _texto(painel)
    assert "Error: Only one usage of each socket address" in texto
    assert "código 1" in texto
    assert DICA_SERVICO in texto
    assert painel.chip_estado.valor() == "Erro: encerrou com código 1"
    esperar(lambda: False, 0.6)  # não reinicia em loop
    assert not console.rodando
    assert _texto(painel).count(">mosquitto -v") == 1


def test_parar_encerra(abrir):
    janela, console, painel = abrir()
    assert esperar(lambda: console.estado == "rodando", 5)
    pid = console.pid
    painel.botao_parar.click()
    assert esperar(lambda: console.estado == "parado", 5), _texto(painel)
    assert not console.rodando
    assert esperar(lambda: not _vivo(pid), 3)
    assert painel.chip_estado.valor() == "Parado"
    assert "Mosquitto parado" in _texto(painel)
    assert painel.botao_iniciar.isEnabled() and not painel.botao_parar.isEnabled()
    # Iniciar de novo funciona e mostra o prompt outra vez
    painel.botao_iniciar.click()
    assert esperar(lambda: console.estado == "rodando", 5)
    assert _texto(painel).count(">mosquitto -v") == 2


def test_comando_extra_mostra_prompt_e_saida(abrir, tmp_path):
    janela, console, painel = abrir(auto=False)
    painel.campo_comando.setText("echo ola-do-extra")
    QTest.keyClick(painel.campo_comando, Qt.Key.Key_Return)
    assert esperar(lambda: "Comando terminou" in _texto(painel), 5), _texto(painel)
    linhas = painel.caixa.linhas_texto()
    assert linhas[0] == f"{tmp_path}>echo ola-do-extra"
    assert "ola-do-extra" in linhas
    assert not console.comando_rodando
    assert painel.botao_executar.text() == "Executar"


def test_comando_extra_um_por_vez_e_parar(abrir):
    janela, console, painel = abrir(auto=False)
    painel.campo_comando.setText("exec sleep 30")
    painel.botao_executar.click()
    assert esperar(lambda: console.comando_rodando, 3)
    assert painel.botao_executar.text() == "Parar"
    assert console.executar("echo outro") is False  # só um comando extra por vez
    assert "Já há um comando rodando" in _texto(painel)
    painel.botao_executar.click()  # "Parar" da linha
    assert esperar(lambda: not console.comando_rodando, 5)
    assert painel.botao_executar.text() == "Executar"
    assert "Comando interrompido." in _texto(painel)


def test_ctrl_c_no_campo_interrompe(abrir):
    janela, console, painel = abrir(auto=False)
    painel.campo_comando.setText("exec sleep 30")
    painel.campo_comando.setFocus()
    QTest.keyClick(painel.campo_comando, Qt.Key.Key_Return)
    assert esperar(lambda: console.comando_rodando, 3)
    painel.campo_comando.deselect()
    QTest.keyClick(painel.campo_comando, Qt.Key.Key_C, Qt.KeyboardModifier.ControlModifier)
    assert esperar(lambda: not console.comando_rodando, 5)
    assert "^C" in painel.caixa.linhas_texto()


def test_fechar_janela_encerra_filhos(abrir):
    janela, console, painel = abrir()
    assert esperar(lambda: console.estado == "rodando", 5)
    assert console.executar("exec sleep 30")
    assert esperar(lambda: console.comando_rodando, 3)
    pid = console.pid
    janela.close()
    assert not console.rodando and not console.comando_rodando
    assert esperar(lambda: not _vivo(pid), 3)
    assert ("encerrar", 8.0) in janela.bridge.chamadas


def test_encerrar_mata_quem_ignora_terminate(abrir):
    janela, console, painel = abrir(corpo=IGNORA_SIGTERM)
    assert esperar(lambda: "teimoso" in _texto(painel), 5)
    pid = console.pid
    inicio = time.monotonic()
    console.encerrar(timeout=0.3)
    assert time.monotonic() - inicio < 2.5
    assert not console.rodando
    assert esperar(lambda: not _vivo(pid), 3)


def test_reinicio_do_vigia_encerra_filhos_antes_da_nova_instancia(qapp, tmp_path):  # noqa: F811
    from app.gui.janela import JanelaPrincipal

    _falso(tmp_path, RODANDO)
    config = Config()
    config.mosquitto = MosquittoConfig(pasta=str(tmp_path))
    estado_ao_iniciar = []

    def iniciar(_cmd):
        estado_ao_iniciar.append(janela.mosquitto.rodando)
        return True

    settings = QSettings(str(tmp_path / "vigia.ini"), QSettings.Format.IniFormat)
    janela = JanelaPrincipal(FakePipelineBridge(config), config, settings=settings, tema="claro",
                             cofre=CofreFalso(), iniciar_processo=iniciar, sair=lambda: None)
    janela.show()
    try:
        janela.iniciar_mosquitto_ao_abrir()
        assert esperar(lambda: janela.mosquitto.estado == "rodando", 5)
        pid = janela.mosquitto.pid
        janela._reiniciar("Sem leitura há 10 min — reiniciando o programa")
        assert estado_ao_iniciar == [False]
        assert esperar(lambda: not _vivo(pid), 3)
    finally:
        janela.close()


def test_inicio_automatico_desligado_nao_inicia(abrir):
    janela, console, painel = abrir(auto=False)
    esperar(lambda: False, 0.4)
    assert not console.rodando
    assert console.estado == "parado"
    assert painel.caixa.linhas_texto() == []
    assert painel.chip_estado.valor() == "Parado"
    assert painel.botao_iniciar.isEnabled()


def test_tema_escuro_e_limpar(abrir):
    janela, console, painel = abrir()
    assert esperar(lambda: "starting" in _texto(painel), 5)
    janela._tema_forcado = "escuro"
    janela._aplicar_tema()  # repinta o console sem perder linhas
    assert "starting" in _texto(painel)
    painel.botao_limpar.click()
    assert painel.caixa.linhas_texto() == []
    assert console.rodando  # Limpar não para o processo
