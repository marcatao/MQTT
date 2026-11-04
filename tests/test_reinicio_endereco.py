"""Gravação imediata, reinício em 1 h e Endereço Mosquitto (CLAUDE.md, decidido em 04/11/2026).

* Chip "Reinício automático em 59:12 se nada for gravado" na aba Atividade (relógio falso).
* Sem o chip "Próxima gravação" no modo de gravação imediata (intervalo 0).
* Campo "Endereço Mosquitto" na aba Mosquitto, com um mosquitto.exe falso numa pasta temporária.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QCoreApplication, QSettings, Qt  # noqa: E402
from PySide6.QtTest import QTest  # noqa: E402
from PySide6.QtWidgets import QFileDialog  # noqa: E402

from app.config import Config, MosquittoConfig, carregar  # noqa: E402
from app.gui import tempo  # noqa: E402
from app.gui.mosquitto import CHAVE_PASTA, DICA_PARE, _DialogoPasta, normalizar_pasta  # noqa: E402
from app.gui.vigia import Vigia, minutos_txt  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401
from tests.test_mosquitto import RODANDO, _falso  # noqa: E402
from tests.test_operacao_continua import Relogio, iso, montar  # noqa: E402,F401

linux = pytest.mark.skipif(sys.platform == "win32", reason="mosquitto.exe falso usa shebang (Linux/macOS)")


# ====================================================================== vigia de 1 hora
def test_vigia_padrao_60_minutos(tmp_path):
    assert Config().vigia.minutos == 60
    assert carregar(tmp_path / "nao_existe.toml").vigia.minutos == 60
    exemplo = Path(__file__).resolve().parent.parent / "config.example.toml"
    assert carregar(exemplo).vigia.minutos == 60


def test_janela_usa_60_por_padrao(qapp, tmp_path):  # noqa: F811
    from app.gui.janela import JanelaPrincipal

    config = Config()
    config.mosquitto.iniciar_automaticamente = False
    settings = QSettings(str(tmp_path / "gui.ini"), QSettings.Format.IniFormat)
    janela = JanelaPrincipal(FakePipelineBridge(config, ciclo=0, intervalo=3600), config, settings=settings,
                             tema="claro", cofre=CofreFalso())
    try:
        assert janela.vigia.minutos == 60
        assert "1 h sem nenhuma gravação" in janela.logs.chip_reinicio.toolTip()
    finally:
        janela.close()


def test_mensagem_do_vigia_em_horas():
    assert minutos_txt(60) == "1 h"
    assert minutos_txt(90) == "1 h 30 min"
    assert minutos_txt(10) == "10 min"
    relogio = Relogio()
    disparos = []
    v = Vigia(60, disparos.append, sql_configurado=lambda: True, relogio=relogio)
    v.iniciar()
    relogio.avancar(30 * 60)
    v.registrar_leitura()  # leituras chegando, nenhuma gravação
    relogio.avancar(30 * 60 + 1)
    assert v.verificar() == "Sem gravação no SQL Server há 1 h — reiniciando o programa"


# ====================================================================== chip do reinício
@pytest.mark.parametrize("restante, valor, estado", [
    (3600, "1:00:00", "neutro"), (3552, "59:12", "neutro"), (600, "10:00", "neutro"),
    (599, "9:59", "aviso"), (60, "1:00", "aviso"), (59, "0:59", "erro"), (0.4, "0:01", "erro"),
])
def test_contagem_reinicio_formato_e_cores(restante, valor, estado):
    prefixo, v, sufixo, e = tempo.contagem_reinicio(restante)
    assert (prefixo, v, sufixo, e) == ("Reinício automático em", valor, "se nada for gravado", estado)


def test_contagem_reinicio_textos_fixos():
    assert tempo.contagem_reinicio(None) == ("Reinício automático", "—", "", "neutro")
    assert tempo.contagem_reinicio(100, ligado=False) == ("Reinício automático", "desligado", "", "neutro")
    assert tempo.contagem_reinicio(100, sql_configurado=False)[2] == "se nenhuma leitura chegar"


def _conectar(janela):
    janela.botao_conectar.click()
    assert esperar(lambda: janela.estado == "conectado")


def test_chip_conta_recomeca_e_muda_de_cor(montar):  # noqa: F811
    janela, bridge, relogio, iniciados, _ = montar(minutos=60)
    chip = janela.logs.chip_reinicio
    assert chip.texto() == "Reinício automático —"  # desconectado
    assert chip.estado == "neutro"
    _conectar(janela)
    assert chip.texto() == "Reinício automático em 1:00:00 se nada for gravado"  # h:mm:ss a partir de 1 h
    relogio.avancar(48)
    janela._tique()
    assert chip.texto() == "Reinício automático em 59:12 se nada for gravado"
    assert chip.estado == "neutro"

    # gravação com sucesso: recomeça
    bridge.ultima_leitura.emit(iso(relogio()))
    bridge.ultima_gravacao_ok.emit(iso(relogio()))
    QCoreApplication.processEvents()
    assert chip.valor() == "1:00:00"

    relogio.avancar(50 * 60 + 1)  # faltam 9:59
    janela._tique()
    assert chip.valor() == "9:59"
    assert chip.estado == "aviso"  # âmbar
    relogio.avancar(9 * 60)  # faltam 0:59
    janela._tique()
    assert chip.valor() == "0:59"
    assert chip.estado == "erro"  # vermelho
    assert not iniciados  # ainda não reiniciou

    # nova gravação: volta ao neutro com 1 h cheia
    bridge.ultima_gravacao_ok.emit(iso(relogio()))
    QCoreApplication.processEvents()
    assert chip.valor() == "1:00:00" and chip.estado == "neutro"

    janela.botao_conectar.click()  # Desconectar
    assert esperar(lambda: janela.estado == "desconectado")
    assert chip.texto() == "Reinício automático —"


def test_chip_sem_sql_recomeca_a_cada_leitura(montar):  # noqa: F811
    janela, bridge, relogio, _, _ = montar(minutos=60, servidor="")
    chip = janela.logs.chip_reinicio
    _conectar(janela)
    assert chip.texto() == "Reinício automático em 1:00:00 se nenhuma leitura chegar"
    relogio.avancar(20 * 60)
    janela._tique()
    assert chip.valor() == "40:00"
    bridge.ultima_leitura.emit(iso(relogio()))
    QCoreApplication.processEvents()
    assert chip.valor() == "1:00:00"
    assert "sem nenhuma leitura" in chip.toolTip()


def test_chip_desligado_com_zero(montar):  # noqa: F811
    janela, _, relogio, _, _ = montar(minutos=0)
    chip = janela.logs.chip_reinicio
    assert chip.texto() == "Reinício automático desligado"
    _conectar(janela)
    relogio.avancar(5 * 3600)
    janela._tique()
    assert chip.texto() == "Reinício automático desligado"


def test_sem_chip_proxima_no_modo_imediato(montar):  # noqa: F811
    janela, bridge, relogio, _, _ = montar(minutos=60)  # intervalo 0 (padrão novo)
    assert Config().gravacao.intervalo_gravacao_segundos == 0
    assert janela.logs.chip_proxima.isHidden()
    _conectar(janela)
    bridge.proxima_gravacao.emit(relogio() + 42)  # bridge antigo: não aparece mesmo assim
    QCoreApplication.processEvents()
    janela._tique()
    assert janela.logs.chip_proxima.isHidden()
    assert not janela.logs.chip_reinicio.isHidden()
    assert not janela.logs.chip_ultima.isHidden()


# ====================================================================== Endereço Mosquitto
def test_normalizar_pasta():
    assert normalizar_pasta(r'  "C:\Program Files\mosquitto\mosquitto.exe" ') == r"C:\Program Files\mosquitto"
    assert normalizar_pasta("C:\\mosquitto\\") == "C:\\mosquitto"
    assert normalizar_pasta("C:\\mosquitto.exe") == "C:\\"
    assert normalizar_pasta("C:\\") == "C:\\"
    assert normalizar_pasta("/opt/mosq/mosquitto.exe") == "/opt/mosq"
    assert normalizar_pasta("/opt/mosq/") == "/opt/mosq"
    assert normalizar_pasta("   ") == ""


@pytest.fixture
def abrir_janela(qapp, tmp_path):  # noqa: F811
    """Abre como o app/main.py. `ini` permite reabrir com o MESMO QSettings (persistência)."""
    from app.main import montar_janela

    criadas = []

    def _abrir(pasta_config, auto=False, ini="gui.ini"):
        config = Config()
        config.inicio.conectar_automaticamente = False
        config.mosquitto = MosquittoConfig(pasta=str(pasta_config), iniciar_automaticamente=auto)
        settings = QSettings(str(tmp_path / ini), QSettings.Format.IniFormat)
        janela = montar_janela(["app"], config=config, bridge_cls=FakePipelineBridge, settings=settings,
                               tema="claro", cofre=CofreFalso())
        criadas.append(janela)
        return janela, janela.mosquitto, janela.painel_mosquitto, settings

    yield _abrir
    for j in criadas:
        j.close()
        j.deleteLater()
    esperar(lambda: False, 0.05)


@pytest.fixture
def pastas(tmp_path):
    """(padrao, outra, vazia): a padrão e a outra têm um mosquitto.exe falso; a vazia não tem."""
    padrao, outra, vazia = tmp_path / "padrao", tmp_path / "outra", tmp_path / "vazia"
    for p in (padrao, outra, vazia):
        p.mkdir()
    _falso(padrao, RODANDO)
    _falso(outra, RODANDO)
    return padrao, outra, vazia


def test_campo_pre_preenchido(abrir_janela, pastas):
    padrao, _, _ = pastas
    janela, console, painel, _ = abrir_janela(padrao)
    assert painel.rotulo_endereco.text() == "ENDEREÇO MOSQUITTO"
    assert painel.campo_endereco.text() == str(padrao)
    assert not painel.campo_endereco.isReadOnly()
    assert painel.botao_procurar.text() == "…" and painel.botao_procurar.isEnabled()
    assert console.pasta == str(padrao)


@linux
def test_somente_leitura_enquanto_roda(abrir_janela, pastas):
    padrao, _, _ = pastas
    janela, console, painel, _ = abrir_janela(padrao)
    painel.botao_iniciar.click()
    assert esperar(lambda: console.estado == "rodando", 5)
    assert painel.campo_endereco.isReadOnly()
    assert not painel.botao_procurar.isEnabled() and not painel.botao_restaurar.isEnabled()
    assert painel.campo_endereco.toolTip() == DICA_PARE
    assert painel.procurar() is False and painel.confirmar_endereco() is False
    painel.botao_parar.click()
    assert esperar(lambda: console.estado == "parado", 5)
    assert not painel.campo_endereco.isReadOnly()
    assert painel.botao_procurar.isEnabled()


def _mock_dialogo(monkeypatch, resposta: str | None):
    """Troca o exec() do QFileDialog do "…": devolve `resposta` (None = Cancelar)."""
    chamadas = []

    def exec_falso(self):
        chamadas.append(self.directory().absolutePath())
        assert isinstance(self, QFileDialog)
        if resposta is None:
            return QFileDialog.DialogCode.Rejected
        self.escolhido = resposta
        return QFileDialog.DialogCode.Accepted

    monkeypatch.setattr(_DialogoPasta, "exec", exec_falso)
    return chamadas


def test_seletor_aceita_pasta(abrir_janela, pastas, monkeypatch):
    padrao, outra, _ = pastas
    janela, console, painel, settings = abrir_janela(padrao)
    chamadas = _mock_dialogo(monkeypatch, str(outra))
    painel.botao_procurar.click()
    # abre na pasta atual (compara o caminho, não o texto: no Windows o Qt usa "/" e o pathlib "\")
    assert [Path(c) for c in chamadas] == [Path(padrao)]
    assert painel.campo_endereco.text() == str(outra)
    assert console.pasta == str(outra)
    assert settings.value(CHAVE_PASTA) == str(outra)


def test_seletor_aceita_o_proprio_exe(abrir_janela, pastas, monkeypatch):
    padrao, outra, _ = pastas
    janela, console, painel, settings = abrir_janela(padrao)
    _mock_dialogo(monkeypatch, str(outra / "mosquitto.exe"))
    painel.botao_procurar.click()
    assert painel.campo_endereco.text() == str(outra)
    assert console.pasta == str(outra)
    assert console.prompt() == f"{outra}>mosquitto -v"
    assert settings.value(CHAVE_PASTA) == str(outra)


def test_seletor_cancelado_ou_pasta_sem_exe(abrir_janela, pastas, monkeypatch):
    padrao, _, vazia = pastas
    janela, console, painel, settings = abrir_janela(padrao)
    _mock_dialogo(monkeypatch, None)
    painel.botao_procurar.click()
    assert painel.campo_endereco.text() == str(padrao) and settings.value(CHAVE_PASTA) is None
    _mock_dialogo(monkeypatch, str(vazia))
    painel.botao_procurar.click()
    assert painel.campo_endereco.property("invalido") is True
    assert "mosquitto.exe não encontrado" in painel.erro_endereco.text()
    assert console.pasta == str(padrao)  # não trocou
    assert settings.value(CHAVE_PASTA) is None


def test_dialogo_real_aceita_exe_selecionado(qapp, pastas):  # noqa: F811
    """O QFileDialog (modo pasta) aceita também um arquivo clicado: devolve o caminho do exe."""
    _, outra, _ = pastas
    d = _DialogoPasta(None, str(outra))
    assert d.fileMode() == QFileDialog.FileMode.Directory
    assert not d.testOption(QFileDialog.Option.ShowDirsOnly)  # mostra o mosquitto.exe
    d.selectFile(str(outra / "mosquitto.exe"))
    monkey = d.selectedFiles
    d.selectedFiles = lambda: [str(outra / "mosquitto.exe")]  # como se o usuário clicasse no arquivo
    try:
        d.accept()
    finally:
        d.selectedFiles = monkey
    assert d.result() == QFileDialog.DialogCode.Accepted
    assert d.escolhido == str(outra / "mosquitto.exe")
    d.deleteLater()


@linux
def test_iniciar_usa_pasta_do_campo(abrir_janela, pastas):
    padrao, outra, _ = pastas
    janela, console, painel, settings = abrir_janela(padrao)
    painel.campo_endereco.setText(str(outra / "mosquitto.exe"))  # colou o caminho do exe
    painel.botao_iniciar.click()
    assert esperar(lambda: f"cwd={outra}" in painel.caixa.texto(), 5), painel.caixa.texto()
    linhas = painel.caixa.linhas_texto()
    assert linhas[0] == f"{outra}>mosquitto -v"
    assert "args=-v" in linhas
    assert painel.campo_endereco.text() == str(outra)
    assert console.programa == str(outra / "mosquitto.exe")
    assert settings.value(CHAVE_PASTA) == str(outra)  # salvo ao iniciar com sucesso
    assert janela.abas.tabToolTip(1) == f"Broker Mosquitto local: {outra}>mosquitto -v"


def test_pasta_invalida_nao_inicia(abrir_janela, pastas):
    padrao, _, vazia = pastas
    janela, console, painel, settings = abrir_janela(padrao)
    painel.campo_endereco.setText(str(vazia))
    assert painel.iniciar() is False
    QCoreApplication.processEvents()
    assert console.estado == "parado" and not console.rodando
    texto = painel.caixa.texto()
    assert f"{vazia}>mosquitto -v" in texto
    assert f"mosquitto.exe não encontrado em {vazia}" in texto
    assert painel.campo_endereco.property("invalido") is True
    assert painel.erro_endereco.isVisibleTo(painel)
    assert settings.value(CHAVE_PASTA) is None
    QTest.keyClick(painel.campo_endereco, Qt.Key.Key_X)  # editar limpa o erro
    assert not painel.campo_endereco.property("invalido")
    assert not painel.erro_endereco.isVisibleTo(painel)
    painel.campo_endereco.setText("")
    assert painel.iniciar() is False
    assert "Informe a pasta" in painel.erro_endereco.text()


@linux
def test_persistencia_reabre_com_o_valor_salvo(abrir_janela, pastas):
    padrao, outra, _ = pastas
    janela, _, painel, settings = abrir_janela(padrao, ini="persistir.ini")
    painel.campo_endereco.setText(str(outra))
    QTest.keyClick(painel.campo_endereco, Qt.Key.Key_Return)  # Enter confirma e salva
    assert settings.value(CHAVE_PASTA) == str(outra)
    janela.close()

    # reabre (como no reinício do vigia: início automático) com o mesmo QSettings: vale mais que o config
    janela2, console2, painel2, _ = abrir_janela(padrao, auto=True, ini="persistir.ini")
    assert painel2.campo_endereco.text() == str(outra)
    assert console2.pasta == str(outra)
    assert esperar(lambda: f"cwd={outra}" in painel2.caixa.texto(), 5), painel2.caixa.texto()
    assert painel2.caixa.linhas_texto()[0] == f"{outra}>mosquitto -v"


def test_enter_com_pasta_invalida_nao_salva(abrir_janela, pastas):
    padrao, _, vazia = pastas
    _, console, painel, settings = abrir_janela(padrao)
    painel.campo_endereco.setText(str(vazia))
    QTest.keyClick(painel.campo_endereco, Qt.Key.Key_Return)
    assert settings.value(CHAVE_PASTA) is None
    assert console.pasta == str(padrao)
    assert painel.campo_endereco.property("invalido") is True


def test_restaurar_padrao(abrir_janela, pastas, monkeypatch):
    padrao, outra, _ = pastas
    janela, console, painel, settings = abrir_janela(padrao)
    _mock_dialogo(monkeypatch, str(outra))
    painel.botao_procurar.click()
    assert settings.value(CHAVE_PASTA) == str(outra)
    assert painel.botao_restaurar.text() == "Restaurar padrão"
    assert str(padrao) in painel.botao_restaurar.toolTip()
    painel.botao_restaurar.click()
    assert painel.campo_endereco.text() == str(padrao)
    assert console.pasta == str(padrao)
    assert settings.value(CHAVE_PASTA) is None
    assert console.prompt() == f"{padrao}>mosquitto -v"
