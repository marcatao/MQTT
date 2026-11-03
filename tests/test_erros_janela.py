"""Card ERROS clicável e janela "Erros registrados" (QT_QPA_PLATFORM=offscreen)."""
from __future__ import annotations

import csv
import json
import os
from datetime import date

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt  # noqa: E402
from PySide6.QtGui import QGuiApplication  # noqa: E402
from PySide6.QtTest import QTest  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui.erros import (  # noqa: E402
    COL_CATEGORIA,
    JanelaErros,
    ListaErros,
    RegistroErro,
    ler_erros_do_dia,
)
from app.gui.widgets import PAPEL_PILULA  # noqa: E402
from app.gui.janela import JanelaPrincipal  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_clientes import destruir  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

HOJE = date.today().isoformat()


def erro(hora: str, categoria: str, mensagem: str, topico: str | None = "NOVUS/Teste_Alumbra/events",
         detalhe: str | None = None, payload: str | None = None) -> dict:
    return {"quando": f"{HOJE}T{hora}", "categoria": categoria, "mensagem": mensagem, "topico": topico,
            "detalhe": detalhe, "payload": payload}


def gravar_arquivo(pasta, registros) -> None:
    caminho = pasta / f"erros_{date.today():%Y%m%d}.jsonl"
    caminho.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in registros) + "lixo\n",
                       encoding="utf-8")


@pytest.fixture
def nova_janela_erros(qapp):
    criadas = []

    def _nova(*a, **kw):
        d = JanelaErros(*a, **kw)
        criadas.append(d)
        return d

    yield _nova
    destruir(criadas)


@pytest.fixture
def janela(qapp, tmp_path):
    criadas = []

    def _montar():
        config = Config()
        config.mosquitto.iniciar_automaticamente = False
        config.gravacao.pasta_logs = str(tmp_path)
        bridge = FakePipelineBridge(config)
        j = JanelaPrincipal(bridge, config, settings=QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat),
                            tema="claro", cofre=CofreFalso(), executor_arp=lambda: "", enderecos_locais=set())
        j.show()
        criadas.append(j)
        return j, bridge

    yield _montar
    destruir(criadas)


# ---------------------------------------------------------------------- card
def test_card_erros_clicavel_abre_janela(janela):
    j, _ = janela()
    card = j.contador_erros
    assert card.cursor().shape() == Qt.CursorShape.PointingHandCursor
    assert card.toolTip() == "Clique para ver os erros"
    assert card.focusPolicy() == Qt.FocusPolicy.StrongFocus
    assert card.testAttribute(Qt.WidgetAttribute.WA_Hover)
    assert not j.janela_erros.isVisible()
    QTest.mouseClick(card, Qt.MouseButton.LeftButton)
    assert j.janela_erros.isVisible()
    assert j.janela_erros.windowTitle() == "Erros registrados"
    assert not j.janela_erros.isModal()
    # os outros cards continuam como estavam
    assert not j.contador_recebidas.clicavel and not j.contador_gravadas.clicavel


@pytest.mark.parametrize("tecla", [Qt.Key.Key_Return, Qt.Key.Key_Enter, Qt.Key.Key_Space])
def test_card_erros_abre_pelo_teclado(janela, tecla):
    j, _ = janela()
    j.contador_erros.setFocus()
    QTest.keyClick(j.contador_erros, tecla)
    assert j.janela_erros.isVisible()


# ---------------------------------------------------------------------- lista
def test_erro_registrado_entra_na_lista(janela):
    j, bridge = janela()
    bridge.emitir_erro("sql", "Falha ao gravar no SQL Server: tempo esgotado.", None, "HYT00 Login timeout")
    bridge.emitir_erro("payload", "Leitura incompleta: falta ts_unix.", "NOVUS/x",
                       None, '{"pid":1,"device_id":"x"}')
    dlg = j.janela_erros
    assert esperar(lambda: dlg.total == 2)
    j.abrir_erros()
    assert esperar(lambda: not dlg._carregando)
    assert dlg.filtro.rowCount() == 2
    # mais recente primeiro (mesmo segundo: ordem de chegada)
    assert [r.categoria for r in dlg.registros_visiveis()] == ["payload", "sql"]
    dlg.tabela.selectRow(0)
    assert dlg.detalhe_topico.text() == "NOVUS/x"
    assert dlg.payload.toPlainText() == '{\n  "pid": 1,\n  "device_id": "x"\n}'  # JSON identado
    assert dlg.payload.font().fixedPitch() or dlg.payload.font().styleHint() == dlg.payload.font().StyleHint.Monospace
    dlg.copiar_detalhe()
    copiado = QGuiApplication.clipboard().text()
    assert "Tópico: NOVUS/x" in copiado and '"device_id": "x"' in copiado
    # cor por categoria (pílula)
    tons = {dlg.filtro.index(r, COL_CATEGORIA).data(PAPEL_PILULA) for r in range(2)}
    assert tons == {"aviso", "erro"}


def test_payload_que_nao_e_json_aparece_como_veio():
    r = RegistroErro.de_dict(erro("10:00:00", "payload", "x", payload="a;b;c"))
    assert r.payload_formatado() == "a;b;c"
    assert RegistroErro.de_dict(erro("10:00:00", "payload", "x", payload="base64:AAEC")).payload_formatado() == \
        "base64:AAEC"


def test_carga_do_arquivo_do_dia_sem_duplicar(janela, tmp_path):
    j, bridge = janela()
    no_arquivo = [erro("08:00:00", "contingencia", "Contingência sem espaço em disco."),
                  erro("09:00:00", "payload", "Leitura incompleta: falta pid."),
                  erro("09:00:00", "payload", "Leitura incompleta: falta pid."),  # repetido de verdade
                  erro("10:00:00", "sql", "Login falhou.", None, "18456")]
    gravar_arquivo(tmp_path, no_arquivo)
    # a sessão já tem 2 deles (vieram pelo sinal) e um novo
    for d in (no_arquivo[1], no_arquivo[3], erro("11:00:00", "fila", "Fila cheia: mensagem descartada.")):
        bridge.erro_registrado.emit(json.dumps(d))
    dlg = j.janela_erros
    assert esperar(lambda: dlg.total == 3)
    j.abrir_erros()
    assert esperar(lambda: not dlg._carregando and dlg.total == 5)
    assert [(r.quando[-8:], r.categoria) for r in dlg.lista.itens] == [
        ("08:00:00", "contingencia"), ("09:00:00", "payload"), ("09:00:00", "payload"), ("10:00:00", "sql"),
        ("11:00:00", "fila")]
    # abrir de novo não duplica
    dlg.close()
    j.abrir_erros()
    assert esperar(lambda: not dlg._carregando)
    assert dlg.total == 5
    assert dlg.chip_total.texto() == "5 erros"


def test_ler_erros_do_dia_le_o_arquivo(tmp_path):
    gravar_arquivo(tmp_path, [erro("08:00:00", "sql", "a")])
    regs = ler_erros_do_dia(str(tmp_path))
    assert [r["mensagem"] for r in regs] == ["a"]
    assert ler_erros_do_dia(str(tmp_path / "nao-existe")) == []


def test_limite_em_memoria():
    lista = ListaErros(limite=5)
    lista.adicionar([RegistroErro(f"{HOJE}T10:00:0{i}", "payload", f"m{i}") for i in range(8)])
    assert [r.mensagem for r in lista.itens] == ["m3", "m4", "m5", "m6", "m7"]


def test_limite_5000_na_janela(nova_janela_erros, tmp_path):
    dlg = nova_janela_erros(str(tmp_path), ler=lambda _p: [])
    for i in range(5010):
        dlg.adicionar_json(json.dumps(erro("10:00:00", "payload", f"m{i}")))
    assert dlg.total == 5000
    assert dlg.lista.itens[0].mensagem == "m10"
    dlg.close()


def test_filtro_e_busca(nova_janela_erros, tmp_path):
    dlg = nova_janela_erros(str(tmp_path), ler=lambda _p: [])
    for d in (erro("08:00:00", "sql", "Login falhou.", None, "erro 18456"),
              erro("09:00:00", "payload", "JSON truncado.", "NOVUS/a", payload='{"device_id":"logbox02"'),
              erro("10:00:00", "payload", "Falta ts_unix.", "NOVUS/b"),
              erro("11:00:00", "mqtt", "Broker recusou.")):
        dlg.adicionar_json(json.dumps(d))
    dlg.descarregar()
    assert dlg.filtro.rowCount() == 4
    dlg.filtrar(categoria="payload")
    assert {r.mensagem for r in dlg.registros_visiveis()} == {"JSON truncado.", "Falta ts_unix."}
    assert dlg.chip_total.texto() == "Mostrando 2 de 4"
    dlg.filtrar(busca="logbox02")  # busca dentro do payload, com a categoria ainda aplicada
    assert [r.mensagem for r in dlg.registros_visiveis()] == ["JSON truncado."]
    dlg.filtrar(categoria="", busca="18456")  # busca no detalhe, todas as categorias
    assert [r.categoria for r in dlg.registros_visiveis()] == ["sql"]
    dlg.filtrar(categoria="mqtt", busca="")  # categoria mqtt pronta mesmo sem uso hoje
    assert [r.mensagem for r in dlg.registros_visiveis()] == ["Broker recusou."]
    dlg.filtrar(categoria="fila")
    assert dlg.filtro.rowCount() == 0 and dlg.pilha.currentWidget() is dlg.vazio
    assert not dlg.botao_exportar.isEnabled()
    dlg.close()


def test_exportar_csv_respeita_filtro(nova_janela_erros, tmp_path):
    dlg = nova_janela_erros(str(tmp_path), ler=lambda _p: [])
    dlg.adicionar_json(json.dumps(erro("08:00:00", "sql", "Login falhou.", None, "18456")))
    dlg.adicionar_json(json.dumps(erro("09:00:00", "payload", "Falta pid; e \"aspas\".", "NOVUS/a",
                                       payload='{"a":1}')))
    dlg.descarregar()
    dlg.filtrar(categoria="payload")
    caminho = dlg.exportar_csv(tmp_path / "erros.csv")
    with caminho.open(encoding="utf-8-sig", newline="") as f:
        linhas = list(csv.reader(f, delimiter=";"))
    assert linhas[0] == ["Data/hora", "Categoria", "Mensagem", "Tópico", "Detalhe", "Payload"]
    assert len(linhas) == 2
    assert linhas[1][1:] == ["Payload", "Falta pid; e \"aspas\".", "NOVUS/a", "", '{"a":1}']
    assert linhas[1][0].endswith("09:00:00") and "/" in linhas[1][0]
    dlg.close()


def test_falha_ao_ler_arquivo_nao_derruba(nova_janela_erros, tmp_path):
    def ler(_p):
        raise OSError("sem permissão")

    dlg = nova_janela_erros(str(tmp_path), ler=ler)
    dlg.abrir()
    assert esperar(lambda: not dlg._carregando)
    assert dlg.total == 0
    dlg.close()
