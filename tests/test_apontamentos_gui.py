"""Aba Apontamentos reais: posição na janela, tabela a partir do JSON do ciclo, cores por legenda, periodicidade
salva e enviada ao bridge, Executar agora e ciclo com falha (QT_QPA_PLATFORM=offscreen).

O bridge de teste é local (subclasse do falso de sempre com o sinal e os métodos novos), para não depender de
tests/fake_bridge.py já ter o contrato dos apontamentos."""
from __future__ import annotations

import json
import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt, Signal  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui import estilo  # noqa: E402
from app.gui.apontamentos import (  # noqa: E402
    CHAVE_SETTINGS,
    COL_DESDE,
    COL_ESTADO,
    COL_MAQUINA,
    COL_PRODUCAO,
    PainelApontamentos,
    TEXTO_AGUARDANDO,
    formatar_data,
    interpretar_ciclo,
    tom_legenda,
)
from app.gui.janela import JanelaPrincipal  # noqa: E402
from app.gui.widgets import PAPEL_PILULA, DelegadoPilula  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_clientes import destruir  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

T0 = datetime(2026, 11, 12, 14, 0, 0).timestamp()


class Relogio:
    def __init__(self, t: float = T0):
        self.t = t

    def __call__(self) -> float:
        return self.t


class BridgeApontamentos(FakePipelineBridge):
    """Falso com o contrato dos apontamentos: registra as chamadas, não roda nada."""

    apontamentos_atualizados = Signal(str)

    def __init__(self, config, **kw):
        super().__init__(config, **kw)
        self.periodicidades: list[float] = []
        self.execucoes = 0

    def definir_periodicidade_apontamentos(self, minutos: float) -> None:
        self.periodicidades.append(minutos)

    def executar_apontamentos_agora(self) -> None:
        self.execucoes += 1


def maquina(**kw) -> dict:
    base = {"maquina_id": 1, "codigo": "INJ01", "nome": "Injetora 01", "canais": "Teste_Alumbra/chd1_value",
            "micro_s": 60, "macro_s": 300, "legenda": "2", "legenda_descricao": "Produção Normal",
            "desde": "2026-11-12T13:40:05", "ultima_producao": "2026-11-12T13:59:50", "apontamentos_hoje": 7,
            "ultima_leitura": "2026-11-12T13:59:58", "aviso": None}
    base.update(kw)
    return base


def ciclo(maquinas=None, ok=True, mensagem="", proxima="2026-11-12T14:03:00", **kw) -> str:
    d = {"quando": "2026-11-12T14:00:00", "proxima": proxima, "minutos": 3.0, "ok": ok, "mensagem": mensagem,
         "criados": 2, "alterados": 5, "excluidos": 1, "maquinas": maquinas if maquinas is not None else []}
    d.update(kw)
    return json.dumps(d)


TRES = [
    maquina(),
    maquina(maquina_id=2, codigo="INJ02", nome="Injetora 02", legenda="99", legenda_descricao="Microparada"),
    maquina(maquina_id=3, codigo="INJ03", nome="Injetora 03", legenda="98", legenda_descricao="Macro parada",
            aviso="Sem leitura há 20 min"),
    maquina(maquina_id=4, codigo="INJ04", nome="Injetora 04", legenda="7", legenda_descricao="Setup"),
]


@pytest.fixture
def novo_painel(qapp, tmp_path):
    criados = []

    def _novo(**kw):
        kw.setdefault("relogio", Relogio())
        kw.setdefault("settings", QSettings(str(tmp_path / "p.ini"), QSettings.Format.IniFormat))
        p = PainelApontamentos(**kw)
        criados.append(p)
        return p

    yield _novo
    destruir(criados)


@pytest.fixture
def janela(qapp, tmp_path):
    criadas = []

    def _montar(bridge_cls=BridgeApontamentos, settings=None, config=None):
        config = config or Config()
        config.mosquitto.iniciar_automaticamente = False
        bridge = bridge_cls(config)
        settings = settings or QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat)
        j = JanelaPrincipal(bridge, config, settings=settings, tema="claro", cofre=CofreFalso(),
                            executor_arp=lambda: "", enderecos_locais=set(), relogio=Relogio())
        j.show()
        criadas.append(j)
        return j, bridge, settings

    yield _montar
    destruir(criadas)


# ---------------------------------------------------------------------- funções puras
def test_formatar_data_e_tons():
    assert formatar_data("2026-11-12T13:40:05") == "12/11 13:40:05"
    assert formatar_data(None) == "—"
    assert formatar_data("ontem") == "ontem"
    assert [tom_legenda(c) for c in ("2", "99", "98", "7", None)] == ["sucesso", "aviso", "erro", "neutro", "neutro"]


def test_interpretar_ciclo_tolera_lixo():
    c = interpretar_ciclo("{não é json")
    assert not c.ok and "ilegível" in c.mensagem
    c = interpretar_ciclo(json.dumps({"ok": True, "maquinas": [{"codigo": "X", "micro_s": "45"}, 3]}))
    assert c.ok and len(c.maquinas) == 1 and c.maquinas[0].micro_s == 45 and c.maquinas[0].legenda == ""


# ---------------------------------------------------------------------- painel
def test_estado_vazio_e_json_popula_tabela(novo_painel):
    p = novo_painel()
    assert p.pilha.currentWidget() is p.vazio and TEXTO_AGUARDANDO in p.vazio.text()
    assert p.chip_ultima.valor() == "—"
    p.ao_ciclo(ciclo(TRES))
    assert p.pilha.currentWidget() is p.tabela
    linhas = p.linhas_visiveis()
    assert [l[COL_MAQUINA] for l in linhas] == ["INJ01 – Injetora 01", "INJ02 – Injetora 02", "INJ03 – Injetora 03",
                                                "INJ04 – Injetora 04"]
    primeira = linhas[0]
    assert primeira[1] == "Teste_Alumbra/chd1_value"
    assert primeira[2:5] == ["60", "300", "2 · Produção Normal"]
    assert primeira[COL_DESDE] == "12/11 13:40:05" and primeira[COL_PRODUCAO] == "12/11 13:59:50"
    assert primeira[7] == "7" and primeira[8] == "12/11 13:59:58"
    assert p.resumo.isVisibleTo(p) and p.resumo.text() == "Último ciclo: 2 criados · 5 atualizados · 1 excluído"
    assert p.chip_ultima.valor() == "14:00:00 · ok" and p.chip_ultima.estado == "ok"
    assert not p.falha.isVisibleTo(p)


def test_cores_por_legenda_e_aviso(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo(TRES))
    m = p.filtro
    tons = {m.index(r, COL_MAQUINA).data(): m.index(r, COL_ESTADO).data(PAPEL_PILULA) for r in range(m.rowCount())}
    assert tons == {"INJ01 – Injetora 01": "sucesso", "INJ02 – Injetora 02": "aviso",
                    "INJ03 – Injetora 03": "erro", "INJ04 – Injetora 04": "neutro"}
    # Cores do tema (claro e escuro) vêm da paleta, via DelegadoPilula
    for paleta in (estilo.CLARA, estilo.ESCURA):
        p.definir_paleta(paleta)
        d: DelegadoPilula = p.delegado
        assert d.cores("sucesso") == (paleta.sucesso, paleta.sucesso_suave)
        assert d.cores("aviso") == (paleta.aviso, paleta.aviso_suave)
        assert d.cores("erro") == (paleta.erro, paleta.erro_suave)
        assert d.cores("neutro") == (paleta.texto_sec, paleta.neutro_suave)
    # Aviso: ícone na coluna Máquina e texto na dica
    linha_aviso = next(r for r in range(m.rowCount()) if m.index(r, COL_MAQUINA).data() == "INJ03 – Injetora 03")
    assert m.index(linha_aviso, COL_MAQUINA).data(Qt.ItemDataRole.DecorationRole) is not None
    assert "Sem leitura há 20 min" in m.index(linha_aviso, COL_DESDE).data(Qt.ItemDataRole.ToolTipRole)
    assert m.index(0, COL_MAQUINA).data(Qt.ItemDataRole.DecorationRole) is None


def test_ordenar_por_estado(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo(TRES))
    p.tabela.sortByColumn(COL_ESTADO, Qt.SortOrder.AscendingOrder)
    assert [l[COL_ESTADO].split(" ")[0] for l in p.linhas_visiveis()] == ["98", "99", "7", "2"]


def test_contagem_regressiva_da_proxima(novo_painel):
    relogio = Relogio()
    p = novo_painel(relogio=relogio)
    p.ao_ciclo(ciclo(TRES, proxima="2026-11-12T14:03:00"))
    assert p.chip_proxima.valor() == "3:00"
    relogio.t += 75
    p.tique()
    assert p.chip_proxima.valor() == "1:45"
    relogio.t += 200
    p.tique()
    assert p.chip_proxima.valor() == "em instantes"


def test_ciclo_com_falha_mostra_mensagem_e_mantem_tabela(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo([], ok=False, mensagem="Sem permissão no banco OEE."))
    assert p.pilha.currentWidget() is p.vazio
    assert TEXTO_AGUARDANDO in p.vazio.text() and "Sem permissão no banco OEE." in p.vazio.text()
    assert p.falha.isVisibleTo(p) and "Sem permissão" in p.falha.texto()
    assert p.chip_ultima.valor() == "14:00:00 · falhou" and p.chip_ultima.estado == "erro"
    # Um ciclo bom e depois uma falha sem máquinas: a tabela anterior continua
    p.ao_ciclo(ciclo(TRES))
    assert not p.falha.isVisibleTo(p)
    p.ao_ciclo(ciclo([], ok=False, mensagem="Tempo esgotado."))
    assert p.pilha.currentWidget() is p.tabela and len(p.linhas_visiveis()) == 4
    assert "Tempo esgotado." in p.falha.texto()


def test_ok_sem_maquinas_explica_o_que_falta(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo([]))
    assert p.pilha.currentWidget() is p.vazio and "Nenhuma máquina" in p.vazio.text()


def test_periodicidade_padrao_da_config_e_salva(novo_painel, tmp_path):
    settings = QSettings(str(tmp_path / "x.ini"), QSettings.Format.IniFormat)
    p = novo_painel(settings=settings, minutos_padrao=5)
    assert p.minutos() == 5
    pedidos = []
    p.periodicidade_alterada.connect(pedidos.append)
    p.campo_minutos.setValue(12)
    assert esperar(lambda: pedidos == [12.0])
    assert int(settings.value(CHAVE_SETTINGS)) == 12
    p2 = novo_painel(settings=settings, minutos_padrao=5)
    assert p2.minutos() == 12  # QSettings vale mais que o config
    assert novo_painel(settings=QSettings(str(tmp_path / "y.ini"), QSettings.Format.IniFormat)).minutos() == 3


# ---------------------------------------------------------------------- na janela
def test_aba_depois_de_clientes(janela):
    j, _bridge, _ = janela()
    nomes = [j.abas.tabText(i) for i in range(j.abas.count())]
    assert nomes.index("Apontamentos reais") == nomes.index("Clientes") + 1
    assert j.abas.widget(nomes.index("Apontamentos reais")) is j.painel_apontamentos


def test_janela_liga_sinal_e_reaplica_periodicidade(janela, tmp_path):
    settings = QSettings(str(tmp_path / "s.ini"), QSettings.Format.IniFormat)
    settings.setValue(CHAVE_SETTINGS, 7)
    j, bridge, _ = janela(settings=settings)
    assert bridge.periodicidades == [7.0]  # reaplicada ao abrir
    assert j.painel_apontamentos.minutos() == 7
    j.painel_apontamentos.campo_minutos.setValue(10)
    assert esperar(lambda: bridge.periodicidades == [7.0, 10.0])
    assert int(settings.value(CHAVE_SETTINGS)) == 10
    bridge.apontamentos_atualizados.emit(ciclo(TRES))
    assert esperar(lambda: len(j.painel_apontamentos.linhas_visiveis()) == 4)


def test_executar_agora_desabilita_ate_proximo_sinal(janela):
    j, bridge, _ = janela()
    p = j.painel_apontamentos
    assert p.botao_executar.isEnabled()
    p.botao_executar.click()
    assert bridge.execucoes == 1
    assert not p.botao_executar.isEnabled() and p.botao_executar.text() == "Executando…"
    assert p.chip_proxima.valor() == "executando…"
    p.botao_executar.click()
    assert bridge.execucoes == 1  # desabilitado: não pede de novo
    bridge.apontamentos_atualizados.emit(ciclo(TRES))
    assert esperar(lambda: p.botao_executar.isEnabled())
    assert p.botao_executar.text() == "Executar agora"


def test_executar_agora_tempo_limite(novo_painel):
    p = novo_painel()
    assert p.executar_agora()
    assert p._timer_limite.isActive() and p._timer_limite.interval() == 120_000
    p._sem_resposta()
    assert p.botao_executar.isEnabled() and not p.executando


def test_bridge_sem_apontamentos_desabilita(janela):
    j, bridge, _ = janela(bridge_cls=FakePipelineBridge)
    p = j.painel_apontamentos
    if hasattr(bridge, "executar_apontamentos_agora"):
        pytest.skip("o bridge falso já tem o contrato dos apontamentos")
    assert not p.botao_executar.isEnabled() and not p.campo_minutos.isEnabled()
    assert "não tem o processo" in p.vazio.text()
