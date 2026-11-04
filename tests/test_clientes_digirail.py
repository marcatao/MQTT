"""Aba Clientes com o DigiRail: colunas do diagnóstico, diálogo Zerar contadores, estados do botão da linha,
Atualizar diagnóstico, colunas ocultáveis e a categoria "comando" na janela de erros (QT_QPA_PLATFORM=offscreen)."""
from __future__ import annotations

import json
import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt  # noqa: E402
from PySide6.QtWidgets import QDialog  # noqa: E402
from PySide6.QtTest import QTest  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui.clientes import (  # noqa: E402
    COL_ACAO,
    COL_DIAG,
    COL_FILA,
    COL_FIRMWARE,
    COL_IP,
    COL_NOME,
    COL_SERIE,
    COL_ULTIMO,
    COL_WIFI,
    CHAVE_COLUNAS_OCULTAS,
    PainelClientes,
)
from app.gui.comandos import (  # noqa: E402
    AVISO_ZERAR,
    PAPEL_ACAO,
    PAPEL_WIFI,
    DialogoZerarContadores,
    barras_wifi,
    interpretar_diag,
    lista_canais,
    normalizar_rssi,
    numeros_canais,
)
from app.gui.erros import CATEGORIAS, JanelaErros  # noqa: E402
from app.gui.janela import JanelaPrincipal  # noqa: E402
from app.gui.widgets import PAPEL_PILULA  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_clientes import SAIDA, T0, Relogio, destruir, novo_painel  # noqa: E402,F401
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

# Exemplo do manual (p. 20–21, 101–102): RSSI em texto e curr_rssi POSITIVO; ipv4 em lista.
DIAG_MANUAL = {
    "error": 0, "title": "Pci v2", "location": "home",
    "curr_timestamp": int(T0) + 192, "cfg_timestamp": 1589311676,
    "fw_v": "1.23", "mqtt_queue": 1, "sn": "00000001",
    "curr_rssi": "55", "min_rssi": "-46", "max_rssi": "-87", "avg_rssi": "-54",
    "ipv4": [192, 168, 0, 23], "gateway": [192, 168, 0, 1], "mask": [255, 255, 255, 0],
}


def iso(t: float) -> str:
    return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def linha_de(painel: PainelClientes, nome: str) -> list[str]:
    return next(l for l in painel.linhas_visiveis() if l[COL_NOME] == nome)


def indice_de(painel: PainelClientes, nome: str, coluna: int):
    for r in range(painel.filtro.rowCount()):
        if painel.filtro.index(r, COL_NOME).data() == nome:
            return painel.filtro.index(r, coluna)
    raise AssertionError(nome)


# ---------------------------------------------------------------------- funções puras
def test_normalizar_rssi_string_positivo_e_negativo():
    assert normalizar_rssi("55") == -55          # curr_rssi do manual: texto e sem o sinal
    assert normalizar_rssi("-54") == -54
    assert normalizar_rssi(55) == -55
    assert normalizar_rssi(-71.6) == -72
    assert normalizar_rssi(" -60 dBm ") == -60
    for vazio in (None, "", "0", 0, "abc", "500", True):
        assert normalizar_rssi(vazio) is None, vazio


def test_interpretar_diag_do_manual():
    d = interpretar_diag(json.dumps(DIAG_MANUAL), T0)
    assert (d.sn, d.firmware, d.rssi, d.rssi_min, d.rssi_med, d.rssi_max) == ("00000001", "1.23", -55, -46, -54, -87)
    assert (d.fila, d.ip, d.titulo, d.local) == (1, "192.168.0.23", "Pci v2", "home")
    assert d.diferenca_relogio == 192
    # aceita a mensagem inteira (reported.diag), ipv4 em texto, e ignora 0.0.0.0
    d2 = interpretar_diag({"reported": {"diag": {"ipv4": "10.0.0.5", "sn": 42}}}, T0)
    assert (d2.ip, d2.sn, d2.rssi) == ("10.0.0.5", "42", None)
    assert interpretar_diag({"ipv4": [0, 0, 0, 0]}, T0).ip is None
    assert interpretar_diag("lixo", T0).sn is None


def test_barras_listas_e_canais():
    assert [barras_wifi(v) for v in (None, -50, -60, -65, -70, -80)] == [0, 4, 4, 3, 2, 1]
    assert lista_canais([2]) == "chd2"
    assert lista_canais([3, 1]) == "chd1 e chd3"
    assert lista_canais([1, 2, 3]) == "chd1, chd2 e chd3"
    assert numeros_canais(["chd1", "chd3_value", "chd3", "x", "chd9", 2]) == [1, 3, 2]


# ---------------------------------------------------------------------- colunas do diagnóstico
def test_colunas_preenchidas_pelo_diag(novo_painel):
    painel = novo_painel(relogio=Relogio(T0), executor_arp=lambda: "", locais=set())
    painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/events", iso(T0 - 3))
    linha = linha_de(painel, "Teste_Alumbra")
    assert [linha[c] for c in (COL_SERIE, COL_FIRMWARE, COL_WIFI, COL_FILA, COL_DIAG, COL_IP)] == ["—"] * 6

    painel.ao_diagnostico("Teste_Alumbra", json.dumps(DIAG_MANUAL))
    linha = linha_de(painel, "Teste_Alumbra")
    assert linha[COL_SERIE] == "00000001"
    assert linha[COL_FIRMWARE] == "1.23"
    assert linha[COL_WIFI] == "-55 dBm"
    assert linha[COL_FILA] == "1"
    assert linha[COL_IP] == "192.168.0.23"  # sem Mosquitto: o IP vem do diag
    assert linha[COL_DIAG] == datetime.fromtimestamp(T0).strftime("%H:%M:%S")

    assert indice_de(painel, "Teste_Alumbra", COL_WIFI).data(PAPEL_WIFI) == -55
    dica_wifi = indice_de(painel, "Teste_Alumbra", COL_WIFI).data(Qt.ItemDataRole.ToolTipRole)
    assert "Mínimo: -46 dBm" in dica_wifi and "Média: -54 dBm" in dica_wifi and "Máximo: -87 dBm" in dica_wifi
    assert indice_de(painel, "Teste_Alumbra", COL_FILA).data(PAPEL_PILULA) == "aviso"  # fila > 0: âmbar
    assert "diagnóstico" in indice_de(painel, "Teste_Alumbra", COL_IP).data(Qt.ItemDataRole.ToolTipRole)
    dica_nome = indice_de(painel, "Teste_Alumbra", COL_NOME).data(Qt.ItemDataRole.ToolTipRole)
    assert "Pci v2" in dica_nome and "home" in dica_nome
    assert "3 min 12 s adiantado" in dica_nome


def test_sem_rssi_mostra_cabo_e_fila_zero_sem_destaque(novo_painel):
    painel = novo_painel(relogio=Relogio(T0), executor_arp=lambda: "", locais=set())
    painel.ao_diagnostico("dev", json.dumps({"sn": "7", "mqtt_queue": 0, "curr_timestamp": int(T0) - 2}))
    linha = linha_de(painel, "dev")  # o diag cria a linha mesmo antes de qualquer leitura
    assert (linha[COL_WIFI], linha[COL_FILA]) == ("cabo", "0")
    assert indice_de(painel, "dev", COL_FILA).data(PAPEL_PILULA) is None
    assert "acertado" in indice_de(painel, "dev", COL_NOME).data(Qt.ItemDataRole.ToolTipRole)


def test_ip_do_mosquitto_tem_prioridade_sobre_o_diag(novo_painel):
    painel = novo_painel(relogio=Relogio(T0 + 20), executor_arp=lambda: "", locais=set())
    painel.ao_saida_mosquitto("saida", SAIDA)
    painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/a", iso(T0 + 16))
    painel.ao_diagnostico("Teste_Alumbra", json.dumps(DIAG_MANUAL))
    assert linha_de(painel, "Teste_Alumbra")[COL_IP] == "192.168.100.50"
    assert [c.nome for c in painel.registro.linhas].count("Teste_Alumbra") == 1  # o diag não duplica a linha


# ---------------------------------------------------------------------- diálogo
@pytest.fixture
def novo_dialogo(qapp):
    criados = []

    def _novo(*a, **kw):
        d = DialogoZerarContadores(*a, **kw)
        criados.append(d)
        return d

    yield _novo
    destruir(criados)


def test_dialogo_canais_vistos_primeiro_e_destacados(novo_dialogo):
    dlg = novo_dialogo("Teste_Alumbra", ["chd3", "chd1"], confirmar=lambda *a: True)
    assert dlg.windowTitle() == "Zerar contadores — Teste_Alumbra"
    assert dlg.aviso.text() == AVISO_ZERAR
    assert [n for n in dlg.caixas] == [3, 1, 2, 4, 5, 6]  # vistos primeiro
    assert {n for n, c in dlg.caixas.items() if c.property("visto")} == {1, 3}
    assert not any(c.isChecked() for c in dlg.caixas.values())  # nenhuma vem marcada


def test_dialogo_sem_selecao_desabilita_e_chd2_confirma(novo_dialogo):
    pedidos = []

    def confirmar(_pai, device, canais):
        pedidos.append((device, canais))
        return True

    dlg = novo_dialogo("dev", ["chd1", "chd2"], confirmar=confirmar)
    assert not dlg.botao_zerar.isEnabled()
    dlg.caixas[2].setChecked(True)
    assert dlg.botao_zerar.isEnabled()
    dlg.botao_zerar.click()
    assert pedidos == [("dev", [2])]
    assert dlg.result() == QDialog.DialogCode.Accepted and dlg.canais_escolhidos() == [2]


def test_dialogo_marcar_todos(novo_dialogo):
    dlg = novo_dialogo("dev", [], confirmar=lambda *a: True)
    assert dlg.botao_todos.text() == "Marcar todos"
    dlg.botao_todos.click()
    assert dlg.canais_escolhidos() == [1, 2, 3, 4, 5, 6]
    assert dlg.botao_todos.text() == "Desmarcar todos"
    dlg.botao_todos.click()
    assert dlg.canais_escolhidos() == [] and not dlg.botao_zerar.isEnabled()


def test_dialogo_cancelar_confirmacao_nao_aceita(novo_dialogo):
    dlg = novo_dialogo("dev", [], confirmar=lambda *a: False)
    dlg.caixas[4].setChecked(True)
    dlg.botao_zerar.click()
    assert dlg.result() != QDialog.DialogCode.Accepted


# ---------------------------------------------------------------------- na janela, com o bridge falso
@pytest.fixture
def janela(qapp, tmp_path):
    criadas = []

    def _montar(**kw_bridge):
        config = Config()
        config.mosquitto.iniciar_automaticamente = False
        kw_bridge.setdefault("responder_comandos", False)
        bridge = FakePipelineBridge(config, intervalo=10.0, **kw_bridge)  # sem leituras simuladas no meio
        j = JanelaPrincipal(bridge, config, settings=QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat),
                            tema="claro", cofre=CofreFalso(), executor_arp=lambda: "", enderecos_locais=set())
        j.resize(1440, 900)
        j.show()
        j.abas.setCurrentIndex(2)
        criadas.append(j)
        return j, bridge

    yield _montar
    destruir(criadas)


def conectar(j) -> None:
    j.botao_conectar.click()
    assert esperar(lambda: j.estado == "conectado", 3)


def com_dispositivos(j, bridge, *nomes) -> None:
    for n in nomes:
        bridge.emitir_dispositivo(n, f"NOVUS/{n}/events")
    assert esperar(lambda: {l[COL_NOME] for l in j.painel_clientes.linhas_visiveis()} >= set(nomes))


def roteiro(canais, confirmar=True):
    """executar_dialogo falso: marca os canais, clica em Zerar selecionados e devolve o resultado."""
    vistos = []

    def executar(dlg):
        vistos.append(dlg)
        dlg._confirmar = lambda *a: confirmar
        for n in canais:
            dlg.caixas[n].setChecked(True)
        dlg.botao_zerar.click()
        return dlg.result()

    return executar, vistos


def clicar_acao(painel, nome) -> None:
    rect = painel.tabela.visualRect(indice_de(painel, nome, COL_ACAO))
    pos = rect.center()
    pos.setX(rect.left() + 20)  # dentro do botão (alinhado à esquerda da célula)
    QTest.mouseClick(painel.tabela.viewport(), Qt.MouseButton.LeftButton, pos=pos)


def zerados(bridge):
    return [c for c in bridge.chamadas if c[0] == "zerar_contadores"]


def test_botao_desabilitado_desconectado(janela):
    j, bridge = janela()
    com_dispositivos(j, bridge, "dev")
    painel = j.painel_clientes
    indice = indice_de(painel, "dev", COL_ACAO)
    assert indice.data(PAPEL_ACAO) == ("desabilitado", "Zerar contadores")
    assert "Conecte ao broker" in indice.data(Qt.ItemDataRole.ToolTipRole)
    executar, vistos = roteiro([2])
    painel.executar_dialogo = executar
    clicar_acao(painel, "dev")
    assert vistos == [] and zerados(bridge) == []
    assert not painel.botao_diag.isEnabled() and "Conecte" in painel.botao_diag.toolTip()
    conectar(j)
    assert indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO) == ("pronto", "Zerar contadores")


def test_clique_zera_e_mostra_estados(janela):
    j, bridge = janela()
    conectar(j)
    com_dispositivos(j, bridge, "dev")
    painel = j.painel_clientes
    bridge.emitir_canais("dev", ["chd1", "chd2", "chd3"])
    executar, vistos = roteiro([2])
    painel.executar_dialogo = executar
    clicar_acao(painel, "dev")
    assert len(vistos) == 1 and vistos[0].device_id == "dev"
    assert vistos[0].canais_vistos == [1, 2, 3]
    assert zerados(bridge) == [("zerar_contadores", "dev", [2])]
    assert indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO) == ("enviando", "Zerando…")

    bridge.emitir_resposta("dev", "reset_counters", True, "Contadores zerados (chd2).")
    hora = datetime.now().strftime("%H:%M")
    assert esperar(lambda: indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO)[0] == "ok")
    _estado, texto = indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO)
    assert texto in (f"Zerado às {hora}", f"Zerado às {datetime.now():%H:%M}")  # virada de minuto no meio
    j.logs.descarregar()
    assert "zerados" not in j.logs.texto()  # o log do resultado é do core; a tela não duplica

    # de novo, agora falhando: "Falhou" com a mensagem na dica (sem log da tela)
    painel.executar_dialogo, _ = roteiro([1, 3])
    clicar_acao(painel, "dev")
    assert zerados(bridge)[-1] == ("zerar_contadores", "dev", [1, 3])
    bridge.emitir_resposta("dev", "reset_counters", False, "Sem resposta do dispositivo em 15 s.")
    assert esperar(lambda: indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO) == ("falha", "Falhou"))
    dica = indice_de(painel, "dev", COL_ACAO).data(Qt.ItemDataRole.ToolTipRole)
    assert "Sem resposta do dispositivo em 15 s." in dica and "chd1 e chd3" in dica
    j.logs.descarregar()
    assert "Falha ao zerar" not in j.logs.texto()


def test_canais_vistos_chegam_ao_dialogo(janela):
    j, bridge = janela()
    conectar(j)
    com_dispositivos(j, bridge, "dev")
    bridge.emitir_canais("dev", ["chd2", "chd5"])
    painel = j.painel_clientes
    assert esperar(lambda: painel.registro.canais.get("dev") == [2, 5])
    vistos = []
    painel.executar_dialogo = lambda dlg: (vistos.append(dlg), QDialog.DialogCode.Rejected)[1]
    assert painel.zerar("dev") is False
    dlg = vistos[0]
    assert [n for n in dlg.caixas][:2] == [2, 5]
    assert dlg.caixas[2].property("visto") and dlg.caixas[5].property("visto")
    assert not dlg.caixas[1].property("visto")


def test_cancelar_a_confirmacao_nao_envia(janela):
    j, bridge = janela()
    conectar(j)
    com_dispositivos(j, bridge, "dev")
    painel = j.painel_clientes
    painel.executar_dialogo, vistos = roteiro([2], confirmar=False)
    clicar_acao(painel, "dev")
    assert len(vistos) == 1 and zerados(bridge) == []
    assert indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO) == ("pronto", "Zerar contadores")


def test_fluxo_completo_com_resposta_do_bridge(janela):
    j, bridge = janela(responder_comandos=True, atraso_resposta=0.05)
    conectar(j)
    com_dispositivos(j, bridge, "dev")
    painel = j.painel_clientes
    painel.executar_dialogo, _ = roteiro([1, 2, 3, 4, 5, 6])
    assert painel.zerar("dev")
    assert zerados(bridge) == [("zerar_contadores", "dev", [1, 2, 3, 4, 5, 6])]
    assert esperar(lambda: indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO)[0] == "ok")


def test_sem_resposta_vira_falha_na_tela(janela):
    j, bridge = janela()
    conectar(j)
    com_dispositivos(j, bridge, "dev")
    painel = j.painel_clientes
    painel.executar_dialogo, _ = roteiro([2])
    painel.zerar("dev")
    painel.modelo.comandos["dev"].quando -= 31  # passou o tempo de proteção
    painel.tique()
    assert indice_de(painel, "dev", COL_ACAO).data(PAPEL_ACAO) == ("falha", "Falhou")
    j.logs.descarregar()
    assert "Zerar contadores chd2 em dev: sem resposta do integrador em 30 s" in j.logs.texto()
    assert "AVISO" in next(l for l in j.logs.texto().splitlines() if "sem resposta do integrador" in l)
    painel.tique()  # não repete o aviso
    j.logs.descarregar()
    assert j.logs.texto().count("sem resposta do integrador") == 1


def test_atualizar_diagnostico_para_todos_e_pelo_menu(janela):
    j, bridge = janela()
    painel = j.painel_clientes
    com_dispositivos(j, bridge, "a", "b", "c")
    assert not painel.botao_diag.isEnabled()
    conectar(j)
    assert painel.botao_diag.isEnabled()
    painel.botao_diag.click()
    pedidos = [c[1] for c in bridge.chamadas if c[0] == "pedir_diagnostico"]
    assert sorted(pedidos) == ["a", "b", "c"]
    assert linha_de(painel, "b")[COL_DIAG] == "pedido…"
    bridge.emitir_diag("b", DIAG_MANUAL)
    assert esperar(lambda: linha_de(painel, "b")[COL_SERIE] == "00000001")
    assert linha_de(painel, "b")[COL_DIAG] != "pedido…"

    menu = painel.menu_da_linha(indice_de(painel, "c", COL_NOME))
    acao = next(a for a in menu.actions() if a.text() == "Atualizar diagnóstico deste dispositivo")
    assert acao.isEnabled()
    acao.trigger()
    pedidos = [c[1] for c in bridge.chamadas if c[0] == "pedir_diagnostico"]
    assert pedidos.count("c") == 2 and pedidos.count("a") == 1
    menu.deleteLater()


def test_ordenacao_mantem_o_botao_na_linha_certa(janela):
    j, bridge = janela()
    conectar(j)
    com_dispositivos(j, bridge, "alfa", "beta", "gama")
    painel = j.painel_clientes
    painel.ao_resposta_comando("beta", "reset_counters", True, "ok")  # beta com "Zerado às …"
    for ordem in (Qt.SortOrder.AscendingOrder, Qt.SortOrder.DescendingOrder):
        painel.tabela.sortByColumn(COL_NOME, ordem)
        nomes = [painel.filtro.index(r, COL_NOME).data() for r in range(painel.filtro.rowCount())]
        assert nomes == sorted(nomes, reverse=ordem == Qt.SortOrder.DescendingOrder)
        for r, nome in enumerate(nomes):
            estado = painel.filtro.index(r, COL_ACAO).data(PAPEL_ACAO)[0]
            assert estado == ("ok" if nome == "beta" else "pronto")
        # clicar no botão da primeira linha visual abre o diálogo do dispositivo daquela linha
        vistos = []
        painel.executar_dialogo = lambda dlg: (vistos.append(dlg.device_id), QDialog.DialogCode.Rejected)[1]
        rect = painel.tabela.visualRect(painel.filtro.index(0, COL_ACAO))
        pos = rect.center()
        pos.setX(rect.left() + 20)
        QTest.mouseClick(painel.tabela.viewport(), Qt.MouseButton.LeftButton, pos=pos)
        assert vistos == [nomes[0]]


def test_colunas_ocultas_pelo_menu_e_lembradas(janela, tmp_path):
    j, _bridge = janela()
    painel = j.painel_clientes
    assert painel.tabela.isColumnHidden(COL_ULTIMO)  # oculta por padrão
    menu = painel._montar_menu_colunas()
    acoes = {a.text(): a for a in menu.actions()}
    assert not acoes["Nome"].isEnabled() and not acoes["Ação"].isEnabled()
    assert painel.tabela.isColumnHidden(COL_FIRMWARE) and not painel.tabela.isColumnHidden(COL_WIFI)
    acoes["Wi-Fi"].setChecked(False)
    acoes["Firmware"].setChecked(True)
    assert painel.tabela.isColumnHidden(COL_WIFI) and not painel.tabela.isColumnHidden(COL_FIRMWARE)
    valor = j.settings.value(CHAVE_COLUNAS_OCULTAS)
    assert "Wi-Fi" in ([valor] if isinstance(valor, str) else list(valor))
    outro = PainelClientes(relogio=Relogio(), executor_arp=lambda: "", locais=set(), settings=j.settings)
    try:
        assert outro.tabela.isColumnHidden(COL_WIFI) and not outro.tabela.isColumnHidden(COL_FIRMWARE)
    finally:
        destruir([outro])


# ---------------------------------------------------------------------- janela de erros
def test_categoria_comando_na_janela_de_erros(qapp, tmp_path):
    assert CATEGORIAS["comando"][2] == "ciano"
    dlg = JanelaErros(str(tmp_path), ler=lambda _p: [])
    try:
        assert dlg.combo_categoria.findData("comando") >= 0
        assert dlg.combo_categoria.itemText(dlg.combo_categoria.findData("comando")) == "Comandos ao dispositivo"
        base = {"quando": "2026-11-04T14:31:00", "topico": "NOVUS/dev/command", "detalhe": None, "payload": None}
        dlg.adicionar_json(json.dumps({**base, "categoria": "comando", "mensagem": "Falha ao zerar chd2 em dev."}))
        dlg.adicionar_json(json.dumps({**base, "categoria": "payload", "mensagem": "Falta pid."}))
        dlg.descarregar()
        dlg.filtrar(categoria="comando")
        assert [r.mensagem for r in dlg.registros_visiveis()] == ["Falha ao zerar chd2 em dev."]
        indice = dlg.filtro.index(0, 1)
        assert indice.data() == "Comando" and indice.data(PAPEL_PILULA) == "ciano"
        cor_texto, cor_fundo = dlg.delegado.cores("ciano")
        assert cor_texto and cor_fundo and cor_texto != dlg.delegado.cores("neutro")[0]
    finally:
        destruir([dlg])
