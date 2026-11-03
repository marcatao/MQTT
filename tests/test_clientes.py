"""Aba Clientes: parser do `mosquitto -v`, junção device_id ↔ client id, ARP/MAC, filtro de assinantes,
inatividade com relógio falso e exportação CSV (QT_QPA_PLATFORM=offscreen)."""
from __future__ import annotations

import csv
import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt  # noqa: E402
from PySide6.QtGui import QColor  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui import estilo  # noqa: E402
from app.gui.clientes import (  # noqa: E402
    COL_HA,
    COL_IP,
    COL_MAC,
    COL_MENSAGENS,
    COL_NOME,
    COL_STATUS,
    COL_TOPICO,
    COL_ULTIMO,
    ESTE_COMPUTADOR,
    PainelClientes,
    RegistroClientes,
    interpretar_arp,
    interpretar_linha,
    normalizar_ip,
    normalizar_mac,
    separar_endereco,
    texto_ha,
)
from app.gui.janela import JanelaPrincipal  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

T0 = 1_793_386_700.0  # 2026-10-30, epoch "real" do NOVUS


@pytest.fixture
def novo_painel(qapp):
    """Cria painéis e os destrói na thread da GUI ao fim do teste (sem pai, o coletor de ciclos do Python poderia
    destruí-los em outra thread)."""
    criados = []

    def _novo(**kw):
        p = PainelClientes(**kw)
        criados.append(p)
        return p

    yield _novo
    destruir(criados)


def destruir(widgets) -> None:
    import gc

    from PySide6.QtCore import QCoreApplication, QEvent

    for w in widgets:
        w.close()
        w.deleteLater()
    QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    widgets.clear()
    gc.collect()


class Relogio:
    def __init__(self, t: float = T0):
        self.t = t

    def __call__(self) -> float:
        return self.t


# ---------------------------------------------------------------------- parser do mosquitto -v
def test_nova_conexao_com_e_sem_porta_do_cliente():
    ev = interpretar_linha("1730650012: New connection from 192.168.100.50:51544 on port 1883.")
    assert (ev.tipo, ev.ip, ev.porta, ev.epoch) == ("conexao", "192.168.100.50", 51544, 1730650012.0)
    ev = interpretar_linha("1730650012: New connection from 192.168.100.51 on port 1883.")
    assert (ev.tipo, ev.ip, ev.porta) == ("conexao", "192.168.100.51", None)


def test_cliente_conectado_com_e_sem_usuario():
    ev = interpretar_linha("1730650012: New client connected from 192.168.100.50:51544 as NOVUS_01 (p2, c1, k60).")
    assert (ev.tipo, ev.ip, ev.porta, ev.client_id, ev.usuario) == (
        "conectado", "192.168.100.50", 51544, "NOVUS_01", None)
    ev = interpretar_linha(
        "1730650013: New client connected from 10.0.0.7:40000 as logbox-02 (p2, c1, k60, u'novus').")
    assert (ev.client_id, ev.usuario, ev.ip) == ("logbox-02", "novus", "10.0.0.7")


def test_publish_subscribe_e_desconexoes():
    ev = interpretar_linha("1730650020: Received PUBLISH from NOVUS_01 (d0, q0, r0, m0, "
                           "'NOVUS/Teste_Alumbra/events', ... (128 bytes))")
    assert (ev.tipo, ev.client_id, ev.topico, ev.tamanho, ev.epoch) == (
        "publish", "NOVUS_01", "NOVUS/Teste_Alumbra/events", 128, 1730650020.0)
    ev = interpretar_linha("1730650021: Received PUBLISH from x (d0, q2, r1, m7, 'a/b c', ... (0 bytes))")
    assert (ev.client_id, ev.topico, ev.tamanho) == ("x", "a/b c", 0)
    assert interpretar_linha("1730650014: Received SUBSCRIBE from mqtt-sqlserver-4f2a").tipo == "subscribe"
    for linha in ("1730650090: Client NOVUS_01 disconnected.",
                  "1730650090: Client NOVUS_01 closed its connection.",
                  "1730650090: Client NOVUS_01 has exceeded timeout, disconnecting."):
        ev = interpretar_linha(linha)
        assert (ev.tipo, ev.client_id) == ("desconectado", "NOVUS_01"), linha


def test_ipv6_e_ipv4_mapeado():
    ev = interpretar_linha("1730650012: New client connected from [::1]:51551 as local (p2, c1, k60).")
    assert (ev.ip, ev.porta) == ("::1", 51551)
    ev = interpretar_linha("1730650012: New client connected from ::ffff:192.168.0.10:51551 as dev (p2, c1, k60).")
    assert (ev.ip, ev.porta) == ("192.168.0.10", 51551)
    assert normalizar_ip("::ffff:192.168.0.10") == "192.168.0.10"
    assert separar_endereco("[::ffff:192.168.0.10]:80") == ("192.168.0.10", 80)
    assert separar_endereco("::1") == ("::1", None)


def test_linhas_que_nao_casam_sao_ignoradas():
    for linha in ("", "1730650000: mosquitto version 2.0.18 starting", "1730650012: No will message specified.",
                  "1730650012: Sending CONNACK to NOVUS_01 (0, 0)",
                  "1730650020: Sending PUBLISH to mqtt-sqlserver-4f2a (d0, q0, r0, m0, 'a', ... (1 bytes))",
                  "1730650074: Received PINGREQ from NOVUS_01", "NOVUS/x {\"pid\":1}", "lixo qualquer"):
        assert interpretar_linha(linha) is None, linha


def test_linha_sem_prefixo_de_epoch():
    ev = interpretar_linha("Received PUBLISH from a (d0, q0, r0, m0, 't', ... (3 bytes))")
    assert ev.tipo == "publish" and ev.epoch is None


# ---------------------------------------------------------------------- registro e junção pelo tópico
SAIDA = f"""{int(T0)}: New connection from 192.168.100.50:51544 on port 1883.
{int(T0)}: New client connected from 192.168.100.50:51544 as NOVUS_A (p2, c1, k60).
{int(T0)}: New client connected from 192.168.100.51:40000 as NOVUS_B (p2, c1, k60, u'novus').
{int(T0)}: New client connected from 127.0.0.1:51551 as mqtt-sqlserver-4f2a (p5, c1, k60).
{int(T0)}: Received SUBSCRIBE from mqtt-sqlserver-4f2a
{int(T0) + 5}: Received PUBLISH from NOVUS_A (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/a', ... (128 bytes))
{int(T0) + 6}: Received PUBLISH from NOVUS_B (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/b', ... (128 bytes))
{int(T0) + 15}: Received PUBLISH from NOVUS_A (d0, q0, r0, m0, 'NOVUS/Teste_Alumbra/a', ... (128 bytes))"""


def test_registro_conta_e_liga_device_id_pelo_topico():
    r = RegistroClientes()
    r.processar_texto(SAIDA, T0)
    a, b = r.cliente("NOVUS_A"), r.cliente("NOVUS_B")
    assert (a.ip, a.mensagens, a.ultimo_envio, a.status) == ("192.168.100.50", 2, T0 + 15, "Conectado")
    assert (b.usuario, b.mensagens) == ("novus", 1)
    assert r.cliente("mqtt-sqlserver-4f2a").status == "Assinante"
    assert set(r.retirar_ips_novos()) == {"192.168.100.50", "192.168.100.51", "127.0.0.1"}

    # o device_id vai para o cliente que publicou por último no MESMO tópico
    r.dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/a", T0 + 16)
    assert a.nome == "Teste_Alumbra" and b.nome is None
    assert a.nome_exibido == "Teste_Alumbra" and b.nome_exibido == "NOVUS_B"
    assert len(r.linhas) == 3  # nenhuma linha avulsa

    r.processar_texto(f"{int(T0) + 30}: Client NOVUS_B disconnected.", T0)
    assert b.status == "Desconectado"


def test_device_antes_do_mosquitto_e_absorvido_pelo_cliente():
    r = RegistroClientes()
    r.dispositivo_visto("logbox01", "NOVUS/x/logbox01", T0)
    assert [c.nome for c in r.linhas] == ["logbox01"] and r.linhas[0].client_id is None
    r.processar_texto(f"{int(T0) + 1}: Received PUBLISH from cli1 (d0, q0, r0, m0, 'NOVUS/x/logbox01', "
                      "... (10 bytes))", T0)
    assert len(r.linhas) == 1
    c = r.linhas[0]
    assert (c.client_id, c.nome, c.mensagens) == ("cli1", "logbox01", 1)


def test_lista_sem_mosquitto_so_nome_topico_e_envio(novo_painel):
    relogio = Relogio()
    painel = novo_painel(relogio=relogio, executor_arp=lambda: "", locais=set())
    painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/events",
                                datetime.fromtimestamp(T0 - 8).strftime("%Y-%m-%dT%H:%M:%S"))
    painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/events",
                                datetime.fromtimestamp(T0 - 3).strftime("%Y-%m-%dT%H:%M:%S"))
    assert painel.filtro.rowCount() == 1
    linha = painel.linhas_visiveis()[0]
    assert linha[COL_NOME] == "Teste_Alumbra"
    assert linha[COL_TOPICO] == "NOVUS/Teste_Alumbra/events"
    assert linha[COL_ULTIMO] == datetime.fromtimestamp(T0 - 3).strftime("%d/%m/%Y %H:%M:%S")
    assert linha[COL_HA] == "há 3 s"
    assert (linha[COL_IP], linha[COL_MAC], linha[COL_STATUS], linha[COL_MENSAGENS]) == ("—", "—", "—", "—")
    assert painel.chip_enviando.texto() == "1 dispositivo enviando"


# ---------------------------------------------------------------------- ARP e MAC
ARP_WINDOWS = """
Interface: 192.168.100.162 --- 0x7
  Endereço IP           Endereço físico       Tipo
  192.168.100.1         00-1a-2b-3c-4d-5e     dinâmico
  192.168.100.50        aa-bb-cc-dd-ee-ff     dinâmico
  192.168.100.255       ff-ff-ff-ff-ff-ff     estático
  224.0.0.22            01-00-5e-00-00-16     estático
"""


def test_interpretar_arp_e_normalizar_mac():
    assert interpretar_arp(ARP_WINDOWS) == {"192.168.100.1": "00:1A:2B:3C:4D:5E",
                                            "192.168.100.50": "AA:BB:CC:DD:EE:FF"}
    assert interpretar_arp("? (192.168.0.9) at 0a:0b:0c:0d:0e:0f [ether] on eth0") == {
        "192.168.0.9": "0A:0B:0C:0D:0E:0F"}
    assert normalizar_mac("aa-bb-cc-dd-ee-ff") == "AA:BB:CC:DD:EE:FF"
    assert normalizar_mac("aabb.ccdd.eeff") == "AA:BB:CC:DD:EE:FF"
    assert normalizar_mac("aa-bb-cc") is None
    assert normalizar_mac("zz-bb-cc-dd-ee-ff") is None


def test_mac_preenchido_pelo_executor_e_ip_local(novo_painel):
    chamadas = []

    def executor():
        chamadas.append(1)
        return ARP_WINDOWS

    painel = novo_painel(relogio=Relogio(T0 + 20), executor_arp=executor, locais={"192.168.100.162"})
    painel.opcao_assinantes.setChecked(True)
    painel.ao_saida_mosquitto("saida", SAIDA + f"\n{int(T0)}: New client connected from 10.9.9.9:1 as remoto "
                                               "(p2, c1, k60).")
    # IP novo dispara a consulta (com uma pequena espera para juntar vários IPs)
    assert esperar(lambda: painel.linhas_visiveis() and any(l[COL_MAC] == "AA:BB:CC:DD:EE:FF"
                                                            for l in painel.linhas_visiveis()), 4)
    assert len(chamadas) == 1
    por_nome = {l[COL_NOME]: l for l in painel.linhas_visiveis()}
    assert por_nome["NOVUS_A"][COL_MAC] == "AA:BB:CC:DD:EE:FF"
    assert por_nome["mqtt-sqlserver-4f2a"][COL_MAC] == ESTE_COMPUTADOR  # 127.0.0.1
    assert por_nome["NOVUS_B"][COL_MAC] == "—"  # não está na tabela ARP
    assert por_nome["remoto"][COL_MAC] == "—"   # outra rede
    # botão Atualizar MAC consulta de novo, fora da thread da GUI
    painel.botao_mac.click()
    assert esperar(lambda: len(chamadas) == 2 and painel.botao_mac.isEnabled(), 3)


def test_falha_do_arp_nao_derruba(novo_painel):
    def executor():
        raise OSError("arp não encontrado")

    painel = novo_painel(relogio=Relogio(), executor_arp=executor, locais=set())
    painel.atualizar_mac()
    assert esperar(lambda: painel.botao_mac.isEnabled() and painel.arp.ultimo_erro, 3)
    assert "arp" in painel.arp.ultimo_erro


# ---------------------------------------------------------------------- Mostrar assinantes
def test_mostrar_assinantes(novo_painel):
    painel = novo_painel(relogio=Relogio(T0 + 20), executor_arp=lambda: "", locais=set())
    painel.ao_saida_mosquitto("saida", SAIDA + f"""
{int(T0) + 2}: New client connected from 127.0.0.1:5000 as mosq-Zx81kPq (p2, c1, k60).
{int(T0) + 2}: Received SUBSCRIBE from mosq-Zx81kPq""")
    assert not painel.opcao_assinantes.isChecked()  # desmarcada por padrão
    nomes = {l[COL_NOME] for l in painel.linhas_visiveis()}
    assert nomes == {"NOVUS_A", "NOVUS_B"}
    painel.opcao_assinantes.setChecked(True)
    nomes = {l[COL_NOME]: l[COL_STATUS] for l in painel.linhas_visiveis()}
    assert nomes == {"NOVUS_A": "Conectado", "NOVUS_B": "Conectado", "mqtt-sqlserver-4f2a": "Assinante",
                     "mosq-Zx81kPq": "Assinante"}
    painel.opcao_assinantes.setChecked(False)
    assert painel.filtro.rowCount() == 2
    # o "consola" (saída do próprio app, não do processo) é ignorado
    painel.ao_saida_mosquitto("info", f"{int(T0)}: New client connected from 1.2.3.4:1 as fantasma (p2, c1, k60).")
    assert painel.registro.cliente("fantasma") is None


# ---------------------------------------------------------------------- inatividade (relógio falso)
def test_texto_ha():
    assert [texto_ha(s) for s in (None, 0.4, 8, 59, 60, 179, 3600, 3720, 90000, 200000)] == [
        "—", "agora", "há 8 s", "há 59 s", "há 1 min", "há 2 min", "há 1 h", "há 1 h 02 min", "há 1 dia",
        "há 2 dias"]


def test_cores_e_textos_de_inatividade(novo_painel):
    relogio = Relogio(T0 + 5)
    painel = novo_painel(relogio=relogio, executor_arp=lambda: "", locais=set())
    painel.definir_paleta(estilo.CLARA)
    painel.ao_saida_mosquitto("saida", f"{int(T0)}: Received PUBLISH from dev (d0, q0, r0, m0, 't/1', ... (1 bytes))")
    modelo = painel.modelo

    def celula(col, papel=Qt.ItemDataRole.DisplayRole):
        return modelo.data(modelo.index(0, col), papel)

    assert celula(COL_HA) == "há 5 s"
    assert celula(COL_NOME, Qt.ItemDataRole.ForegroundRole) is None
    assert painel.chip_enviando.texto() == "1 dispositivo enviando"

    relogio.t = T0 + 180
    painel.tique()
    assert celula(COL_HA) == "há 3 min · atrasado"
    assert celula(COL_NOME, Qt.ItemDataRole.ForegroundRole) == QColor(estilo.CLARA.aviso)
    assert "2 minutos" in celula(COL_HA, Qt.ItemDataRole.ToolTipRole)
    assert painel.chip_enviando.texto() == "0 dispositivos enviando"

    relogio.t = T0 + 660
    painel.tique()
    assert celula(COL_HA) == "há 11 min · sem envio"
    assert celula(COL_TOPICO, Qt.ItemDataRole.ForegroundRole) == QColor(estilo.CLARA.erro)

    painel.definir_paleta(estilo.ESCURA)  # tema escuro: mesma regra, cor do tema
    assert celula(COL_NOME, Qt.ItemDataRole.ForegroundRole) == QColor(estilo.ESCURA.erro)


def test_ordenar_por_coluna(novo_painel):
    painel = novo_painel(relogio=Relogio(T0 + 20), executor_arp=lambda: "", locais=set())
    painel.ao_saida_mosquitto("saida", SAIDA)
    painel.tabela.sortByColumn(COL_IP, Qt.SortOrder.DescendingOrder)
    assert [l[COL_IP] for l in painel.linhas_visiveis()] == ["192.168.100.51", "192.168.100.50"]
    painel.tabela.sortByColumn(COL_MENSAGENS, Qt.SortOrder.DescendingOrder)
    assert [l[COL_NOME] for l in painel.linhas_visiveis()] == ["NOVUS_A", "NOVUS_B"]


def test_exportar_csv(novo_painel, tmp_path):
    painel = novo_painel(relogio=Relogio(T0 + 20), executor_arp=lambda: "", locais=set())
    painel.ao_saida_mosquitto("saida", SAIDA)
    painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/a",
                                datetime.fromtimestamp(T0 + 16).strftime("%Y-%m-%dT%H:%M:%S"))
    caminho = painel.exportar_csv(tmp_path / "clientes.csv")
    with caminho.open(encoding="utf-8-sig", newline="") as f:
        linhas = list(csv.reader(f, delimiter=";"))
    assert linhas[0] == ["Nome", "IP", "MAC", "Último envio", "Há quanto tempo", "Tópico", "Status", "Mensagens",
                         "Client id"]
    por_nome = {l[0]: l for l in linhas[1:]}
    assert set(por_nome) == {"Teste_Alumbra", "NOVUS_B"}
    assert por_nome["Teste_Alumbra"][1] == "192.168.100.50"
    assert por_nome["Teste_Alumbra"][7] == "2"
    assert por_nome["Teste_Alumbra"][8] == "NOVUS_A"


# ---------------------------------------------------------------------- na janela
@pytest.fixture
def janela(qapp, tmp_path):
    criadas = []

    def _montar(**kw):
        config = Config()
        config.mosquitto.iniciar_automaticamente = False
        bridge = FakePipelineBridge(config)
        j = JanelaPrincipal(bridge, config, settings=QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat),
                            tema="claro", cofre=CofreFalso(), executor_arp=lambda: ARP_WINDOWS,
                            enderecos_locais=set(), **kw)
        j.show()
        criadas.append(j)
        return j, bridge

    yield _montar
    destruir(criadas)


def test_janela_liga_mosquitto_e_bridge_na_aba_clientes(janela):
    j, bridge = janela()
    assert j.abas.tabText(2) == "Clientes" and j.abas.widget(2) is j.painel_clientes
    j.mosquitto.saida.emit("saida", SAIDA)
    bridge.emitir_dispositivo("Teste_Alumbra", "NOVUS/Teste_Alumbra/a")
    assert esperar(lambda: any(l[COL_NOME] == "Teste_Alumbra" for l in j.painel_clientes.linhas_visiveis()))
    assert esperar(lambda: any(l[COL_MAC] == "AA:BB:CC:DD:EE:FF" for l in j.painel_clientes.linhas_visiveis()), 4)
