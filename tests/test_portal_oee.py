"""Aviso ao portal OEE (SS automática dos apontamentos reais): app/core/portal_oee.py."""
import json

from app.core.portal_oee import MAX_POR_CHAMADA, ROTA, NotificadorPortal


class PortalFalso:
    def __init__(self, status=200, resposta=None):
        self.status = status
        self.resposta = resposta or {"apontamentos": []}
        self.chamadas = []

    def __call__(self, url, corpo, cab, timeout):
        self.chamadas.append((url, json.loads(corpo), cab))
        if isinstance(self.status, Exception):
            raise self.status
        return self.status, json.dumps(self.resposta).encode()


def test_desligado_sem_url_ou_token_avisa_uma_vez_e_nao_chama(monkeypatch):
    monkeypatch.delenv("OEE_PORTAL_TOKEN", raising=False)
    logs, portal = [], PortalFalso()
    n = NotificadorPortal("", "", lambda nv, m: logs.append((nv, m)), enviar=portal)
    n.avisar([1, 2])
    n.avisar([3])
    assert portal.chamadas == [] and len(logs) == 1 and logs[0][0] == "INFO"


def test_envia_com_token_e_registra_a_ss_aberta():
    logs = []
    portal = PortalFalso(resposta={"apontamentos": [{"id": 7, "ss": "000123", "mensagem": None}]})
    n = NotificadorPortal("https://portal/oee/", "tok", lambda nv, m: logs.append((nv, m)), enviar=portal)
    n.avisar([7, 7])
    url, corpo, cab = portal.chamadas[0]
    assert url == "https://portal/oee" + ROTA and corpo == {"apontamentos": [7]} and cab["Authorization"] == "Bearer tok"
    assert any("SS 000123" in m for _n, m in logs) and n.pendentes == []


def test_token_pela_variavel_de_ambiente(monkeypatch):
    monkeypatch.setenv("OEE_PORTAL_TOKEN", "do-ambiente")
    portal = PortalFalso()
    NotificadorPortal("https://portal", "", lambda *_: None, enviar=portal).avisar([1])
    assert portal.chamadas[0][2]["Authorization"] == "Bearer do-ambiente"


def test_falha_guarda_pendentes_e_reenvia_no_proximo_ciclo_com_um_aviso_so():
    logs = []
    portal = PortalFalso(status=401)
    n = NotificadorPortal("https://portal", "tok", lambda nv, m: logs.append((nv, m)), enviar=portal)
    n.avisar([1])
    n.avisar([2])
    assert n.pendentes == [1, 2]
    assert [nv for nv, _m in logs] == ["AVISO"] and "token recusado" in logs[0][1]
    portal.status = 200
    n.avisar([])
    assert n.pendentes == [] and portal.chamadas[-1][1] == {"apontamentos": [1, 2]}
    assert logs[-1][0] == "INFO" and "voltou" in logs[-1][1]


def test_sem_rede_nao_levanta_excecao():
    portal = PortalFalso(status=OSError("rede fora"))
    n = NotificadorPortal("https://portal", "tok", lambda *_: None, enviar=portal)
    n.avisar([5])
    assert n.pendentes == [5]


def test_lotes_de_ate_100():
    portal = PortalFalso()
    n = NotificadorPortal("https://portal", "tok", lambda *_: None, enviar=portal)
    n.avisar(list(range(1, MAX_POR_CHAMADA + 3)))
    assert [len(c[1]["apontamentos"]) for c in portal.chamadas] == [MAX_POR_CHAMADA, 2]


def test_pecas_e_ops_vao_na_mesma_chamada():
    portal = PortalFalso()
    n = NotificadorPortal("https://portal", "tok", lambda *_: None, enviar=portal)
    n.avisar([], [11, 12, 11], [40])
    assert portal.chamadas[0][1] == {"apontamentos": [], "producao": [11, 12], "ops": [40]}
    n.avisar([5])
    assert portal.chamadas[1][1] == {"apontamentos": [5]}


def test_pecas_pendentes_voltam_no_proximo_ciclo():
    portal = PortalFalso(status=500)
    n = NotificadorPortal("https://portal", "tok", lambda *_: None, enviar=portal)
    n.avisar([], [7], [3])
    portal.status = 200
    n.avisar([])
    assert portal.chamadas[-1][1] == {"apontamentos": [], "producao": [7], "ops": [3]}
