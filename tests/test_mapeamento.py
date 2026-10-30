"""Testes do mapeamento mensagem MQTT → linhas da dbo.IOT001."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.config import MapeamentoConfig
from app.core.mapeamento import (
    LinhaIot,
    converter_chd,
    converter_pid,
    converter_ts_unix,
    mapear,
)

ASS = "NOVUS/Teste_Alumbra/#"
TOP = "NOVUS/Teste_Alumbra/N7/dados"
RECEBIDO = datetime(2026, 10, 30, 15, 0, 0, 123456, tzinfo=timezone.utc)
TS = 1730000000


def _m(payload, cfg=None, topico=TOP, assinatura=ASS):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return mapear(topico, payload, RECEBIDO, assinatura, cfg)


def _d(t):
    return Decimal(t)


def test_json_plano():
    payload = f'{{"pid": 17, "device_id": "N1", "ts": {TS}, "chd1_value": 23.45678, "chd2_value": 40, ' \
              f'"chd3_value": null}}'
    r = _m(payload)
    assert r.ok and r.formato == "JSON"
    (l,) = r.linhas
    assert l.completa
    assert l.como_tupla() == (17, "N1", TS, _d("23.4568"), _d("40.0000"), None, payload,
                              datetime(2026, 10, 30, 15, 0, 0, 123000))
    assert r.avisos == []


def test_colunas_inseridas_nao_incluem_id_nem_data_hora_utc():
    assert LinhaIot.COLUNAS == ("pid", "device_id", "ts_unix", "chd1_value", "chd2_value", "chd3_value",
                                "payload_json", "recebido_em")


def test_json_aninhado_chd_value_e_chaves_alternativas():
    r = _m(f'{{"PID": "5", "Device": "N2", "timestamp": "{TS}", '
           f'"chd1": {{"value": 1.5}}, "chd2": {{"valor": "2,25"}}, "chd3": 3}}')
    (l,) = r.linhas
    assert l.completa
    assert (l.pid, l.device_id, l.ts_unix) == (5, "N2", TS)
    assert (l.chd1_value, l.chd2_value, l.chd3_value) == (_d("1.5000"), _d("2.2500"), _d("3.0000"))


def test_chave_literal_com_ponto():
    r = _m(f'{{"pid": 1, "device_id": "N1", "ts": {TS}, "chd1.value": 9}}')
    assert r.linhas[0].chd1_value == _d("9.0000")


def test_lista_de_leituras_gera_varias_linhas():
    payload = (f'[{{"pid": 1, "device_id": "A", "ts": {TS}, "chd1": 1}}, '
               f'{{"pid": 2, "device_id": "B", "ts": {TS + 1}, "chd1": 2}}]')
    r = _m(payload)
    assert r.formato == "JSON lista"
    assert [(l.pid, l.device_id, l.ts_unix, l.chd1_value) for l in r.linhas] == [
        (1, "A", TS, _d("1.0000")), (2, "B", TS + 1, _d("2.0000"))]
    assert all(l.payload_json == payload for l in r.linhas)


def test_envelope_com_lista_de_leituras_herda_campos():
    r = _m(f'{{"device_id": "N9", "leituras": [{{"pid": 1, "ts": {TS}}}, '
           f'{{"pid": 2, "ts": {TS + 1}, "device_id": "OUTRO"}}]}}')
    assert [(l.pid, l.device_id) for l in r.linhas] == [(1, "N9"), (2, "OUTRO")]
    assert all(l.completa for l in r.linhas)


@pytest.mark.parametrize("ts", [TS, TS * 1000 + 123, TS * 1_000_000, TS * 1_000_000_000, f"{TS}000",
                                "2024-10-27T03:33:20Z", "2024-10-27T00:33:20-03:00", float(TS) + 0.9])
def test_ts_em_segundos_ms_us_ns_e_iso(ts):
    assert converter_ts_unix(ts) == TS


def test_ts_em_ms_no_payload():
    r = _m(f'{{"pid": 1, "device_id": "N1", "ts": {TS}123}}')
    assert r.linhas[0].ts_unix == TS


def test_ts_iso_sem_fuso_respeita_config():
    assert converter_ts_unix("2024-10-27 03:33:20", "utc") == TS
    local = datetime(2024, 10, 27, 3, 33, 20).astimezone()
    assert converter_ts_unix("2024-10-27 03:33:20", "local") == int(local.timestamp())


@pytest.mark.parametrize("ts", [0, 12345, True, "ontem", "", None, -TS, 99999999999999999999999])
def test_ts_invalido(ts):
    assert converter_ts_unix(ts) is None


def test_falta_pid_e_ts_vai_para_contingencia_sem_inventar():
    r = _m('{"device_id": "N1", "chd1": 1}')
    (l,) = r.linhas
    assert not l.completa
    assert l.pid is None and l.ts_unix is None          # nada de pid=0 nem ts = hora de recebimento
    assert "sem pid" in l.motivo and "sem ts_unix" in l.motivo
    assert r.rejeitadas == [l] and r.completas == []


def test_pid_invalido():
    r = _m(f'{{"pid": "abc", "device_id": "N1", "ts": {TS}}}')
    assert "pid inválido" in r.linhas[0].motivo
    assert converter_pid(1.5) is None and converter_pid(2**63) is None and converter_pid("12") == 12


def test_device_id_do_topico_como_fallback():
    r = _m(f'{{"pid": 1, "ts": {TS}}}')
    assert r.linhas[0].device_id == "N7" and r.linhas[0].completa


def test_device_id_sem_payload_nem_topico():
    r = _m(f'{{"pid": 1, "ts": {TS}}}', topico="outro/topico")
    assert "sem device_id" in r.linhas[0].motivo
    cfg = MapeamentoConfig(device_do_topico=False)
    assert "sem device_id" in _m(f'{{"pid": 1, "ts": {TS}}}', cfg).linhas[0].motivo


def test_device_id_maior_que_100():
    r = _m(f'{{"pid": 1, "device_id": "{"x" * 101}", "ts": {TS}}}')
    assert "mais de 100" in r.linhas[0].motivo


def test_valor_fora_da_faixa_do_decimal_vira_null_com_aviso():
    r = _m(f'{{"pid": 1, "device_id": "N1", "ts": {TS}, "chd1": 1e20, "chd2": 99999999999999.99999, '
           f'"chd3": "abc"}}')
    (l,) = r.linhas
    assert l.completa
    assert (l.chd1_value, l.chd2_value, l.chd3_value) == (None, None, None)
    assert len(r.avisos) == 3
    assert "fora da faixa" in r.avisos[0] and "não numérico" in r.avisos[2]


def test_converter_chd_arredonda_e_aceita_formatos():
    assert converter_chd(1.23456) == (_d("1.2346"), None)
    assert converter_chd("1.234,5") == (_d("1234.5000"), None)
    assert converter_chd("-99999999999999.9999") == (_d("-99999999999999.9999"), None)
    assert converter_chd(None) == (None, None)
    assert converter_chd(True)[0] is None
    assert converter_chd(float("nan"))[0] is None


def test_tabular_com_cabecalho():
    payload = "pid;device_id;ts;chd1;chd2;chd3\n1;N1;1730000000;23,5;40;0\n2;N1;1730000001000;1.234,5;;x"
    r = _m(payload)
    assert r.formato == "tabular"
    assert [l.como_tupla()[:6] for l in r.linhas] == [
        (1, "N1", TS, _d("23.5000"), _d("40.0000"), _d("0.0000")),
        (2, "N1", TS + 1, _d("1234.5000"), None, None)]
    assert all(l.payload_json == payload for l in r.linhas)
    assert any("não numérico" in a for a in r.avisos)


def test_tabular_chave_valor():
    r = _m(f"pid=3;device_id=N1;ts={TS};chd1=2")
    assert r.linhas[0].como_tupla()[:4] == (3, "N1", TS, _d("2.0000"))


def test_tabular_sem_cabecalho_so_com_config():
    payload = f"1;N1;{TS};5"
    r = _m(payload)
    assert not r.linhas[0].completa and "sem cabeçalho" in r.linhas[0].motivo
    cfg = MapeamentoConfig(colunas_sem_cabecalho=["pid", "device_id", "ts", "chd1"])
    l = _m(payload, cfg).linhas[0]
    assert l.completa and l.chd1_value == _d("5.0000")


def test_chaves_configuraveis():
    cfg = MapeamentoConfig(pid=["seqno"], chd1=["canais.temp"])
    r = _m(f'{{"seqno": 8, "device_id": "N1", "ts": {TS}, "canais": {{"temp": 21}}, "pid": 999}}', cfg)
    assert r.linhas[0].pid == 8 and r.linhas[0].chd1_value == _d("21.0000")


def test_json_invalido_binario_e_vazio():
    r = _m('{"pid": ')
    assert r.ok and not r.linhas[0].completa and "JSON inválido" in r.linhas[0].motivo
    r = _m(b"\xff\xfe\x00")
    assert r.formato == "binário" and r.linhas[0].payload_json == "base64://4A"
    r = _m(b"   ")
    assert not r.ok and r.linhas == []


def test_recebido_em_ingenuo_e_tratado_como_local_e_vai_para_utc():
    local = datetime(2026, 10, 30, 12, 0, 0)
    r = mapear(TOP, f'{{"pid": 1, "device_id": "N1", "ts": {TS}}}', local, ASS)
    esperado = local.astimezone(timezone.utc).replace(tzinfo=None)
    assert r.linhas[0].recebido_em == esperado
    assert abs(r.linhas[0].recebido_em - esperado) < timedelta(seconds=1)


# ------------------------------------------------------------------ payload real do NOVUS (chaves aninhadas)

NOVUS_REAL = ('{"pid":51387408,"device_id":"Teste_Alumbra","channels":{"timestamp":1793386748,'
              '"chd1_value":67,"chd2_value":119,"chd3_value":0}}')
TOP_NOVUS = "NOVUS/Teste_Alumbra/events"


@pytest.fixture(autouse=True)
def _avisos_limpos():
    from app.core.mapeamento import _esquecer_avisos
    _esquecer_avisos()
    yield
    _esquecer_avisos()


def _confere_novus(l, payload):
    assert l.motivo is None and l.completa
    assert (l.pid, l.device_id, l.ts_unix) == (51387408, "Teste_Alumbra", 1793386748)
    assert (l.chd1_value, l.chd2_value, l.chd3_value) == (_d("67.0000"), _d("119.0000"), _d("0.0000"))
    assert l.payload_json == payload
    assert l.recebido_em == datetime(2026, 10, 30, 15, 0, 0, 123000)


def test_payload_real_novus_com_channels():
    r = _m(NOVUS_REAL, topico=TOP_NOVUS)
    assert r.formato == "JSON" and r.avisos == []
    (l,) = r.linhas
    _confere_novus(l, NOVUS_REAL)
    assert datetime.fromtimestamp(l.ts_unix, timezone.utc) == datetime(2026, 10, 30, 18, 59, 8, tzinfo=timezone.utc)


def test_payload_real_novus_em_lista():
    payload = f"[{NOVUS_REAL}, {NOVUS_REAL}]"
    r = _m(payload, topico=TOP_NOVUS)
    assert r.formato == "JSON lista" and r.avisos == []
    assert len(r.linhas) == 2
    for l in r.linhas:
        _confere_novus(l, payload)


def test_payload_novus_em_envelope_de_leituras():
    payload = ('{"device_id":"Teste_Alumbra","leituras":[{"pid":1,"channels":{"timestamp":1793386748,"chd1_value":1}},'
               '{"pid":2,"channels":{"timestamp":1793386749,"chd1_value":2}}]}')
    r = _m(payload, topico=TOP_NOVUS)
    assert [(l.pid, l.device_id, l.ts_unix, l.chd1_value, l.motivo) for l in r.linhas] == [
        (1, "Teste_Alumbra", 1793386748, _d("1.0000"), None), (2, "Teste_Alumbra", 1793386749, _d("2.0000"), None)]


def test_nivel_de_cima_vence_o_aninhado():
    r = _m('{"pid":1,"device_id":"N1","timestamp":1730000000,"chd1_value":5,'
           '"channels":{"timestamp":1793386748,"chd1_value":67,"chd2_value":119,"pid":999}}')
    (l,) = r.linhas
    assert l.completa
    assert (l.pid, l.ts_unix, l.chd1_value, l.chd2_value) == (1, TS, _d("5.0000"), _d("119.0000"))
    assert r.avisos == []


def test_aninhado_em_qualquer_profundidade_e_formas_antigas():
    r = _m('{"pid":1,"device_id":"N1","a":{"b":{"c":{"ts":1730000000}}},'
           '"canais":{"chd1":{"value":1.5},"chd2.value":2,"chd3":3}}')
    (l,) = r.linhas
    assert l.completa and l.ts_unix == TS
    assert (l.chd1_value, l.chd2_value, l.chd3_value) == (_d("1.5000"), _d("2.0000"), _d("3.0000"))
    assert r.avisos == []


def test_duas_chaves_aninhadas_usa_a_primeira_do_json_e_avisa_uma_vez():
    payload = '{"pid":1,"device_id":"N1","meta":{"timestamp":1730000000},"channels":{"ts":1793386748}}'
    r = _m(payload)
    assert r.linhas[0].ts_unix == TS                      # primeira no JSON, mesmo "ts" vindo antes na config
    assert len(r.avisos) == 1
    assert "ts_unix" in r.avisos[0] and "meta.timestamp" in r.avisos[0] and "channels.ts" in r.avisos[0]
    r2 = _m(payload)
    assert r2.linhas[0].ts_unix == TS and r2.avisos == []  # não repete


def test_aninhado_nao_entra_em_listas_nem_inventa():
    r = _m('{"pid":1,"device_id":"N1","historico":[{"timestamp":1730000000}]}')
    assert "sem ts_unix" in r.linhas[0].motivo
