"""Testes da auto-detecção de formato do parser."""
from __future__ import annotations

import base64
from datetime import datetime, timezone

import pytest

from app.core.parser import (
    MAX_TEXTO,
    analisar,
    converter_data_hora,
    converter_numero,
    dispositivo_do_topico,
)

ASSINATURA = "NOVUS/Teste_Alumbra/#"
TOPICO = "NOVUS/Teste_Alumbra/logger01/dados"
RECEBIDO = datetime(2026, 10, 30, 12, 0, 0)


def campos(res):
    return {r.Campo: (r.ValorNumerico, r.ValorTexto) for r in res.registros}


def analisar_(payload, topico=TOPICO):
    return analisar(topico, payload, RECEBIDO, ASSINATURA)


def local(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone().replace(tzinfo=None)


# ------------------------------------------------------------------------------------------- JSON


def test_json_objeto_com_dispositivo_e_timestamp_ms():
    res = analisar_(b'{"serial": "N123", "ts": 1730000000000, "temp": 23.5, "umid": "40,2", "ok": true}')
    assert res.ok and res.formato == "JSON"
    assert campos(res) == {"temp": (23.5, None), "umid": (40.2, None), "ok": (1.0, "true")}
    r = res.registros[0]
    assert r.Dispositivo == "N123"
    assert r.DataHoraLeitura == local(1730000000)
    assert r.RecebidoEm == RECEBIDO and r.Topico == TOPICO


def test_json_objeto_sem_dispositivo_usa_nivel_do_topico():
    res = analisar_(b'{"t1": 1, "msg": "alarme"}')
    assert {r.Dispositivo for r in res.registros} == {"logger01"}
    assert campos(res)["msg"] == (None, "alarme")
    assert res.registros[0].DataHoraLeitura is None


def test_json_aninhado_vira_caminho_com_pontos():
    res = analisar_(b'{"a": {"b": {"c": 7}}, "lista": [1, 2], "nulo": null}')
    assert campos(res) == {"a.b.c": (7.0, None), "lista.0": (1.0, None), "lista.1": (2.0, None),
                           "nulo": (None, None)}


def test_json_lista_de_objetos_com_tempo_iso_por_item():
    res = analisar_(b'[{"id": "A", "datahora": "2026-10-30T10:00:00", "t": 1},'
                    b' {"id": "B", "datahora": "2026-10-30 10:01:00.250", "t": 2}]')
    assert res.ok and res.formato == "JSON lista"
    assert [(r.Dispositivo, r.DataHoraLeitura, r.Campo, r.ValorNumerico) for r in res.registros] == [
        ("A", datetime(2026, 10, 30, 10, 0), "t", 1.0),
        ("B", datetime(2026, 10, 30, 10, 1, 0, 250000), "t", 2.0),
    ]


def test_json_lista_de_escalares():
    res = analisar_(b"[10, 20.5]")
    assert campos(res) == {"valor.0": (10.0, None), "valor.1": (20.5, None)}


def test_json_canais_nome_valor():
    res = analisar_(b'{"device": "NV1", "time": 1730000000, "canais": '
                    b'[{"nome": "T1", "valor": 21.5}, {"nome": "T2", "valor": "22,5"}]}')
    assert campos(res) == {"canais.T1": (21.5, None), "canais.T2": (22.5, None)}
    assert all(r.Dispositivo == "NV1" and r.DataHoraLeitura == local(1730000000) for r in res.registros)


def test_json_timestamp_iso_com_fuso_e_convertido_para_local():
    res = analisar_(b'{"timestamp": "2026-10-30T13:00:00Z", "v": 1}')
    esperado = datetime(2026, 10, 30, 13, tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
    assert res.registros[0].DataHoraLeitura == esperado


def test_json_timestamp_invalido_vira_aviso_e_campo_texto():
    res = analisar_(b'{"timestamp": "ontem", "v": 1}')
    assert res.ok and res.avisos
    assert campos(res)["timestamp"] == (None, "ontem")


@pytest.mark.parametrize("payload", [b'{"a": 1', b"[1, 2", b'{"a": }', b"{nao json}"])
def test_json_quebrado_e_erro(payload):
    res = analisar_(payload)
    assert not res.ok and res.registros == []
    assert "JSON inválido" in res.erro


# ---------------------------------------------------------------------------------------- tabular


def test_csv_ponto_e_virgula_com_cabecalho_e_virgula_decimal():
    res = analisar_("dispositivo;data;T1;T2\nNV9;30/10/2026 10:00:00;23,5;40\nNV9;30/10/2026 10:01:00;23,6;41"
                    .encode())
    assert res.ok and res.formato == "tabular"
    assert [(r.Dispositivo, r.DataHoraLeitura, r.Campo, r.ValorNumerico) for r in res.registros] == [
        ("NV9", datetime(2026, 10, 30, 10, 0), "T1", 23.5),
        ("NV9", datetime(2026, 10, 30, 10, 0), "T2", 40.0),
        ("NV9", datetime(2026, 10, 30, 10, 1), "T1", 23.6),
        ("NV9", datetime(2026, 10, 30, 10, 1), "T2", 41.0),
    ]


def test_csv_virgula_com_cabecalho():
    res = analisar_(b"temp,umid\n23.5,40\n")
    assert campos(res) == {"temp": (23.5, None), "umid": (40.0, None)}


def test_csv_virgula_sem_cabecalho():
    res = analisar_(b"1,2.5,abc")
    assert res.formato == "tabular"
    assert campos(res) == {"col1": (1.0, None), "col2": (2.5, None), "col3": (None, "abc")}


def test_csv_ponto_e_virgula_sem_cabecalho_varias_linhas():
    res = analisar_(b"1;2\n3;4")
    assert [(r.Campo, r.ValorNumerico) for r in res.registros] == [
        ("col1", 1.0), ("col2", 2.0), ("col1", 3.0), ("col2", 4.0)]


def test_tab_e_barra():
    assert campos(analisar_(b"a\tb\n1\t2")) == {"a": (1.0, None), "b": (2.0, None)}
    assert campos(analisar_(b"a|b\n1|2")) == {"a": (1.0, None), "b": (2.0, None)}


def test_pares_chave_valor():
    res = analisar_(b"T1=23,5;T2=40;serial=XYZ")
    assert campos(res) == {"T1": (23.5, None), "T2": (40.0, None)}
    assert res.registros[0].Dispositivo == "XYZ"


# ------------------------------------------------------------------------------------ texto simples


@pytest.mark.parametrize("payload,esperado", [
    (b"23.5", (23.5, None)),
    (b"23,5", (23.5, None)),
    (b"  -7 \n", (-7.0, None)),
    (b"Alarme ativo", (None, "Alarme ativo")),
    (b'"texto entre aspas"', (None, "texto entre aspas")),
    (b"ON", (1.0, "ON")),
])
def test_texto_simples_vira_campo_valor(payload, esperado):
    res = analisar_(payload)
    assert res.ok and res.formato == "texto"
    assert campos(res) == {"valor": esperado}


def test_texto_com_bom_utf8():
    res = analisar_("﻿{\"t\": 1}".encode("utf-8"))
    assert campos(res) == {"t": (1.0, None)}


# ------------------------------------------------------------------------------------------ binário


def test_binario_nao_utf8_guardado_em_base64():
    dados = bytes([0xFF, 0x00, 0x10, 0x80, 0xFE])
    res = analisar_(dados)
    assert res.ok and res.formato == "binário"
    assert len(res.registros) == 1
    r = res.registros[0]
    assert r.Campo == "payload_base64" and base64.b64decode(r.ValorTexto) == dados
    assert any("ff 00 10 80 fe" in a for a in res.avisos)


def test_binario_utf8_valido_mas_com_nul():
    res = analisar_(b"abc\x00\x01")
    assert res.formato == "binário"


def test_binario_grande_demais_e_descartado():
    res = analisar_(b"\xff" * 5000)
    assert not res.ok and res.formato == "binário" and "5000 bytes" in res.erro


@pytest.mark.parametrize("payload", [b"", b"   \n"])
def test_vazio(payload):
    res = analisar_(payload)
    assert not res.ok and res.formato == "vazio"


# ------------------------------------------------------------------------------------------ limites


def test_texto_longo_e_cortado_com_aviso():
    res = analisar_(('{"x": "' + "a" * 5000 + '"}').encode())
    assert len(res.registros[0].ValorTexto) == MAX_TEXTO and res.avisos


def test_nunca_levanta_excecao():
    for payload in [b"{}", b"[]", b"[[[]]]", b";;;", b"\n\n", "ç;ã\n1;2".encode("latin-1")]:
        analisar_(payload)


# ------------------------------------------------------------------------------------------ utilidades


@pytest.mark.parametrize("topico,assinatura,esperado", [
    ("NOVUS/Teste_Alumbra/logger01/dados", "NOVUS/Teste_Alumbra/#", "logger01"),
    ("NOVUS/Teste_Alumbra/logger01", "NOVUS/Teste_Alumbra/#", "logger01"),
    ("NOVUS/Teste_Alumbra", "NOVUS/Teste_Alumbra/#", None),
    ("NOVUS/X1/dados", "NOVUS/+/dados", "X1"),
    ("a/b", "a/b", None),
    ("a/b", None, None),
])
def test_dispositivo_do_topico(topico, assinatura, esperado):
    assert dispositivo_do_topico(topico, assinatura) == esperado


@pytest.mark.parametrize("texto,esperado", [
    ("1.234,5", 1234.5), ("23,5", 23.5), ("1e3", 1000.0), ("abc", None), ("nan", None), ("", None),
])
def test_converter_numero(texto, esperado):
    assert converter_numero(texto) == esperado


def test_converter_data_hora_epoch_s_ms_us():
    base = local(1730000000)
    assert converter_data_hora(1730000000) == base
    assert converter_data_hora(1730000000000) == base
    assert converter_data_hora("1730000000000000") == base
    assert converter_data_hora("30/10/2026 08:15") == datetime(2026, 10, 30, 8, 15)
    assert converter_data_hora("xyz") is None
    assert converter_data_hora(True) is None
