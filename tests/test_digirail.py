"""NOVUS DigiRail IoT: classificação das mensagens, amostragem pelo minuto da leitura, duplicatas, relógio adiantado
e comandos (zerar contadores, diagnóstico).

Cliente MQTT falso (``ClienteFalso`` de test_amostragem: grava as publicações) e relógio injetado. Os payloads são
os exemplos de docs/digirail_iot_mqtt.md (seções 3 e 5). No fim, integração com o broker amqtt local e um
"dispositivo falso" que assina ``command`` e responde em ``ack/command``.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from app.config import Config, MapeamentoConfig, SqlServerConfig
from app.core import digirail
from app.core.erros import ler_erros_do_dia
from app.core.mapeamento import EVENTO, LEITURA, RESPOSTA, classificar, mapear
from app.core.mqtt_client import ClienteMqtt
from app.core.parser import dispositivo_do_topico, dispositivo_novus
from app.core.pipeline import Pipeline, _MemoriaGravadas, duracao_texto, proximo_ciclo
from app.core.sink_sqlserver import SinkSqlServer
from tests.test_amostragem import ClienteFalso, Relogio, esperar, novus
from tests.test_sink import ConexaoFalsa

DEV = "Teste_Alumbra"
T_DADOS = f"NOVUS/{DEV}/events"
T_ACK_CMD = f"NOVUS/{DEV}/ack/command"
T_ACK_CFG = f"NOVUS/{DEV}/ack/config"
T_CMD = f"NOVUS/{DEV}/command"
ASSINATURA = f"NOVUS/{DEV}/#"

EVENTO_MANUAL = {"pid": 51584019, "device_id": DEV, "events": {"chd1": {"timestamp": 1585819219.685, "edge": 1}}}
DIAG = {"error": 0, "title": "Pci v2", "location": "home", "curr_timestamp": 1589326517,
        "cfg_timestamp": 1589311676, "fw_v": "1.23", "mqtt_queue": 1, "sn": "00000001", "curr_rssi": "55",
        "min_rssi": "-46", "max_rssi": "-87", "avg_rssi": "-54", "ipv4": [192, 168, 0, 23],
        "gateway": [192, 168, 0, 1], "mask": [255, 255, 255, 0], "dns1": [192, 168, 0, 1], "dns2": [8, 8, 8, 8]}
NTP_ACK = {"pid": 51584019, "device_id": DEV, "timestamp": 1585819219,
           "reported": {"ntp": {"error": 0, "enable": 1, "diff_to_update": 5, "host": "time.google.com"}}}


def resposta_diag(ts: int, device: str = DEV) -> dict:
    return {"pid": 51584019, "device_id": device, "timestamp": ts, "reported": {"diag": DIAG}}


def resposta_reset(ts: int, erro: int = 0, device: str = DEV) -> dict:
    return {"pid": 51584019, "device_id": device, "timestamp": ts,
            "reported": {"reset_counters": {"error": erro, **{f"reset_chd{i}": 0 for i in range(1, 7)}}}}


# =========================================================================================== funções puras


@pytest.mark.parametrize("topico,obj,esperado", [
    (T_DADOS, {"pid": 1, "device_id": DEV, "channels": {"timestamp": 1, "chd1_value": 1}}, LEITURA),
    (T_DADOS, EVENTO_MANUAL, EVENTO),
    (T_ACK_CMD, resposta_diag(1), RESPOSTA),
    (T_ACK_CFG, NTP_ACK, RESPOSTA),
    (T_ACK_CMD, {"qualquer": "coisa"}, RESPOSTA),           # tudo em ack/* é resposta
    (T_CMD, {"timestamp": 1, "desired": {"diag": {}}}, RESPOSTA),
    (T_DADOS, {"device_id": DEV, "reported": {"logs": {}}}, RESPOSTA),
    (T_DADOS, {"pid": 1, "ts": 1730000000, "chd1": 2}, LEITURA),   # formato genérico segue o fluxo atual
    (T_DADOS, [1, 2], LEITURA),
])
def test_classificar(topico, obj, esperado):
    assert classificar(topico, obj) == esperado


def test_mapear_evento_e_resposta_nao_geram_linhas():
    r = mapear(T_DADOS, json.dumps(EVENTO_MANUAL))
    assert (r.tipo, r.linhas, r.ok, r.device_id) == (EVENTO, [], True, DEV)
    r = mapear(T_ACK_CMD, json.dumps(resposta_diag(5)))
    assert (r.tipo, r.linhas, r.device_id) == (RESPOSTA, [], DEV)
    assert r.objeto["reported"]["diag"]["sn"] == "00000001"
    # resposta com JSON inválido (o manual tem vírgula sobrando), vazia ou binária: resposta, não erro
    for payload in (b'{"device_id":"x","reported":{"logs":{"error":0,}}}', b"", b"\xff\xfe\x00"):
        r = mapear(T_ACK_CMD, payload, assinatura=ASSINATURA)
        assert r.tipo == RESPOSTA and r.ok and r.linhas == [] and r.device_id == DEV
    # pedido "desired" de terceiros, sem pid: não é mais "sem pid" nem device "command"
    r = mapear(T_CMD, b'{"timestamp":1,"desired":{"output":{}}}', assinatura=ASSINATURA)
    assert r.tipo == RESPOSTA and r.device_id == DEV


def test_mapear_payload_desconhecido_continua_erro():
    r = mapear(T_DADOS, b'{"T1": 23.5}', assinatura=ASSINATURA)
    assert r.tipo == LEITURA and r.rejeitadas and "sem pid" in r.rejeitadas[0].motivo
    r = mapear(T_DADOS, b'{"quebrado": ', assinatura=ASSINATURA)
    assert r.tipo == LEITURA and "JSON inválido" in r.rejeitadas[0].motivo


@pytest.mark.parametrize("topico,assinatura,esperado", [
    ("NOVUS/Teste_Alumbra/events", "NOVUS/Teste_Alumbra/#", "Teste_Alumbra"),
    ("NOVUS/Teste_Alumbra/ack/command", "NOVUS/Teste_Alumbra/#", "Teste_Alumbra"),
    ("NOVUS/Teste_Alumbra/ack/config", "NOVUS/#", "Teste_Alumbra"),
    ("NOVUS/Teste_Alumbra/command", "NOVUS/Teste_Alumbra/#", "Teste_Alumbra"),
    ("NOVUS/X9/events", "NOVUS/+/events", "X9"),
    ("NOVUS/Teste_Alumbra/logger01/dados", "NOVUS/Teste_Alumbra/#", "logger01"),   # fora do padrão: como antes
    ("NOVUS/Teste_Alumbra/ack", "NOVUS/Teste_Alumbra/#", "ack"),
])
def test_dispositivo_do_topico_padrao_novus(topico, assinatura, esperado):
    assert dispositivo_do_topico(topico, assinatura) == esperado


def test_dispositivo_novus_sem_padrao():
    assert dispositivo_novus("NOVUS/events") == "NOVUS"
    assert dispositivo_novus("a/b/c") is None
    assert dispositivo_novus("NOVUS/X/events/extra") is None


def test_pid_nao_sugere_numero_de_pacote():
    cfg = MapeamentoConfig()
    assert cfg.pid[0] == "pid"
    assert not {"packet_id", "id_pacote", "seq", "sequencia"} & set(cfg.pid)
    r = mapear(T_DADOS, b'{"pid_id":51387408,"device_id":"D","channels":{"timestamp":1793386748}}')
    assert r.linhas[0].pid == 51387408 and r.linhas[0].completa


def test_payload_e_mensagens_dos_comandos():
    assert digirail.payload_comando(1793386748, "reset_counters", digirail.desejado_reset([1, 3])) == \
        '{"timestamp":1793386748,"desired":{"reset_counters":{"reset_chd1":1,"reset_chd3":1}}}'
    assert digirail.payload_comando(5, "diag", {}) == '{"timestamp":5,"desired":{"diag":{}}}'
    assert digirail.validar_canais_reset([3, 1, 3]) == [1, 3]
    for ruim in ([], [0], [7], ["x"]):
        with pytest.raises(ValueError):
            digirail.validar_canais_reset(ruim)
    for ruim in ("", "a/b", "+", "#"):
        with pytest.raises(ValueError):
            digirail.validar_device_id(ruim)
    assert digirail.mensagem_reset(0, [2]) == (True, "Contadores zerados: chd2.")
    ok, msg = digirail.mensagem_reset(2)
    assert not ok and msg == ("Dispositivo recusou (erro 2). Confira se o canal tem 'reset por MQTT' habilitado "
                              "no NXperience.")


def test_canais_presentes_ordem_natural():
    channels = {"timestamp": 1, "chd10_value": 0, "chd2_value": 0, "chd1_value": 0, "ch1_user_range": 2.1,
                "chr3_value": 0, "outro": 1}
    assert digirail.canais_presentes(channels) == ["ch1", "chd1", "chd2", "chd10", "chr3"]


def test_texto_evento():
    hora = datetime.fromtimestamp(1585819219.685).strftime("%H:%M:%S")
    assert digirail.texto_evento(EVENTO_MANUAL["events"], DEV) == \
        f"Evento chd1 (borda de subida) em {DEV} às {hora}"
    assert digirail.texto_evento({"chd2": {"timestamp": 1585819219685, "edge": 0}}, DEV) == \
        f"Evento chd2 (borda de descida) em {DEV} às {hora}"


def test_memoria_gravadas_expira_e_limita():
    m = _MemoriaGravadas(validade=100, limite=3)
    m.adicionar(("ts", "D", 1), 0)
    assert m.contem(("ts", "D", 1), 50)
    assert not m.contem(("ts", "D", 1), 101)          # expirou
    for i in range(5):
        m.adicionar(("ts", "D", i), 200)
    assert len(m) == 3 and not m.contem(("ts", "D", 0), 200) and m.contem(("ts", "D", 4), 200)


# =========================================================================================== pipeline


class Coletor:
    def __init__(self) -> None:
        self.logs: list[tuple[str, str]] = []
        self.erros: list[str] = []
        self.diags: list[tuple[str, dict]] = []
        self.respostas: list[tuple[str, str, bool, str]] = []
        self.canais: list[tuple[str, list]] = []
        self.vistos: list[tuple[str, str, str]] = []

    def kwargs(self) -> dict:
        return dict(ao_log=lambda n, m: self.logs.append((n, m)), ao_erro=self.erros.append,
                    ao_diagnostico=lambda d, j: self.diags.append((d, json.loads(j))),
                    ao_resposta_comando=lambda *a: self.respostas.append(a),
                    ao_canais=lambda d, j: self.canais.append((d, json.loads(j))),
                    ao_dispositivo=lambda *a: self.vistos.append(a))

    def com(self, trecho: str, nivel: str | None = None) -> list[str]:
        return [m for n, m in self.logs if trecho in m and (nivel is None or n == nivel)]


@pytest.fixture
def inicio() -> float:
    """Meio segundo depois de uma virada de minuto local (como 14:31:00.5)."""
    return proximo_ciclo(time.time(), 60) + 0.5


@pytest.fixture
def montar(tmp_path, inicio):
    criados: list[Pipeline] = []

    def _montar(intervalo: int = 60, assinatura: str = ASSINATURA, diag_min: float = 0, sql: bool = True):
        cfg = Config()
        cfg.sqlserver = SqlServerConfig(servidor="FALSO", banco="MQTT") if sql else SqlServerConfig(servidor="",
                                                                                                    banco="")
        cfg.gravacao.lote_segundos = 0.05
        cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
        cfg.gravacao.pasta_logs = str(tmp_path / "logs")
        cfg.gravacao.intervalo_gravacao_segundos = intervalo
        cfg.dispositivos.diagnostico_minutos = diag_min
        conexao = ConexaoFalsa()
        relogio = Relogio(inicio)
        col = Coletor()

        def fabrica(sql_cfg, config, logar):
            return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=lambda _c: conexao, dormir=lambda s: None)

        kw = dict(fabrica_sink=fabrica) if sql else {}
        p = Pipeline(cfg, relogio=relogio, fabrica_cliente=ClienteFalso, sessao_persistente=False, pasta=tmp_path,
                     **kw, **col.kwargs())
        p.LOGS_POR_SEGUNDO = 10_000      # os testes contam as linhas "Mensagem de ..."
        p.conectar("127.0.0.1", 1883, assinatura, 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, col, conexao

    yield _montar
    for p in criados:
        p.encerrar(5)


def _linhas(conexao: ConexaoFalsa) -> list[tuple]:
    return [linha for _sql, linhas in conexao.gravado for linha in linhas]


def _erros_arquivo(tmp_path) -> list[dict]:
    return [r for d in sorted({date.today(), date(2026, 11, 4)}) for r in ler_erros_do_dia(tmp_path / "logs", d)]


def _bate(p: Pipeline, col: Coletor, esperado: int) -> None:
    """Regra de ouro: contador de erros == registros pelo callback."""
    assert esperar(lambda: p.contadores[2] == esperado and len(col.erros) == esperado), (p.contadores, col.erros)


# ------------------------------------------------------------------------------------- classificação


def test_evento_nao_grava_nem_e_erro(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    relogio.ir(inicio + 1)
    cli.entregar(T_DADOS, EVENTO_MANUAL)
    hora = datetime.fromtimestamp(1585819219.685).strftime("%H:%M:%S")
    assert esperar(lambda: col.com("Evento chd1")), col.logs
    assert col.com("Evento chd1", "INFO") == [f"Evento chd1 (borda de subida) em {DEV} às {hora}"]
    relogio.ir(inicio + 59.5)
    assert esperar(lambda: col.com("Gravação "))
    assert col.com("Gravação ") == [f"Gravação {datetime.fromtimestamp(inicio + 59.5):%H:%M}: nenhuma leitura "
                                     "completa recebida no período"]
    assert conexao.gravado == []
    assert p.contadores == (1, 0, 0) and col.erros == []
    assert [v[0] for v in col.vistos] == [DEV]       # o dispositivo está vivo


def test_evento_nao_substitui_a_leitura_do_minuto(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    relogio.ir(inicio + 1)
    cli.entregar(T_DADOS, novus(51387408, ts=inicio, chd1=67))
    cli.entregar(T_DADOS, {**EVENTO_MANUAL, "events": {"chd1": {"timestamp": inicio + 0.5, "edge": 0}}})
    relogio.ir(inicio + 59.5)
    assert esperar(lambda: _linhas(conexao))
    (linha,) = _linhas(conexao)
    assert linha[3] == pytest.approx(67)


def test_ack_command_com_diag_emite_sinal_sem_leitura_nem_erro(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    relogio.ir(inicio + 1)
    cli.entregar(T_ACK_CMD, resposta_diag(1589326517))       # diagnóstico periódico, ninguém pediu
    assert esperar(lambda: col.diags)
    assert col.diags == [(DEV, DIAG)]
    assert col.respostas == []                               # não foi pedido por nós
    assert col.com("Diagnóstico de Teste_Alumbra (enviado pelo próprio dispositivo): nº série 00000001, "
                   "firmware 1.23, sinal médio -54 dBm, 1 leitura(s) na fila do aparelho, IP 192.168.0.23")
    p.desconectar().result(5)
    assert conexao.gravado == [] and p.contadores == (1, 0, 0) and col.erros == []
    assert col.vistos == []                                  # tópico ack não muda o tópico da aba Clientes


@pytest.mark.parametrize("topico,payload", [
    (T_ACK_CFG, NTP_ACK),
    (T_ACK_CFG, {"device_id": DEV, "timestamp": 15, "reported": {"rtc": {"error": 0}}}),   # sem pid, ts 15
    (T_ACK_CFG, {"timestamp": 1, "desired": {"ntp": {}}}),
    (T_CMD, {"timestamp": 1, "desired": {"output": {"out1": 1}}}),
    (T_ACK_CMD, {"pid_id": 51387408, "device_id": DEV, "timestamp": 1, "reported": {"logs": {"error": 0}}}),
    (T_ACK_CMD, b'{"device_id":"x","reported":{"gateway_485":{"error":0; "mb_buffer":""}}}'),
])
def test_ack_config_e_pedidos_nao_sao_erro(montar, inicio, topico, payload):
    p, cli, relogio, col, conexao = montar(intervalo=0)
    relogio.ir(inicio + 1)
    cli.entregar(topico, payload)
    assert esperar(lambda: p.contadores[0] == 1)
    p.desconectar().result(5)
    assert conexao.gravado == [] and p.contadores == (1, 0, 0) and col.erros == []
    assert not col.com("", "ERRO")


# ------------------------------------------------------------------------------------- amostragem por ts_unix


def test_reenvio_da_memoria_60_leituras_de_10_minutos_gravam_10_linhas(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    base = int(inicio - 0.5) - 3600                   # uma hora atrás, início de minuto
    relogio.ir(inicio + 2)
    for i in range(60):                               # a cada 10 s, 10 minutos, chegando de uma vez
        cli.entregar(T_DADOS, novus(51387408, ts=base + 10 * i, chd1=i))
    for i in range(6):                                # e o minuto que está terminando, ao vivo
        cli.entregar(T_DADOS, novus(51387408, ts=int(inicio) + 10 * i, chd1=100 + i))
    assert esperar(lambda: len(col.com("Mensagem de")) == 66)
    assert _linhas(conexao) == []                     # nada antes da virada
    virada = inicio - 0.5 + 60
    relogio.ir(virada)
    assert esperar(lambda: col.com("Gravação ")), col.logs
    linhas = _linhas(conexao)
    assert len(linhas) == 11
    assert [l[2] for l in linhas] == [base + 60 * m + 50 for m in range(10)] + [int(inicio) + 50]  # a mais recente
    assert [float(l[3]) for l in linhas] == [6 * m + 5 for m in range(10)] + [105]
    rotulo = datetime.fromtimestamp(virada).strftime("%H:%M")
    assert col.com("Gravação ") == [
        f"Gravação {rotulo}: 1 leitura gravada no SQL Server (6 recebidas, 5 descartadas pela amostragem) + 10 "
        "recuperadas da memória do dispositivo (60 recebidas, 50 descartadas)"]
    assert esperar(lambda: p.contadores == (66, 11, 0))


def test_so_recuperadas(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    base = int(inicio - 0.5) - 600
    relogio.ir(inicio + 2)
    for i in range(12):
        cli.entregar(T_DADOS, novus(51387408, ts=base + 10 * i))
    assert esperar(lambda: len(col.com("Mensagem de")) == 12)
    relogio.ir(inicio + 59.5)
    assert esperar(lambda: col.com("Gravação "))
    assert col.com("Gravação ")[0].endswith(": 2 leituras gravadas no SQL Server, recuperadas da memória do "
                                            "dispositivo (12 recebidas, 10 descartadas pela amostragem)")


def test_minuto_corrente_espera(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    relogio.ir(inicio + 62)                           # já passou da virada; o ciclo roda
    assert esperar(lambda: col.com("Gravação "))
    cli.entregar(T_DADOS, novus(1, ts=inicio + 61))   # minuto corrente
    assert esperar(lambda: col.com("Mensagem de"))
    time.sleep(0.3)
    assert _linhas(conexao) == []
    relogio.ir(inicio + 119.5)
    assert esperar(lambda: _linhas(conexao))
    assert [l[0] for l in _linhas(conexao)] == [1]


# ------------------------------------------------------------------------------------- duplicatas


def test_duplicatas_retida_qos1_e_minuto_ja_gravado_nao_regravam(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    relogio.ir(inicio + 1)
    leitura = novus(51387408, ts=inicio + 1)
    cli.entregar(T_DADOS, leitura)
    cli.entregar(T_DADOS, leitura)                    # reenvio QoS 1 antes do ciclo
    assert esperar(lambda: col.com("1 duplicada ignorada"))
    relogio.ir(inicio + 59.5)
    assert esperar(lambda: _linhas(conexao))
    assert col.com("Gravação ")[0].endswith("(1 recebida, 0 descartadas pela amostragem); 1 duplicada ignorada")
    # depois do ciclo: a mesma leitura retida (reconexão) e outra leitura do mesmo minuto já gravado
    relogio.ir(inicio + 61)
    cli.entregar(T_DADOS, leitura, retida=True)
    cli.entregar(T_DADOS, novus(51387408, ts=inicio + 30))
    assert esperar(lambda: len([m for m in col.com("Mensagem de") if "duplicada ignorada" in m]) == 3)
    assert col.com("(retida): JSON, 1 duplicada ignorada")
    relogio.ir(inicio + 119.5)
    assert esperar(lambda: len(col.com("Gravação ")) == 2)
    assert col.com("Gravação ")[1].endswith("nada novo para gravar (2 duplicadas ignoradas)")
    assert len(_linhas(conexao)) == 1
    assert p.contadores == (4, 1, 0) and col.erros == []


def test_duplicatas_sem_amostragem(montar, inicio):
    p, cli, relogio, col, conexao = montar(intervalo=0)
    relogio.ir(inicio + 1)
    a, b = novus(1, ts=inicio + 1), novus(1, ts=inicio + 11)
    for payload in (a, b, a, b):
        cli.entregar(T_DADOS, payload)
    cli.entregar(T_DADOS, json.dumps([a, novus(1, ts=inicio + 21)]).encode())
    assert esperar(lambda: p.contadores == (5, 3, 0)), p.contadores
    assert [l[2] for l in _linhas(conexao)] == [int(inicio) + 1, int(inicio) + 11, int(inicio) + 21]
    assert len(col.com("duplicada ignorada")) == 3


def test_duplicata_entre_conexoes(montar, inicio):
    """Mensagem retida reentregue ao reconectar não regrava (a memória vale entre conexões)."""
    p, cli, relogio, col, conexao = montar(intervalo=0)
    relogio.ir(inicio + 1)
    cli.entregar(T_DADOS, novus(1, ts=inicio + 1))
    assert esperar(lambda: p.contadores[1] == 1)
    p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
    ClienteFalso.ultimo.entregar(T_DADOS, novus(1, ts=inicio + 1), retida=True)
    assert esperar(lambda: p.contadores[0] == 2)
    time.sleep(0.2)
    assert p.contadores == (2, 1, 0)


# ------------------------------------------------------------------------------------- relógio adiantado


def test_ts_no_futuro_grava_e_avisa_uma_vez_por_dispositivo(montar, inicio):
    p, cli, relogio, col, conexao = montar()
    relogio.ir(inicio + 1)
    futuro = int(inicio) + 3600
    cli.entregar(T_DADOS, novus(1, ts=futuro))
    cli.entregar(T_DADOS, novus(2, ts=futuro + 60))
    cli.entregar(T_DADOS, novus(3, device="B", ts=futuro))
    cli.entregar(T_DADOS, novus(4, device="C", ts=inicio + 200))   # menos de 5 min: sem aviso
    assert esperar(lambda: len(col.com("Mensagem de")) == 4)
    avisos = col.com("adiantado", "AVISO")
    assert len(avisos) == 2
    assert avisos[0].startswith(f"Relógio do dispositivo {DEV} adiantado {duracao_texto(futuro - inicio - 1)}: "
                                f"leitura com horário {datetime.fromtimestamp(futuro):%Y-%m-%d %H:%M:%S} chegou às")
    assert "B adiantado" in avisos[1]
    relogio.ir(inicio + 59.5)
    assert esperar(lambda: col.com("Gravação "))
    assert sorted(l[0] for l in _linhas(conexao)) == [1, 2, 3]      # gravadas no próximo ciclo; C espera


# ------------------------------------------------------------------------------------- comandos


def test_zerar_contadores_publica_o_json_exato_no_topico_certo(montar, inicio):
    p, cli, relogio, col, _ = montar()
    relogio.ir(inicio + 3)
    p.zerar_contadores(DEV, [3, 1]).result(5)
    assert cli.publicados == [(T_CMD, json.dumps(
        {"timestamp": int(inicio + 3), "desired": {"reset_counters": {"reset_chd1": 1, "reset_chd3": 1}}},
        separators=(",", ":")), 1)]
    assert col.com("Comando enviado a Teste_Alumbra: zerar contadores (chd1, chd3)", "INFO")
    assert cli.assinaturas_extras == []             # NOVUS/Teste_Alumbra/# já cobre o ack/command


def test_resposta_error_0_da_sucesso(montar, inicio):
    p, cli, relogio, col, _ = montar()
    relogio.ir(inicio + 3)
    p.zerar_contadores(DEV, [2]).result(5)
    ts = json.loads(cli.publicados[0][1])["timestamp"]
    cli.entregar(T_ACK_CMD, resposta_reset(ts + 1))   # outro timestamp: não casa
    cli.entregar(T_ACK_CMD, resposta_reset(ts))
    assert esperar(lambda: col.respostas)
    assert col.respostas == [(DEV, "reset_counters", True, "Contadores zerados: chd2.")]
    assert col.com("sem pedido pendente")
    relogio.ir(inicio + 30)
    time.sleep(0.3)
    assert len(col.respostas) == 1 and p.contadores[2] == 0 and col.erros == []


def test_resposta_com_erro_da_falha_e_erro_comando(montar, inicio, tmp_path):
    p, cli, relogio, col, _ = montar()
    relogio.ir(inicio + 3)
    p.zerar_contadores(DEV, [4]).result(5)
    ts = json.loads(cli.publicados[0][1])["timestamp"]
    cli.entregar(T_ACK_CMD, resposta_reset(ts, erro=2))
    assert esperar(lambda: col.respostas)
    msg = "Dispositivo recusou (erro 2). Confira se o canal tem 'reset por MQTT' habilitado no NXperience."
    assert col.respostas == [(DEV, "reset_counters", False, msg)]
    _bate(p, col, 1)
    reg = json.loads(col.erros[0])
    assert reg["categoria"] == "comando" and reg["topico"] == T_CMD and DEV in reg["mensagem"]
    assert msg in reg["detalhe"] and "chd4" in reg["detalhe"]
    assert len(_erros_arquivo(tmp_path)) == 1
    assert col.com("Zerar contadores em Teste_Alumbra falhou", "ERRO")


def test_timeout_do_comando(montar, inicio):
    p, cli, relogio, col, _ = montar()
    relogio.ir(inicio + 3)
    p.zerar_contadores(DEV, [1, 2, 3, 4, 5, 6]).result(5)
    relogio.ir(inicio + 3 + 14.9)
    time.sleep(0.3)
    assert col.respostas == []
    relogio.ir(inicio + 3 + 15)
    assert esperar(lambda: col.respostas)
    (resp,) = col.respostas
    assert resp[:3] == (DEV, "reset_counters", False) and "sem resposta do dispositivo" in resp[3].lower()
    _bate(p, col, 1)
    assert json.loads(col.erros[0])["categoria"] == "comando"
    # resposta atrasada depois do timeout não muda nada
    ts = json.loads(cli.publicados[0][1])["timestamp"]
    cli.entregar(T_ACK_CMD, resposta_reset(ts))
    time.sleep(0.3)
    assert len(col.respostas) == 1


def test_comando_invalido_ou_desconectado_nao_publica(montar, inicio):
    p, cli, relogio, col, _ = montar()
    p.zerar_contadores(DEV, [7]).result(5)
    p.zerar_contadores("a/b", [1]).result(5)
    cli.falhar_publicacao = "não conectado ao broker"
    p.zerar_contadores(DEV, [1]).result(5)
    assert cli.publicados == []
    assert [r[2] for r in col.respostas] == [False, False, False]
    assert "fora da faixa" in col.respostas[0][3] and "não conectado" in col.respostas[2][3]
    assert p.contadores[2] == 0                      # erro de uso ou sem conexão não conta como erro do dispositivo
    p.desconectar().result(5)
    p.pedir_diagnostico(DEV).result(5)
    assert col.respostas[-1] == (DEV, "diag", False, "Não conectado ao broker.")


def test_desconectar_cancela_pendentes_sem_erro(montar, inicio):
    p, cli, relogio, col, _ = montar()
    p.zerar_contadores(DEV, [1]).result(5)
    p.desconectar().result(5)
    assert col.respostas == [(DEV, "reset_counters", False, "Conexão encerrada antes da resposta do dispositivo.")]
    assert p.contadores[2] == 0


def test_diagnostico_automatico_na_primeira_vez_e_periodico(montar, inicio):
    p, cli, relogio, col, _ = montar(diag_min=30)
    relogio.ir(inicio + 1)
    cli.entregar(T_DADOS, novus(1, ts=inicio + 1))
    cli.entregar(T_DADOS, novus(1, ts=inicio + 11))
    cli.entregar(T_DADOS, EVENTO_MANUAL)
    assert esperar(lambda: len(col.com("Mensagem de")) == 2)
    assert cli.publicados == [(T_CMD, f'{{"timestamp":{int(inicio + 1)},"desired":{{"diag":{{}}}}}}', 1)]
    assert col.com("Pedido de diagnóstico enviado a Teste_Alumbra (automático)")
    cli.entregar(T_ACK_CMD, resposta_diag(int(inicio + 1)))
    assert esperar(lambda: col.diags)
    assert col.diags == [(DEV, DIAG)] and col.respostas == []   # automático: sem resposta_comando
    # 30 min depois: pede de novo; desta vez sem resposta → só AVISO, sem erro
    relogio.ir(inicio + 1 + 30 * 60)
    assert esperar(lambda: len(cli.publicados) == 2)
    assert json.loads(cli.publicados[1][1]) == {"timestamp": int(inicio + 1 + 30 * 60), "desired": {"diag": {}}}
    relogio.ir(inicio + 1 + 30 * 60 + 15)
    assert esperar(lambda: col.com("Diagnóstico de Teste_Alumbra: Sem resposta", "AVISO"))
    assert p.contadores[2] == 0 and col.erros == [] and col.respostas == []
    # nova conexão: pede de novo ao ver o dispositivo
    p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
    cli2 = ClienteFalso.ultimo
    cli2.entregar(T_DADOS, novus(1, ts=inicio + 21))
    assert esperar(lambda: len(cli2.publicados) == 1)


def test_diagnostico_desligado_com_zero(montar, inicio):
    p, cli, relogio, col, _ = montar(diag_min=0)
    cli.entregar(T_DADOS, novus(1, ts=inicio))
    assert esperar(lambda: col.com("Mensagem de"))
    relogio.ir(inicio + 7200)
    time.sleep(0.3)
    assert cli.publicados == []


def test_pedir_diagnostico_manual(montar, inicio):
    p, cli, relogio, col, _ = montar()
    relogio.ir(inicio + 5)
    p.pedir_diagnostico(DEV).result(5)
    ts = json.loads(cli.publicados[0][1])["timestamp"]
    cli.entregar(T_ACK_CMD, resposta_diag(ts))
    assert esperar(lambda: col.respostas)
    assert col.respostas == [(DEV, "diag", True, "Diagnóstico recebido.")] and col.diags == [(DEV, DIAG)]
    # manual sem resposta: falha na resposta_comando, mas não é erro
    p.pedir_diagnostico(DEV).result(5)
    relogio.ir(inicio + 30)
    assert esperar(lambda: len(col.respostas) == 2)
    assert col.respostas[1][:3] == (DEV, "diag", False) and p.contadores[2] == 0


def test_dois_comandos_no_mesmo_segundo_tem_timestamps_distintos(montar, inicio):
    p, cli, relogio, col, _ = montar()
    p.zerar_contadores(DEV, [1]).result(5)
    p.zerar_contadores(DEV, [2]).result(5)
    t1, t2 = (json.loads(x[1])["timestamp"] for x in cli.publicados)
    assert t2 == t1 + 1
    cli.entregar(T_ACK_CMD, resposta_reset(t2))
    assert esperar(lambda: col.respostas)
    assert col.respostas == [(DEV, "reset_counters", True, "Contadores zerados: chd2.")]


def test_eco_do_proprio_comando_e_ignorado(montar, inicio):
    p, cli, relogio, col, _ = montar()
    p.zerar_contadores(DEV, [1]).result(5)
    topico, payload, _q = cli.publicados[0]
    cli.entregar(topico, payload.encode())          # o broker devolve o nosso comando (assinatura cobre)
    time.sleep(0.3)
    assert p.contadores == (0, 0, 0)


def test_topico_de_resposta_assinado_quando_nao_coberto(montar, inicio):
    p, cli, relogio, col, _ = montar(assinatura="NOVUS/+/events")
    p.zerar_contadores("X9", [1]).result(5)
    assert cli.assinaturas_extras == ["NOVUS/+/ack/command"]
    assert cli.publicados[0][0] == "NOVUS/X9/command"
    p.pedir_diagnostico("X9").result(5)
    assert cli.assinaturas_extras == ["NOVUS/+/ack/command"]
    assert len(col.com("Assinando NOVUS/+/ack/command")) == 1


def test_canais_vistos(montar, inicio):
    p, cli, relogio, col, _ = montar()
    cli.entregar(T_DADOS, novus(1, ts=inicio))
    cli.entregar(T_DADOS, novus(1, ts=inicio + 10))                # mesmo conjunto: não repete
    cli.entregar(T_DADOS, {"pid": 1, "device_id": DEV, "channels": {
        "timestamp": int(inicio + 20), "chd1_value": 1, "chd2_value": 2, "chd3_value": 3, "chd10_value": 0,
        "ch1_user_range": 2.17}})
    cli.entregar(T_DADOS, novus(1, device="B", ts=inicio))
    cli.entregar(T_DADOS, EVENTO_MANUAL)                           # evento não mexe nos canais
    assert esperar(lambda: len(col.com("Mensagem de")) == 4)
    assert col.canais == [(DEV, ["chd1", "chd2", "chd3"]), (DEV, ["ch1", "chd1", "chd2", "chd3", "chd10"]),
                          ("B", ["chd1", "chd2", "chd3"])]


# =========================================================================================== ClienteMqtt real


def _rc(valor: int, falha: bool = False):
    return SimpleNamespace(value=valor, is_failure=falha, __str__=lambda self: str(valor))


def test_cliente_mqtt_publicar_e_assinaturas_extras():
    logs, eventos = [], []
    c = ClienteMqtt("127.0.0.1", 1883, "NOVUS/+/events", 2, ao_mensagem=lambda *a: None,
                    ao_status=lambda e, d: None, logar=lambda n, m: logs.append((n, m)),
                    ao_evento=lambda *a: eventos.append(a))
    assert c.publicar("NOVUS/X/command", "{}", 1) == (False, "não conectado ao broker")
    pedidos = []
    mids = iter(range(1, 100))
    paho = SimpleNamespace(subscribe=lambda t, qos=0: (pedidos.append((t, qos)) or (0, next(mids))))
    c.assinar_extra("NOVUS/+/ack/command", 1)       # desconectado: só guarda
    assert pedidos == []
    c._on_connect(paho, None, SimpleNamespace(session_present=False), _rc(0), None)
    assert pedidos == [("NOVUS/+/events", 2), ("NOVUS/+/ack/command", 1)]
    c._on_subscribe(paho, None, 2, [_rc(1)], None)
    assert ("INFO", "Assinado NOVUS/+/ack/command (QoS 1)") in logs
    c._on_subscribe(paho, None, 1, [_rc(1)], None)
    assert ("INFO", "Assinado NOVUS/+/events (QoS 2; broker concedeu QoS 1)") in logs
    # reconexão: assina de novo as duas
    pedidos.clear()
    c._on_connect(paho, None, SimpleNamespace(session_present=False), _rc(0), None)
    assert [t for t, _ in pedidos] == ["NOVUS/+/events", "NOVUS/+/ack/command"]
    c._on_subscribe(paho, None, 4, [_rc(128, True)], None)
    assert (eventos[-1][0], eventos[-1][2]) == ("assinatura_recusada", "NOVUS/+/ack/command")
    # mensagens do filtro extra passam; as de fora, não
    recebidas = []
    c._ao_mensagem = lambda t, p, q, r: recebidas.append(t)
    for t in ("NOVUS/X/ack/command", "NOVUS/X/events", "NOVUS/X/config"):
        c._on_message(None, None, SimpleNamespace(topic=t, payload=b"{}", qos=1, retain=False))
    assert recebidas == ["NOVUS/X/ack/command", "NOVUS/X/events"]


# =========================================================================================== integração amqtt


def test_integracao_amqtt_dispositivo_falso_responde_comandos(tmp_path):
    pytest.importorskip("amqtt.broker")
    import paho.mqtt.client as mqtt
    from paho.mqtt.enums import CallbackAPIVersion

    from tests.test_queda_broker import BrokerLocal, _porta_livre

    porta = _porta_livre()
    broker = BrokerLocal(porta)
    broker.subir()
    recebidos_dev: list[dict] = []

    # dispositivo falso: assina NOVUS/Disp_Falso/command e responde em ack/command
    dev = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="digirail-falso")

    def ao_comando(client, userdata, msg):
        pedido = json.loads(msg.payload)
        recebidos_dev.append(pedido)
        ts, desired = pedido["timestamp"], pedido["desired"]
        if "diag" in desired:
            reported = {"diag": DIAG}
        else:
            erro = 0 if set(desired["reset_counters"]) <= {"reset_chd1", "reset_chd2"} else 1
            reported = {"reset_counters": {"error": erro}}
        client.publish("NOVUS/Disp_Falso/ack/command", json.dumps(
            {"pid": 51518482, "device_id": "Disp_Falso", "timestamp": ts, "reported": reported}), qos=1)

    dev.on_message = ao_comando
    dev.connect("127.0.0.1", porta)
    dev.subscribe("NOVUS/Disp_Falso/command", qos=1)
    dev.loop_start()

    cfg = Config()
    cfg.sqlserver = SqlServerConfig(servidor="", banco="")
    cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    cfg.gravacao.intervalo_gravacao_segundos = 0
    cfg.gravacao.lote_segundos = 0.1
    col = Coletor()
    p = Pipeline(cfg, sessao_persistente=False, pasta=tmp_path, **col.kwargs())
    try:
        # assinatura só dos dados: o ack/command não está coberto e precisa ser assinado
        p.conectar("127.0.0.1", porta, "NOVUS/+/events", 1).result(5)
        assert esperar(lambda: col.com("Assinado NOVUS/+/events"), 15), col.logs
        agora = int(time.time())
        dev.publish("NOVUS/Disp_Falso/events", json.dumps(novus(51518482, device="Disp_Falso", ts=agora)),
                    qos=1).wait_for_publish(5)
        # 1ª leitura do dispositivo: diagnóstico automático, respondido pelo dispositivo falso
        assert esperar(lambda: col.diags, 15), col.logs
        assert col.diags == [("Disp_Falso", DIAG)]
        assert col.com("Assinado NOVUS/+/ack/command (QoS 1)")
        assert col.canais == [("Disp_Falso", ["chd1", "chd2", "chd3"])]

        p.zerar_contadores("Disp_Falso", [1, 2])
        assert esperar(lambda: col.respostas, 15), col.logs
        assert col.respostas == [("Disp_Falso", "reset_counters", True, "Contadores zerados: chd1, chd2.")]
        p.zerar_contadores("Disp_Falso", [5])
        assert esperar(lambda: len(col.respostas) == 2, 15), col.logs
        assert col.respostas[1][2] is False and "erro 1" in col.respostas[1][3]
        assert recebidos_dev[1]["desired"] == {"reset_counters": {"reset_chd1": 1, "reset_chd2": 1}}
        assert esperar(lambda: p.contadores[1] == 1)
        # 1 leitura + 3 respostas recebidas; 1 gravada (contingência); 1 erro (o reset recusado)
        assert esperar(lambda: p.contadores == (4, 1, 1)), p.contadores
        assert json.loads(col.erros[0])["categoria"] == "comando"
    finally:
        assert p.encerrar(5)
        dev.loop_stop()
        dev.disconnect()
        broker.fechar()


# =========================================================================================== bridge (Qt)


def test_bridge_real_sinais_e_metodos_novos(tmp_path, inicio, monkeypatch):
    import os
    from functools import partial

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    QtWidgets = pytest.importorskip("PySide6.QtWidgets")
    from app import bridge as modulo_bridge

    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    relogio = Relogio(inicio)
    monkeypatch.setattr(modulo_bridge, "Pipeline", partial(Pipeline, relogio=relogio, fabrica_cliente=ClienteFalso,
                                                           pasta=tmp_path))
    cfg = Config()
    cfg.sqlserver = SqlServerConfig(servidor="", banco="")
    cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    b = modulo_bridge.PipelineBridge(cfg)
    canais, diags, respostas = [], [], []
    b.canais_vistos.connect(lambda d, j: canais.append((d, j)))
    b.diagnostico_dispositivo.connect(lambda d, j: diags.append((d, j)))
    b.resposta_comando.connect(lambda *a: respostas.append(a))

    def esperar_qt(cond, timeout=5.0):
        fim = time.monotonic() + timeout
        while time.monotonic() < fim:
            app.processEvents()
            if cond():
                return True
            time.sleep(0.01)
        return False

    ClienteFalso.ultimo = None                         # não pegar o cliente de outro teste
    try:
        b.conectar("127.0.0.1", 1883, ASSINATURA, 1)
        assert esperar_qt(lambda: ClienteFalso.ultimo is not None)
        cli = ClienteFalso.ultimo
        cli.entregar(T_DADOS, novus(1, ts=inicio))          # 1ª vez: canais + diagnóstico automático
        assert esperar_qt(lambda: canais and cli.publicados)
        assert canais == [(DEV, '["chd1", "chd2", "chd3"]')]
        cli.entregar(T_ACK_CMD, resposta_diag(json.loads(cli.publicados[0][1])["timestamp"]))
        assert esperar_qt(lambda: diags)
        assert json.loads(diags[0][1]) == DIAG
        b.zerar_contadores(DEV, [2])
        assert esperar_qt(lambda: len(cli.publicados) == 2)
        cli.entregar(T_ACK_CMD, resposta_reset(json.loads(cli.publicados[1][1])["timestamp"]))
        assert esperar_qt(lambda: respostas)
        assert respostas == [(DEV, "reset_counters", True, "Contadores zerados: chd2.")]
        b.pedir_diagnostico(DEV)
        assert esperar_qt(lambda: len(cli.publicados) == 3)
    finally:
        b.encerrar(5)
