"""Core: tópicos por dispositivo (``dispositivos.json``), assinaturas extras e respostas fora do padrão NOVUS.

Caso real: o NXperience do ``Teste_Alumbra`` publica a confirmação de comando em ``NOÏUS/Teste_Alumbra/ack/command``
(V trocado por Ï) e a assinatura do usuário é ``NOVUS/Teste_Alumbra/#``: sem o tópico salvo, as respostas de
``diag`` e ``reset_counters`` se perdiam. (Os testes da tela ficam em test_topicos.py.)
"""
from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import pytest

from app.config import Config, DispositivosConfig, SqlServerConfig
from app.core import topicos as tp
from app.core.mqtt_client import ClienteMqtt
from app.core.pipeline import Pipeline
from tests.test_amostragem import ClienteFalso, esperar, novus
from tests.test_digirail import (ASSINATURA, DEV, DIAG, T_ACK_CMD, T_CMD, T_DADOS, Coletor, montar,  # noqa: F401
                                 inicio, resposta_diag, resposta_reset)

ACK_REAL = "NOÏUS/Teste_Alumbra/ack/command"
PADRAO = {"events": T_DADOS, "config": f"NOVUS/{DEV}/config", "ack_config": f"NOVUS/{DEV}/ack/config",
          "command": T_CMD, "ack_command": T_ACK_CMD}


def _salvar_real(p: Pipeline) -> None:
    p.salvar_topicos_dispositivo(DEV, {**PADRAO, "ack_command": ACK_REAL}).result(5)


def _cfg(tmp_path) -> Config:
    cfg = Config()
    cfg.sqlserver = SqlServerConfig(servidor="", banco="")
    cfg.gravacao.pasta_contingencia = str(tmp_path / "c")
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    cfg.dispositivos.diagnostico_minutos = 0
    return cfg


# =========================================================================================== armazenamento


def test_padroes_vem_dos_modelos(tmp_path):
    loja = tp.TopicosDispositivos(tmp_path / "dispositivos.json")
    assert loja.efetivos(DEV) == PADRAO
    cfg = DispositivosConfig(topico_dados="X/{id}/dados", topico_resposta="X/{id}/resp")
    assert tp.padroes("D1", cfg)["events"] == "X/D1/dados" and tp.padroes("D1", cfg)["ack_command"] == "X/D1/resp"
    assert not (tmp_path / "dispositivos.json").exists()       # só ler não cria o arquivo


def test_gravacao_atomica_utf8_com_acento_preservado(tmp_path, monkeypatch):
    caminho = tmp_path / "dispositivos.json"
    loja = tp.TopicosDispositivos(caminho)
    salvos = loja.salvar(DEV, {"ack_command": "  NOÏUS/Teste_Alumbra/ack/command  ",
                               "config": "NOVUS/Teste Alumbra/config"})
    assert salvos["ack_command"] == ACK_REAL                     # só as pontas perdem o espaço
    assert salvos["config"] == "NOVUS/Teste Alumbra/config"      # espaço interno fica
    assert salvos["events"] == T_DADOS                           # faltou: padrão
    bruto = caminho.read_bytes()
    assert "NOÏUS".encode("utf-8") in bruto and b"\\u00cf" not in bruto      # ensure_ascii=False, UTF-8
    assert json.loads(bruto.decode("utf-8")) == {DEV: salvos}
    assert [f.name for f in tmp_path.iterdir()] == ["dispositivos.json"]    # nenhum temporário sobrando
    # outra instância (reinício do programa) lê igual
    assert tp.TopicosDispositivos(caminho).efetivos(DEV) == salvos
    assert tp.TopicosDispositivos(caminho).por_topico(ACK_REAL) == (DEV, "ack_command")

    # falha no meio da gravação: o arquivo antigo continua inteiro e o temporário some
    def explodir(_a, _b):
        raise OSError("disco cheio")
    monkeypatch.setattr(tp.os, "replace", explodir)
    with pytest.raises(OSError):
        loja.salvar("Outro", {"command": "A/command"})
    assert json.loads(caminho.read_text(encoding="utf-8")) == {DEV: salvos}
    assert [f.name for f in tmp_path.iterdir()] == ["dispositivos.json"]
    assert loja.salvos("Outro") is None                          # memória não mudou


@pytest.mark.parametrize("valor,trecho", [
    ("", "Informe"), ("   ", "Informe"), ("NOVUS/+/ack/command", "'+' ou '#'"), ("NOVUS/#", "'+' ou '#'"),
    ("NOVUS/a\x00b", "nulo"), ("x" * 65536, "longo demais"), (None, None), (5, "texto"),
])
def test_validacao_de_topicos_invalidos(tmp_path, valor, trecho):
    loja = tp.TopicosDispositivos(tmp_path / "dispositivos.json")
    if valor is None:             # None = "não informado": usa o padrão
        assert loja.salvar(DEV, {"ack_command": None})["ack_command"] == T_ACK_CMD
        return
    with pytest.raises(ValueError) as exc:
        loja.salvar(DEV, {"ack_command": valor})
    assert trecho in str(exc.value)
    assert not (tmp_path / "dispositivos.json").exists()


def test_validacao_chave_e_device(tmp_path):
    loja = tp.TopicosDispositivos(tmp_path / "d.json")
    with pytest.raises(ValueError, match="desconhecida"):
        loja.salvar(DEV, {"ack": "x"})
    with pytest.raises(ValueError):
        loja.salvar("a/b", {"command": "x"})
    assert tp.validar_topico("ÁÏ/çã/ack/command") == "ÁÏ/çã/ack/command"
    assert tp.validar_topico("é" * 32767) == "é" * 32767        # 65534 bytes: cabe


@pytest.mark.parametrize("conteudo", [b"{quebrado", b"[1, 2]", b"\xff\xfe\x00", b'{"Teste_Alumbra": 3}',
                                      b'{"Teste_Alumbra": {"command": "NOVUS/+/x"}}'])
def test_json_corrompido_nao_derruba(tmp_path, conteudo):
    caminho = tmp_path / "dispositivos.json"
    caminho.write_bytes(conteudo)
    avisos = []
    loja = tp.TopicosDispositivos(caminho, logar=lambda n, m: avisos.append((n, m)))
    assert loja.efetivos(DEV) == PADRAO
    assert avisos and all(n == "AVISO" for n, _ in avisos) and loja.avisos
    # salvar depois sobrescreve com um arquivo bom
    loja.salvar(DEV, {"ack_command": ACK_REAL})
    assert tp.TopicosDispositivos(caminho).efetivos(DEV)["ack_command"] == ACK_REAL


def test_json_corrompido_no_pipeline_vira_aviso_na_conexao(tmp_path):
    (tmp_path / "dispositivos.json").write_text("{ruim", encoding="utf-8")
    col = Coletor()
    p = Pipeline(_cfg(tmp_path), fabrica_cliente=ClienteFalso, pasta=tmp_path, **col.kwargs())
    try:
        assert p.topicos_do_dispositivo(DEV) == PADRAO
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        assert col.com("ilegível", "AVISO") and (tmp_path / "dispositivos.json.corrompido").exists()
    finally:
        p.encerrar(5)


def test_restaurar(tmp_path):
    loja = tp.TopicosDispositivos(tmp_path / "d.json")
    assert loja.restaurar(DEV) is False
    loja.salvar(DEV, {"ack_command": ACK_REAL})
    loja.salvar("B", {"command": "B/cmd"})
    assert loja.restaurar(DEV) is True
    assert loja.efetivos(DEV) == PADRAO and loja.por_topico(ACK_REAL) is None
    assert json.loads((tmp_path / "d.json").read_text(encoding="utf-8")).keys() == {"B"}


# =========================================================================================== pipeline (cliente falso)


def test_caso_real_assina_extra_e_casa_diag_e_reset(montar, inicio):
    p, cli, relogio, col, _ = montar()
    assert p.topico_coberto(T_ACK_CMD) and not p.topico_coberto(ACK_REAL)
    assert p.topicos_ouvidos() == [ASSINATURA]
    _salvar_real(p)
    # só o ack fora do padrão vira extra (events e ack/config já são cobertos por NOVUS/Teste_Alumbra/#)
    assert cli.assinaturas_extras == [ACK_REAL]
    assert p.topico_coberto(ACK_REAL) and p.topicos_ouvidos() == [ASSINATURA, ACK_REAL]
    assert col.com(f"Tópicos de Teste_Alumbra salvos; assinando '{ACK_REAL}'", "INFO")
    assert p.topicos_do_dispositivo(DEV)["ack_command"] == ACK_REAL

    # diag manual respondido no tópico com Ï, payload sem device_id: é do Teste_Alumbra
    relogio.ir(inicio + 2)
    p.pedir_diagnostico(DEV).result(5)
    assert cli.publicados[-1][0] == T_CMD
    ts = json.loads(cli.publicados[-1][1])["timestamp"]
    resp = resposta_diag(ts)
    del resp["device_id"]
    cli.entregar(ACK_REAL, resp)
    assert esperar(lambda: col.diags)
    assert col.diags == [(DEV, DIAG)] and col.respostas == [(DEV, "diag", True, "Diagnóstico recebido.")]

    # diag espontâneo (sem pedido) no tópico com Ï: ainda é do Teste_Alumbra
    cli.entregar(ACK_REAL, {"timestamp": 1, "reported": {"diag": DIAG}})
    assert esperar(lambda: len(col.diags) == 2) and col.diags[1][0] == DEV

    # reset_counters casa pela resposta no tópico com Ï
    relogio.ir(inicio + 5)
    p.zerar_contadores(DEV, [2]).result(5)
    ts = json.loads(cli.publicados[-1][1])["timestamp"]
    cli.entregar(ACK_REAL, resposta_reset(ts))
    assert esperar(lambda: len(col.respostas) == 2)
    assert col.respostas[1] == (DEV, "reset_counters", True, "Contadores zerados: chd2.")
    assert p.contadores[2] == 0 and col.erros == []


def test_ack_salvo_fora_do_padrao_e_resposta_mesmo_sem_reported(montar, inicio):
    p, cli, relogio, col, _ = montar()
    p.salvar_topicos_dispositivo(DEV, {"ack_command": "outro/lugar/respostas", "events": "outro/lugar/dados"}
                                 ).result(5)
    assert set(cli.assinaturas_extras) == {"outro/lugar/respostas", "outro/lugar/dados"}
    cli.entregar("outro/lugar/respostas", {"qualquer": 1})      # não é leitura nem erro
    leitura = novus(1, ts=inicio)
    del leitura["device_id"]
    cli.entregar("outro/lugar/dados", leitura)                   # sem device_id: o dono do tópico
    assert esperar(lambda: col.vistos)
    assert col.vistos[0][:2] == (DEV, "outro/lugar/dados")
    assert esperar(lambda: col.com("Resposta de Teste_Alumbra ignorada"))
    assert p.contadores[2] == 0


def test_restaurar_volta_ao_padrao_e_desassina(montar):
    p, cli, relogio, col, _ = montar()
    _salvar_real(p)
    assert cli.assinaturas_extras == [ACK_REAL]
    p.restaurar_topicos_padrao(DEV).result(5)
    assert cli.assinaturas_extras == []
    assert p.topicos_do_dispositivo(DEV) == PADRAO
    assert not p.topico_coberto(ACK_REAL) and p.topicos_ouvidos() == [ASSINATURA]
    assert col.com(f"Tópicos de Teste_Alumbra restaurados para o padrão; assinatura cancelada de '{ACK_REAL}'")
    assert p._topicos.por_topico(ACK_REAL) is None


def test_salvar_desconectado_vale_na_conexao(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.mqtt.topico = ASSINATURA
    col = Coletor()
    p = Pipeline(cfg, fabrica_cliente=ClienteFalso, pasta=tmp_path, **col.kwargs())
    try:
        assert p.topicos_ouvidos() == []
        assert not p.topico_coberto(ACK_REAL) and p.topico_coberto(T_ACK_CMD)   # pela assinatura de [mqtt]
        p.salvar_topicos_dispositivo(DEV, {"ack_command": ACK_REAL}).result(5)
        assert col.com(f"Tópicos de Teste_Alumbra salvos; serão assinados ao conectar '{ACK_REAL}'")
        assert p.topico_coberto(ACK_REAL)        # desconectado: o que seria ouvido ao conectar
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        assert ClienteFalso.ultimo.assinaturas_extras == [ACK_REAL] and p.topico_coberto(ACK_REAL)
        p.desconectar().result(5)
        assert p.topicos_ouvidos() == [] and p.topico_coberto(ACK_REAL)
    finally:
        p.encerrar(5)


def test_salvar_invalido_levanta_e_nao_muda_nada(montar, tmp_path):
    p, cli, relogio, col, _ = montar()
    with pytest.raises(ValueError, match="'\\+' ou '#'"):
        p.salvar_topicos_dispositivo(DEV, {"ack_command": "NOVUS/+/ack/command"})
    with pytest.raises(ValueError, match="Informe"):
        p.salvar_topicos_dispositivo(DEV, {"command": ""})
    assert col.com("Tópicos de Teste_Alumbra não salvos", "ERRO")
    assert not (tmp_path / "dispositivos.json").exists() and cli.assinaturas_extras == []


def test_comando_publicado_no_command_salvo(montar, inicio):
    p, cli, relogio, col, _ = montar()
    p.salvar_topicos_dispositivo(DEV, {"command": "NOÏUS/Teste_Alumbra/command"}).result(5)
    p.zerar_contadores(DEV, [1]).result(5)
    assert cli.publicados[-1][0] == "NOÏUS/Teste_Alumbra/command"


def test_diag_automatico_sem_resposta_emite_resposta_comando(montar, inicio):
    p, cli, relogio, col, _ = montar(diag_min=30)
    _salvar_real(p)
    relogio.ir(inicio + 1)
    cli.entregar(T_DADOS, novus(1, ts=inicio + 1))
    assert esperar(lambda: cli.publicados)
    relogio.ir(inicio + 1 + 15)
    assert esperar(lambda: col.respostas)
    assert col.respostas == [(DEV, "diag", False, "Sem resposta do dispositivo em 15 s (pedido em "
                              f"'NOVUS/Teste_Alumbra/command', resposta esperada em '{ACK_REAL}').")]
    assert col.com("Diagnóstico de Teste_Alumbra: Sem resposta", "AVISO")
    assert p.contadores[2] == 0 and col.erros == []


def test_diag_automatico_com_resposta_nao_emite_resposta_comando(montar, inicio):
    p, cli, relogio, col, _ = montar(diag_min=30)
    relogio.ir(inicio + 1)
    cli.entregar(T_DADOS, novus(1, ts=inicio + 1))
    assert esperar(lambda: cli.publicados)
    cli.entregar(T_ACK_CMD, resposta_diag(json.loads(cli.publicados[0][1])["timestamp"]))
    assert esperar(lambda: col.diags)
    relogio.ir(inicio + 60)
    time.sleep(0.3)
    assert col.respostas == []


# =========================================================================================== ClienteMqtt real (sem rede)


def _rc(valor: int, falha: bool = False):
    return SimpleNamespace(value=valor, is_failure=falha, __str__=lambda self: str(valor))


class _PahoFalso:
    def __init__(self) -> None:
        self.pedidos: list[tuple[str, int]] = []
        self.cancelados: list[str] = []
        self._mid = 0

    def subscribe(self, topico, qos=0):
        self._mid += 1
        self.pedidos.append((topico, qos))
        return 0, self._mid

    def unsubscribe(self, topico):
        self._mid += 1
        self.cancelados.append(topico)
        return 0, self._mid


def test_reconexao_reassina_os_extras(tmp_path):
    criados: list[ClienteMqtt] = []

    def fabrica(*a, **kw):
        c = ClienteMqtt(*a, **kw)
        c.iniciar = lambda: None             # sem rede: o teste chama os callbacks do paho
        criados.append(c)
        return c

    col = Coletor()
    p = Pipeline(_cfg(tmp_path), fabrica_cliente=fabrica, pasta=tmp_path, sessao_persistente=False, **col.kwargs())
    try:
        p.salvar_topicos_dispositivo(DEV, {"ack_command": ACK_REAL}).result(5)
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        c = criados[-1]
        paho = _PahoFalso()
        c._cliente = paho
        c._on_connect(paho, None, SimpleNamespace(session_present=False), _rc(0), None)
        assert paho.pedidos == [(ASSINATURA, 1), (ACK_REAL, 1)]
        c._on_disconnect(paho, None, None, _rc(128, True), None)
        paho.pedidos.clear()
        c._on_connect(paho, None, SimpleNamespace(session_present=False), _rc(0), None)
        assert paho.pedidos == [(ASSINATURA, 1), (ACK_REAL, 1)]      # reconexão: assina de novo
        c._on_subscribe(paho, None, 4, [_rc(1)], None)
        assert col.com(f"Assinado {ACK_REAL} (QoS 1)")
        # mensagens no tópico com Ï passam pelo filtro do cliente
        recebidas = []
        c._ao_mensagem = lambda t, *_: recebidas.append(t)
        c._on_message(None, None, SimpleNamespace(topic=ACK_REAL, payload=b"{}", qos=1, retain=False))
        assert recebidas == [ACK_REAL]
        # restaurar: UNSUBSCRIBE e não volta na próxima reconexão
        p.restaurar_topicos_padrao(DEV).result(5)
        assert paho.cancelados == [ACK_REAL] and col.com(f"Assinatura de {ACK_REAL} cancelada")
        paho.pedidos.clear()
        c._on_connect(paho, None, SimpleNamespace(session_present=False), _rc(0), None)
        assert paho.pedidos == [(ASSINATURA, 1)]
    finally:
        p.encerrar(5)


def test_topic_matches_sub_aceita_nao_ascii():
    import paho.mqtt.client as mqtt
    assert mqtt.topic_matches_sub("NOÏUS/+/ack/command", ACK_REAL)
    assert mqtt.topic_matches_sub("NOÏUS/#", ACK_REAL)
    assert not mqtt.topic_matches_sub(ASSINATURA, ACK_REAL)


# =========================================================================================== integração amqtt


def test_integracao_amqtt_ack_com_i_trema(tmp_path):
    pytest.importorskip("amqtt.broker")
    import paho.mqtt.client as mqtt
    from paho.mqtt.enums import CallbackAPIVersion

    from tests.test_queda_broker import BrokerLocal, _porta_livre

    porta = _porta_livre()
    broker = BrokerLocal(porta)
    broker.subir()
    pedidos: list[dict] = []

    # dispositivo falso: igual ao Teste_Alumbra real, responde em NOÏUS/Teste_Alumbra/ack/command
    dev = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="digirail-falso-i-trema")

    def ao_comando(client, userdata, msg):
        pedido = json.loads(msg.payload)
        pedidos.append(pedido)
        reported = {"diag": DIAG} if "diag" in pedido["desired"] else {"reset_counters": {"error": 0}}
        client.publish(ACK_REAL, json.dumps({"pid": 51518482, "device_id": DEV, "timestamp": pedido["timestamp"],
                                             "reported": reported}, ensure_ascii=False).encode("utf-8"), qos=1)

    dev.on_message = ao_comando
    dev.connect("127.0.0.1", porta)
    dev.subscribe(T_CMD, qos=1)
    dev.loop_start()

    # observador: confirma que o broker entrega o tópico com Ï intacto
    vistos: list[str] = []
    obs = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="observador-i-trema")
    obs.on_message = lambda c, u, m: vistos.append(m.topic)
    obs.connect("127.0.0.1", porta)
    obs.subscribe("NOÏUS/#", qos=1)
    obs.loop_start()

    cfg = _cfg(tmp_path)
    cfg.dispositivos.diagnostico_minutos = 30
    cfg.gravacao.intervalo_gravacao_segundos = 0
    cfg.gravacao.lote_segundos = 0.1
    col = Coletor()
    p = Pipeline(cfg, sessao_persistente=False, pasta=tmp_path, **col.kwargs())
    try:
        p.conectar("127.0.0.1", porta, ASSINATURA, 1).result(5)
        assert esperar(lambda: col.com(f"Assinado {ASSINATURA}"), 15), col.logs
        assert not p.topico_coberto(ACK_REAL)
        # salvar com o cliente conectado: assina na hora
        _salvar_real(p)
        assert esperar(lambda: col.com(f"Assinado {ACK_REAL} (QoS 1)"), 15), col.logs
        assert p.topico_coberto(ACK_REAL) and ACK_REAL in p.topicos_ouvidos()

        # 1ª leitura: diagnóstico automático, respondido no tópico com Ï
        dev.publish(T_DADOS, json.dumps(novus(51518482, ts=int(time.time()))), qos=1).wait_for_publish(5)
        assert esperar(lambda: col.diags, 15), col.logs
        assert col.diags == [(DEV, DIAG)]
        assert ACK_REAL in vistos                 # o broker roteou o tópico com Ï sem alterar

        p.zerar_contadores(DEV, [1, 3])
        assert esperar(lambda: col.respostas, 15), col.logs
        assert col.respostas == [(DEV, "reset_counters", True, "Contadores zerados: chd1, chd3.")]
        assert pedidos[-1]["desired"] == {"reset_counters": {"reset_chd1": 1, "reset_chd3": 1}}

        # restaurar com o cliente conectado: UNSUBSCRIBE; nova resposta no tópico com Ï não chega mais
        p.restaurar_topicos_padrao(DEV).result(5)
        assert esperar(lambda: col.com(f"Assinatura de {ACK_REAL} cancelada"), 5)
        assert not p.topico_coberto(ACK_REAL)
        recebidas = p.contadores[0]
        n_vistos = len(vistos)
        dev.publish(ACK_REAL, json.dumps({"timestamp": 1, "reported": {"diag": DIAG}}), qos=1).wait_for_publish(5)
        assert esperar(lambda: len(vistos) > n_vistos, 5)
        time.sleep(0.5)
        assert p.contadores[0] == recebidas and len(col.diags) == 1
        assert p.contadores[2] == 0, col.erros
    finally:
        assert p.encerrar(5)
        for c in (dev, obs):
            c.loop_stop()
            c.disconnect()
        broker.fechar()
        assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]
