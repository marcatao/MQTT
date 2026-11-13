"""Funções puras do protocolo MQTT do NOVUS DigiRail IoT (sem estado, sem rede, sem Qt).

Referência: docs/digirail_iot_mqtt.md (seções 3 e 5) e o manual (págs. 15–24, 92–105).

- Comandos (tópico Command): ``{"timestamp": T, "desired": {"<comando>": {...}}}``; ``T`` volta igual na resposta
  (tópico Command Ack) e serve de id de correlação. ``reported.<comando>.error``: 0 = êxito, 1 = parâmetro fora da
  faixa, 2 = parâmetro desconhecido.
- Canais em ``channels``: ``chdN_value`` (digitais 1–6), ``chN_user_range`` (analógicas 1–2), ``chrN_value``
  (remotos 1–8). Canal desabilitado não aparece.
- Eventos: ``{"events": {"chd1": {"timestamp": 1585819219.685, "edge": 1}}}`` (edge 1 = subida, 0 = descida).
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

CANAIS_RESET = range(1, 7)          # reset_counters aceita chd1..chd6
_RE_CANAL = re.compile(r"^(chd|chr|ch)(\d+)(?:_|$)", re.IGNORECASE)
_RE_DIGITOS = re.compile(r"(\d+)")
_CARACTERES_PROIBIDOS = ("+", "#", "/", "\x00")

ERROS_COMANDO = {1: "parâmetro fora da faixa", 2: "parâmetro desconhecido"}


def validar_device_id(device_id: Any) -> str:
    """device_id usado para montar o tópico do comando: sem curingas nem '/' (ValueError se inválido)."""
    d = str(device_id or "").strip()
    if not d:
        raise ValueError("Informe o dispositivo.")
    if any(c in d for c in _CARACTERES_PROIBIDOS):
        raise ValueError(f"Dispositivo inválido para tópico MQTT: {d!r} (não pode ter '+', '#' nem '/').")
    return d


def validar_canais_reset(canais: Any) -> list[int]:
    """Canais para zerar: inteiros de 1 a 6, sem repetição, em ordem crescente (ValueError se inválido)."""
    try:
        lista = sorted({int(c) for c in canais})
    except (TypeError, ValueError):
        raise ValueError(f"Canais inválidos: {canais!r} (use números de 1 a 6).") from None
    if not lista:
        raise ValueError("Escolha pelo menos um canal para zerar.")
    fora = [c for c in lista if c not in CANAIS_RESET]
    if fora:
        raise ValueError(f"Canal fora da faixa: {', '.join(map(str, fora))} (use 1 a 6).")
    return lista


def topico_do_modelo(modelo: str, device_id: str) -> str:
    """``"NOVUS/{id}/command"`` + ``"Teste_Alumbra"`` → ``"NOVUS/Teste_Alumbra/command"``."""
    return modelo.replace("{id}", device_id)


def filtro_do_modelo(modelo: str) -> str:
    """Filtro de assinatura para todos os dispositivos: ``{id}`` vira ``+``."""
    return modelo.replace("{id}", "+")


def payload_comando(timestamp: int, comando: str, desejado: dict) -> str:
    """JSON compacto do pedido, na ordem do manual: ``{"timestamp":T,"desired":{comando:{...}}}``."""
    return json.dumps({"timestamp": int(timestamp), "desired": {comando: desejado}}, separators=(",", ":"),
                      ensure_ascii=False)


def desejado_reset(canais: list[int]) -> dict:
    return {f"reset_chd{c}": 1 for c in canais}


def mensagem_reset(erro: Any, canais: list[int] | None = None) -> tuple[bool, str]:
    """(sucesso, mensagem em português) a partir de ``reported.reset_counters.error``."""
    try:
        codigo = int(erro)
    except (TypeError, ValueError):
        return False, f"Resposta do dispositivo sem código de erro válido ({erro!r})."
    if codigo == 0:
        nomes = ", ".join(f"chd{c}" for c in canais) if canais else "pedidos"
        return True, f"Contadores zerados: {nomes}."
    return False, (f"Dispositivo recusou (erro {codigo}). Confira se o canal tem 'reset por MQTT' habilitado "
                   "no NXperience.")


SAIDAS = (1, 2)                     # saídas digitais NPN: out1, out2 (manual p. 6, 17 e 45)


def validar_saida(saida: Any) -> int:
    """Número da saída digital (1 ou 2); ValueError se inválido."""
    try:
        n = int(saida)
    except (TypeError, ValueError):
        raise ValueError(f"Saída inválida: {saida!r} (use 1 ou 2).") from None
    if isinstance(saida, bool) or n not in SAIDAS:
        raise ValueError(f"Saída inválida: {saida!r} (use 1 ou 2).")
    return n


def desejado_output(saida: int, valor: int) -> dict:
    """Corpo de ``desired.output`` (manual 6.5.1): ``{"out1": 1}`` liga, ``{"out1": 0}`` desliga."""
    return {f"out{int(saida)}": 1 if valor else 0}


def mensagem_output(erro: Any, saida: int) -> tuple[bool, str]:
    """(sucesso, mensagem) a partir de ``reported.output.error`` (0 ok, 1 fora da faixa, 2 desconhecido)."""
    try:
        codigo = int(erro)
    except (TypeError, ValueError):
        return False, f"Resposta do comando da saída {saida} sem código de erro válido ({erro!r})."
    if codigo == 0:
        return True, f"Saída {saida} acionada."
    motivo = ERROS_COMANDO.get(codigo, "erro desconhecido")
    return False, f"O aparelho recusou o comando da saída {saida} (erro {codigo}: {motivo})."


def chave_natural(nome: str) -> list:
    """Ordem natural: chd2 antes de chd10."""
    return [int(p) if p.isdigit() else p.lower() for p in _RE_DIGITOS.split(nome)]


def canais_presentes(channels: Any) -> list[str]:
    """Canais (``chdN``, ``chN``, ``chrN``) presentes em ``channels``, sem repetição, em ordem natural."""
    if not isinstance(channels, dict):
        return []
    achados = set()
    for chave in channels:
        m = _RE_CANAL.match(str(chave))
        if m:
            achados.add(f"{m.group(1).lower()}{int(m.group(2))}")
    return sorted(achados, key=chave_natural)


def _hora_local(ts: Any) -> str | None:
    try:
        t = float(ts)
    except (TypeError, ValueError):
        return None
    if t >= 1e11:          # milissegundos inteiros
        t /= 1000.0
    try:
        return datetime.fromtimestamp(t).strftime("%H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return None


def texto_evento(events: Any, device_id: str | None) -> str:
    """``"Evento chd1 (borda de subida) em Teste_Alumbra às 14:31:05"`` (vários canais separados por vírgula)."""
    quem = device_id or "dispositivo desconhecido"
    if not isinstance(events, dict) or not events:
        return f"Evento em {quem} (formato não reconhecido)"
    partes, horas = [], []
    for canal, dados in events.items():
        borda = ""
        if isinstance(dados, dict):
            edge = dados.get("edge")
            if edge in (1, "1", True):
                borda = " (borda de subida)"
            elif edge in (0, "0", False):
                borda = " (borda de descida)"
            hora = _hora_local(dados.get("timestamp"))
            if hora and hora not in horas:
                horas.append(hora)
        partes.append(f"{canal}{borda}")
    quando = f" às {', '.join(horas)}" if horas else ""
    return f"Evento {', '.join(partes)} em {quem}{quando}"


def resumo_diag(diag: dict) -> str:
    """Resumo curto para o log: nº de série, firmware, sinal e fila do aparelho."""
    partes = []
    if diag.get("sn") not in (None, ""):
        partes.append(f"nº série {diag['sn']}")
    if diag.get("fw_v") not in (None, ""):
        partes.append(f"firmware {diag['fw_v']}")
    if diag.get("avg_rssi") not in (None, ""):
        partes.append(f"sinal médio {diag['avg_rssi']} dBm")
    elif diag.get("curr_rssi") not in (None, ""):
        partes.append(f"sinal {diag['curr_rssi']}%")
    if diag.get("mqtt_queue") not in (None, ""):
        partes.append(f"{diag['mqtt_queue']} leitura(s) na fila do aparelho")
    ip = diag.get("ipv4")
    if isinstance(ip, list) and len(ip) == 4:
        partes.append("IP " + ".".join(str(x) for x in ip))
    return ", ".join(partes) or "sem dados"


def timestamp_resposta(obj: Any) -> int | None:
    """``timestamp`` de primeiro nível de uma resposta (o mesmo do pedido)."""
    if not isinstance(obj, dict):
        return None
    valor = obj.get("timestamp")
    if isinstance(valor, bool):
        return None
    try:
        return int(float(valor))
    except (TypeError, ValueError):
        return None
