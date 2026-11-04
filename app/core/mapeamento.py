"""Mapeia uma mensagem MQTT em linhas da tabela ``dbo.IOT001`` (formato largo: uma linha por leitura).

Colunas inseridas (``LinhaIot.COLUNAS``): ``pid, device_id, ts_unix, chd1_value, chd2_value, chd3_value,
payload_json, recebido_em``. Nunca ``id`` (identity) nem ``data_hora_utc`` (calculada).

Payload real do NOVUS (tópico ``NOVUS/Teste_Alumbra/events``, confirmado pelo usuário): ``timestamp`` e os
canais vêm aninhados em ``channels``; ``timestamp`` é epoch em segundos UTC::

    {"pid":51387408,"device_id":"Teste_Alumbra","channels":{"timestamp":1793386748,"chd1_value":67,"chd2_value":119,"chd3_value":0}}

    → pid=51387408, device_id='Teste_Alumbra', ts_unix=1793386748 (2026-10-30T18:59:08Z),
      chd1=67.0000, chd2=119.0000, chd3=0.0000

As chaves aceitas para cada coluna vêm de ``[mapeamento]`` no ``config.toml`` (``MapeamentoConfig``). A busca não
diferencia maiúsculas, e ``a.b`` é um caminho aninhado.

Como cada coluna é procurada
----------------------------
1. **Nível do item**: as chaves configuradas, na ordem da configuração (``ts_unix``, ``ts``, ``timestamp``...).
   Inclui caminhos com ponto a partir do item (``chd1.value`` = ``{"chd1": {"value": x}}``) e a chave literal
   ``"chd1.value"``.
2. **Objetos aninhados**, em qualquer profundidade (não entra em listas): a chave casa pelo final do caminho,
   então ``channels.timestamp`` casa com ``timestamp`` e ``channels.chd1_value`` com ``chd1_value``; ``chd1.value``
   casa com ``x.chd1.value``. Nada é amarrado ao nome ``channels``.

O nível de cima sempre vence o aninhado. Se duas chaves aninhadas diferentes casarem com a mesma coluna (ex.:
``channels.timestamp`` e ``meta.ts``), vale a primeira na ordem do JSON e um aviso é registrado uma única vez (por
coluna e conjunto de chaves, durante a execução). Com envelope de leituras (formato 3), o item inteiro (topo e
aninhados) é procurado antes do envelope.

Formatos aceitos
----------------
1. **Objeto JSON = uma leitura**::

       {"pid": 51387408, "device_id": "Teste_Alumbra", "channels": {"timestamp": 1793386748, "chd1_value": 67}}
       {"pid": 17, "device_id": "N1", "ts": 1730000000, "chd1_value": 23.5, "chd2_value": 40, "chd3_value": null}
       {"pid": 17, "device": "N1", "timestamp": 1730000000123, "chd1": {"value": 23.5}, "chd2": "40,1"}

2. **Lista JSON de leituras = várias linhas**::

       [{"pid": 1, "device_id": "N1", "ts": ..., "chd1": 1}, {"pid": 2, ...}]

3. **Objeto com uma lista de leituras** (chave em ``mapeamento.leituras``: ``leituras``, ``readings``, ``dados``...).
   Os campos de fora valem para todos os itens, e o item tem prioridade::

       {"device_id": "N1", "leituras": [{"pid": 1, "ts": ..., "chd1": 1}, {"pid": 2, "ts": ..., "chd1": 2}]}

4. **Tabular com cabeçalho**. Delimitador ``;``, tab, ``|`` ou ``,``. Cada linha de dados vira uma leitura::

       pid;device_id;ts;chd1;chd2;chd3
       1;N1;1730000000;23,5;40;0

   Linhas ``chave=valor`` também valem (``pid=1;device_id=N1;ts=1730000000;chd1=23,5``). Sem cabeçalho, só se
   ``mapeamento.colunas_sem_cabecalho`` estiver configurado.

Regras de cada coluna
---------------------
- ``pid``: inteiro (aceita ``"17"`` e ``17.0``), dentro da faixa do ``bigint``.
- ``device_id``: texto de até 100 caracteres. Se faltar no payload (e ``device_do_topico`` for verdadeiro), usa o
  ``{id}`` do padrão NOVUS (``NOVUS/{id}/events``, ``.../ack/command``...); fora desse padrão, o nível do tópico após
  o prefixo assinado (``NOVUS/Teste_Alumbra/#`` + ``NOVUS/Teste_Alumbra/N1/dados`` → ``N1``).
- ``ts_unix``: epoch em **segundos** (UTC). Aceita s, ms, µs e ns (pela magnitude), texto de dígitos, ISO 8601
  (com fuso, ou sem fuso interpretado conforme ``fuso_sem_offset``) e ``dd/mm/aaaa hh:mm[:ss]``. Só aceita
  valores entre 2000-01-01 e o ano 3000.
- ``chdN_value``: ``Decimal`` arredondado a 4 casas (``decimal(18,4)``). Aceita número, ``"23,5"`` e
  ``"1.234,5"``. Se a chave achada contiver um objeto, usa ``value``/``valor``/``v`` dele. Valor fora da faixa
  (|x| ≥ 10^14) ou não numérico vai como ``NULL`` com aviso. Ausente vai como ``NULL`` sem aviso.
- ``payload_json``: o texto original da mensagem, igual para todas as linhas da mesma mensagem. Payload binário
  vira ``"base64:..."``.
- ``recebido_em``: hora em que o app recebeu a mensagem, em **UTC** sem fuso, com milissegundos.

Classificação das mensagens (NOVUS DigiRail; ``classificar``)
------------------------------------------------------------
Antes de mapear, cada objeto JSON é classificado (ver docs/digirail_iot_mqtt.md, seção 3.8):

- ``"leitura"``: tem ``channels``, ou é um formato genérico (nenhuma das regras abaixo). Segue o mapeamento.
- ``"evento"``: tem ``events`` (borda de um canal digital no modo Evento). Não é leitura, não grava, não é erro.
- ``"resposta"``: tem ``reported`` ou ``desired``, ou chegou num tópico ``.../ack/command`` ou ``.../ack/config``
  (mesmo com JSON ilegível). Não é leitura nem erro; ``diag`` e ``reset_counters`` são tratados no pipeline.

Para eventos e respostas, ``ResultadoMapa`` traz ``tipo``, ``objeto`` (o JSON) e ``device_id`` (do payload ou do
``{id}`` do tópico), sem linhas.

``pid`` no DigiRail é o código do modelo do produto (constante), não número de pacote: não serve como chave.

Nenhum valor é inventado. Se faltar ``pid``, ``device_id`` ou ``ts_unix`` (ou se forem inválidos), a linha sai com
``motivo`` preenchido e vai para a contingência, não para o banco. O mesmo vale para JSON quebrado, binário e
texto sem cabeçalho reconhecível: a mensagem é guardada, não descartada. Só o payload vazio é ignorado.
"""
from __future__ import annotations

import base64
import csv
import math
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, ClassVar

from app.config import MapeamentoConfig
from app.core.parser import (
    _JSONDecodeError,
    _escolher_delimitador,
    _json_loads,
    _parece_binario,
    dispositivo_do_topico,
    dispositivo_novus,
    hex_abreviado,
    trecho,
)

LEITURA, EVENTO, RESPOSTA = "leitura", "evento", "resposta"

MAX_DEVICE_ID = 100
BIGINT_MIN, BIGINT_MAX = -(2**63), 2**63 - 1
DECIMAL_LIMITE = Decimal(10) ** 14          # decimal(18,4): 14 dígitos inteiros
QUATRO_CASAS = Decimal("0.0001")
TS_MIN, TS_MAX = 946684800, 32503680000      # 2000-01-01 .. ano 3000 (UTC)

_RE_NUMERO = re.compile(r"^[+-]?(\d+([.,]\d*)?|[.,]\d+)([eE][+-]?\d+)?$")
_RE_NUMERO_MILHAR_BR = re.compile(r"^[+-]?\d{1,3}(\.\d{3})+(,\d+)?$")   # 1.234,5
_RE_DIGITOS = re.compile(r"^[+-]?\d+(\.\d+)?$")
_RE_PAR = re.compile(r"^\s*([A-Za-z_][\w .\-/]*?)\s*[=:]\s*(.*?)\s*$")
_FORMATOS_BR = ("%d/%m/%Y %H:%M:%S.%f", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y %H:%M", "%Y/%m/%d %H:%M:%S")
_CHAVES_VALOR_INTERNO = ("value", "valor", "v", "val")


@dataclass
class LinhaIot:
    """Uma linha da ``dbo.IOT001``. ``motivo`` preenchido = incompleta (vai para a contingência)."""

    pid: int | None
    device_id: str | None
    ts_unix: int | None
    chd1_value: Decimal | None
    chd2_value: Decimal | None
    chd3_value: Decimal | None
    payload_json: str | None
    recebido_em: datetime
    topico: str = ""
    motivo: str | None = None

    COLUNAS: ClassVar[tuple[str, ...]] = ("pid", "device_id", "ts_unix", "chd1_value", "chd2_value", "chd3_value",
                                          "payload_json", "recebido_em")

    @property
    def completa(self) -> bool:
        return self.motivo is None

    def como_tupla(self) -> tuple:
        return (self.pid, self.device_id, self.ts_unix, self.chd1_value, self.chd2_value, self.chd3_value,
                self.payload_json, self.recebido_em)

    def como_json(self) -> dict:
        """Para a contingência (JSON Lines): decimais como texto e ``recebido_em`` ISO com ``Z`` (UTC)."""
        def dec(v):
            return None if v is None else str(v)
        return {
            "pid": self.pid, "device_id": self.device_id, "ts_unix": self.ts_unix,
            "chd1_value": dec(self.chd1_value), "chd2_value": dec(self.chd2_value),
            "chd3_value": dec(self.chd3_value), "payload_json": self.payload_json,
            "recebido_em": self.recebido_em.isoformat(timespec="milliseconds") + "Z",
            "topico": self.topico, "motivo": self.motivo,
        }

    def descricao(self) -> str:
        return f"pid={self.pid} device_id={self.device_id} ts_unix={self.ts_unix}"


@dataclass
class ResultadoMapa:
    formato: str                      # "JSON", "JSON lista", "tabular", "texto", "binário", "vazio"
    linhas: list[LinhaIot] = field(default_factory=list)   # completas e incompletas, na ordem
    avisos: list[str] = field(default_factory=list)
    erro: str | None = None           # só para mensagem ignorada (payload vazio)
    tipo: str = LEITURA               # "leitura" | "evento" | "resposta" (ver classificar)
    objeto: Any = None                # JSON decodificado (dict/list) quando o payload é JSON válido
    device_id: str | None = None      # eventos e respostas: device_id do payload ou {id} do tópico

    @property
    def ok(self) -> bool:
        return self.erro is None

    @property
    def completas(self) -> list[LinhaIot]:
        return [l for l in self.linhas if l.completa]

    @property
    def rejeitadas(self) -> list[LinhaIot]:
        return [l for l in self.linhas if not l.completa]


# ------------------------------------------------------------------------------------------- conversões


def _norm(chave: Any) -> str:
    return re.sub(r"[\s\-]", "_", str(chave).strip().lower())


def _lista(valor: Any) -> list[str]:
    if valor is None:
        return []
    if isinstance(valor, str):
        return [valor]
    return [str(v) for v in valor]


def _resolver(d: dict, caminho: str) -> tuple[bool, Any]:
    """Procura ``caminho`` em ``d``: primeiro a chave literal, depois ``a.b.c`` aninhado. Devolve (achou, valor)."""
    normal = {_norm(k): k for k in d}
    alvo = _norm(caminho)
    if alvo in normal:
        return True, d[normal[alvo]]
    if "." not in caminho:
        return False, None
    atual: Any = d
    for parte in caminho.split("."):
        if not isinstance(atual, dict):
            return False, None
        normal = {_norm(k): k for k in atual}
        if _norm(parte) not in normal:
            return False, None
        atual = atual[normal[_norm(parte)]]
    return True, atual


def _vazio(valor: Any) -> bool:
    return valor is None or (isinstance(valor, str) and not valor.strip())


def _buscar_topo(fonte: dict, caminhos: list[str]) -> tuple[str | None, Any]:
    """No nível do próprio objeto: o primeiro caminho (na ordem da configuração) com valor não vazio."""
    for c in caminhos:
        achou, valor = _resolver(fonte, c)
        if achou and not _vazio(valor):
            return c, valor
    return None, None


def _buscar_aninhado(fonte: dict, caminhos: list[str]) -> list[tuple[str, Any]]:
    """Dentro dos objetos aninhados de ``fonte`` (qualquer profundidade, sem entrar em listas).

    Um caminho casa pelo final: ``timestamp`` casa com ``channels.timestamp``; ``chd1.value`` casa com
    ``channels.chd1.value``. Devolve ``[(caminho_completo, valor), ...]`` na ordem do JSON (busca em
    profundidade), no máximo um por chave (o primeiro caminho da configuração que casar com ela).
    """
    alvos = [(c, _norm(c), _norm(c.split(".", 1)[0]) if "." in c else None) for c in caminhos]
    achados: list[tuple[str, Any]] = []

    def visitar(d: dict, prefixo: str) -> None:
        for k, v in d.items():
            nk = _norm(k)
            casou = False
            for c, literal, primeiro in alvos:
                if nk == literal:
                    achou, valor = True, v
                elif primeiro is not None and nk == primeiro:
                    achou, valor = _resolver({k: v}, c)
                else:
                    continue
                if achou and not _vazio(valor):
                    achados.append((f"{prefixo}{k}" if nk == literal else f"{prefixo}{c}", valor))
                    casou = True
                    break
            if not casou and isinstance(v, dict):
                visitar(v, f"{prefixo}{k}.")

    for k, v in fonte.items():
        if isinstance(v, dict):
            visitar(v, f"{k}.")
    return achados


_avisos_ambiguos: set[tuple[str, tuple[str, ...]]] = set()
_avisos_lock = threading.Lock()


def _aviso_ambiguo_novo(coluna: str, caminhos: tuple[str, ...]) -> bool:
    """Verdadeiro só na primeira vez (no processo) para a mesma coluna e o mesmo conjunto de chaves."""
    with _avisos_lock:
        if (coluna, caminhos) in _avisos_ambiguos:
            return False
        _avisos_ambiguos.add((coluna, caminhos))
        return True


def _esquecer_avisos() -> None:
    """Para testes: volta a emitir os avisos de chave ambígua."""
    with _avisos_lock:
        _avisos_ambiguos.clear()


def _buscar(fontes: list[dict], caminhos: list[str], coluna: str = "",
            avisos: list[str] | None = None) -> tuple[str | None, Any]:
    """Procura o valor de uma coluna. Devolve (chave achada, valor) ou (None, None).

    Ordem: para cada fonte (item antes do envelope), primeiro o nível do próprio objeto (na ordem dos caminhos
    configurados), depois os objetos aninhados (na ordem do JSON). Se mais de uma chave aninhada diferente casar,
    vale a primeira e um aviso é registrado (uma única vez por coluna e conjunto de chaves).
    """
    for fonte in fontes:
        chave, valor = _buscar_topo(fonte, caminhos)
        if chave is not None:
            return chave, valor
        achados = _buscar_aninhado(fonte, caminhos)
        if achados:
            if len(achados) > 1 and avisos is not None:
                nomes = tuple(c for c, _ in achados)
                if _aviso_ambiguo_novo(coluna, nomes):
                    avisos.append(f"{coluna}: várias chaves aninhadas casam ({', '.join(nomes)}); usando "
                                  f"'{nomes[0]}' (primeira no JSON). Este aviso não se repete.")
            return achados[0]
    return None, None


def _texto_numero(t: str) -> Decimal | None:
    t = t.strip()
    if _RE_NUMERO_MILHAR_BR.match(t):
        t = t.replace(".", "").replace(",", ".")
    elif _RE_NUMERO.match(t):
        t = t.replace(",", ".")
    else:
        return None
    try:
        d = Decimal(t)
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def _para_decimal(valor: Any) -> Decimal | None:
    if isinstance(valor, bool) or valor is None:
        return None
    if isinstance(valor, int):
        return Decimal(valor)
    if isinstance(valor, float):
        return Decimal(repr(valor)) if math.isfinite(valor) else None
    if isinstance(valor, Decimal):
        return valor if valor.is_finite() else None
    if isinstance(valor, str):
        return _texto_numero(valor)
    return None


def converter_pid(valor: Any) -> int | None:
    d = _para_decimal(valor)
    if d is None or d != d.to_integral_value():
        return None
    n = int(d)
    return n if BIGINT_MIN <= n <= BIGINT_MAX else None


def converter_chd(valor: Any) -> tuple[Decimal | None, str | None]:
    """(valor decimal(18,4) ou None, aviso ou None)."""
    if isinstance(valor, dict):
        for k in _CHAVES_VALOR_INTERNO:
            achou, v = _resolver(valor, k)
            if achou:
                valor = v
                break
        else:
            return None, "objeto sem 'value'"
    if valor is None:
        return None, None
    d = _para_decimal(valor)
    if d is None:
        return None, f"valor não numérico {trecho(str(valor), 30)!r}"
    if abs(d) >= DECIMAL_LIMITE:
        return None, f"valor {d} fora da faixa de decimal(18,4)"
    q = d.quantize(QUATRO_CASAS, rounding=ROUND_HALF_UP)
    if abs(q) >= DECIMAL_LIMITE:
        return None, f"valor {d} fora da faixa de decimal(18,4)"
    return q, None


def converter_ts_unix(valor: Any, fuso_sem_offset: str = "local") -> int | None:
    """Epoch em segundos (UTC) a partir de s/ms/µs/ns, texto de dígitos, ISO 8601 ou dd/mm/aaaa hh:mm[:ss]."""
    if isinstance(valor, bool) or valor is None:
        return None
    n: Decimal | None = None
    if isinstance(valor, (int, float, Decimal)):
        n = _para_decimal(valor)
    elif isinstance(valor, str):
        t = valor.strip()
        if _RE_DIGITOS.match(t):
            n = Decimal(t)
        else:
            dt = _texto_data_hora(t)
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc) if fuso_sem_offset.lower() == "utc" else dt.astimezone()
            n = Decimal(math.floor(dt.timestamp()))
    if n is None:
        return None
    a = abs(n)
    if a >= Decimal("1e17"):
        n /= Decimal(10**9)
    elif a >= Decimal("1e14"):
        n /= Decimal(10**6)
    elif a >= Decimal("1e11"):
        n /= Decimal(10**3)
    s = int(n.to_integral_value(rounding=ROUND_FLOOR))
    return s if TS_MIN <= s < TS_MAX else None


def _texto_data_hora(t: str) -> datetime | None:
    iso = t[:-1] + "+00:00" if t.endswith(("Z", "z")) else t
    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        pass
    for fmt in _FORMATOS_BR:
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            continue
    return None


def _utc_sem_fuso(dt: datetime | None) -> datetime:
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        dt = dt.astimezone()  # ingênuo = hora local do PC
    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


# ------------------------------------------------------------------------------------------------ montagem


class _Montador:
    def __init__(self, cfg: MapeamentoConfig, topico: str, payload_json: str, recebido_em: datetime,
                 device_topico: str | None) -> None:
        self.cfg = cfg
        self.topico = topico
        self.payload_json = payload_json
        self.recebido_em = recebido_em
        self.device_topico = device_topico if cfg.device_do_topico else None
        self.linhas: list[LinhaIot] = []
        self.avisos: list[str] = []

    def rejeitar(self, motivo: str) -> None:
        self.linhas.append(LinhaIot(None, None, None, None, None, None, self.payload_json, self.recebido_em,
                                    self.topico, motivo))

    def leitura(self, fontes: list[dict]) -> None:
        cfg = self.cfg
        motivos: list[str] = []

        chave, bruto = _buscar(fontes, _lista(cfg.pid), "pid", self.avisos)
        pid = None
        if chave is None:
            motivos.append("sem pid")
        else:
            pid = converter_pid(bruto)
            if pid is None:
                motivos.append(f"pid inválido em '{chave}': {trecho(str(bruto), 30)!r}")

        chave, bruto = _buscar(fontes, _lista(cfg.device_id), "device_id", self.avisos)
        device = None
        if chave is None:
            device = self.device_topico or None
            if device is None:
                motivos.append("sem device_id (nem no payload nem no tópico)")
        elif isinstance(bruto, (dict, list, bool)):
            motivos.append(f"device_id inválido em '{chave}'")
        else:
            if isinstance(bruto, float) and bruto.is_integer():
                bruto = int(bruto)
            device = str(bruto).strip()
        if device is not None and len(device) > MAX_DEVICE_ID:
            motivos.append(f"device_id com mais de {MAX_DEVICE_ID} caracteres")
            device = None

        chave, bruto = _buscar(fontes, _lista(cfg.ts_unix), "ts_unix", self.avisos)
        ts = None
        if chave is None:
            motivos.append("sem ts_unix")
        else:
            ts = converter_ts_unix(bruto, cfg.fuso_sem_offset)
            if ts is None:
                motivos.append(f"ts inválido em '{chave}': {trecho(str(bruto), 40)!r}")

        canais: list[Decimal | None] = []
        for n, caminhos in enumerate((cfg.chd1, cfg.chd2, cfg.chd3), start=1):
            chave, bruto = _buscar(fontes, _lista(caminhos), f"chd{n}_value", self.avisos)
            valor, aviso = (None, None) if chave is None else converter_chd(bruto)
            if aviso:
                self.avisos.append(f"chd{n}_value em '{chave}' (pid={pid}): {aviso}; gravado como NULL.")
            canais.append(valor)

        self.linhas.append(LinhaIot(pid, device, ts, canais[0], canais[1], canais[2], self.payload_json,
                                    self.recebido_em, self.topico, "; ".join(motivos) or None))

    def objeto(self, obj: dict) -> None:
        """Objeto JSON: uma leitura, ou um envelope com lista de leituras."""
        for c in _lista(self.cfg.leituras):
            achou, lista = _resolver(obj, c)
            if achou and isinstance(lista, list) and lista and all(isinstance(i, dict) for i in lista):
                envelope = {k: v for k, v in obj.items() if _norm(k) != _norm(c)}
                for item in lista:
                    self.leitura([item, envelope])
                return
        self.leitura([obj])


def _celulas_par(row: list[str]) -> dict | None:
    pares = {}
    for c in row:
        if not c:
            continue
        m = _RE_PAR.match(c)
        if not m:
            return None
        pares[m.group(1)] = m.group(2)
    return pares or None


def _parece_cabecalho(row: list[str], cfg: MapeamentoConfig) -> bool:
    conhecidas = {_norm(c) for campo in (cfg.pid, cfg.device_id, cfg.ts_unix, cfg.chd1, cfg.chd2, cfg.chd3)
                  for c in _lista(campo)}
    return any(_norm(c) in conhecidas for c in row) and not any(_RE_DIGITOS.match(c.strip()) for c in row if c)


def _tabular(m: _Montador, texto: str) -> str:
    linhas = [l for l in texto.splitlines() if l.strip()]
    delim = _escolher_delimitador(linhas)
    tabela = [[c.strip() for c in row] for row in csv.reader(linhas, delimiter=delim)] if delim else \
        [[l.strip()] for l in linhas]
    tabela = [r for r in tabela if any(r)]

    pares = [_celulas_par(r) for r in tabela]
    if all(p is not None for p in pares):
        for p in pares:
            m.leitura([p])
        return "texto" if len(tabela) == 1 and delim is None else "tabular"

    if len(tabela) > 1 and _parece_cabecalho(tabela[0], m.cfg):
        nomes, dados = tabela[0], tabela[1:]
    elif m.cfg.colunas_sem_cabecalho:
        nomes, dados = _lista(m.cfg.colunas_sem_cabecalho), tabela
    else:
        m.rejeitar("texto sem cabeçalho reconhecido (pid/device_id/ts...); configure "
                   "mapeamento.colunas_sem_cabecalho se o dispositivo manda só os valores")
        return "texto" if len(tabela) == 1 else "tabular"
    for row in dados:
        m.leitura([{nomes[i] if i < len(nomes) else f"col{i + 1}": c for i, c in enumerate(row)}])
    return "tabular"


def topico_de_resposta(topico: str) -> bool:
    """``.../ack/command`` ou ``.../ack/config`` (Command Ack / Config Ack do DigiRail)."""
    niveis = (topico or "").split("/")
    return len(niveis) >= 2 and niveis[-2] == "ack" and niveis[-1] in ("command", "config")


def classificar(topico: str, obj: Any, resposta: bool = False) -> str:
    """Tipo da mensagem: ``"leitura"``, ``"evento"`` ou ``"resposta"`` (ponto único da regra; ver docstring).

    ``obj`` é o JSON decodificado (ou None se não for JSON válido). Formatos desconhecidos são ``"leitura"``
    (seguem o mapeamento, e o que hoje é erro de payload continua erro). ``resposta=True``: o tópico é a
    confirmação (ack) de algum dispositivo salvo em ``dispositivos.json``, mesmo fora do padrão ``.../ack/...``.
    """
    if resposta or topico_de_resposta(topico):
        return RESPOSTA
    if not isinstance(obj, dict):
        return LEITURA
    chaves = {_norm(k) for k in obj}
    if "channels" in chaves:
        return LEITURA
    if "events" in chaves:
        return EVENTO
    if "reported" in chaves or "desired" in chaves:
        return RESPOSTA
    return LEITURA


def _device_da_mensagem(obj: Any, topico: str, assinatura: str | None, device_topico: str | None = None
                        ) -> str | None:
    """device_id do payload; senão ``device_topico`` (tópico salvo de um device); senão o deduzido do tópico."""
    if isinstance(obj, dict):
        achou, valor = _resolver(obj, "device_id")
        if achou and isinstance(valor, (str, int)) and not isinstance(valor, bool) and str(valor).strip():
            return str(valor).strip()[:MAX_DEVICE_ID]
    return device_topico or dispositivo_novus(topico) or dispositivo_do_topico(topico, assinatura)


def mapear(topico: str, payload: bytes | bytearray | str, recebido_em: datetime | None = None,
           assinatura: str | None = None, cfg: MapeamentoConfig | None = None,
           device_topico: str | None = None, resposta: bool = False) -> ResultadoMapa:
    """Mensagem → linhas da IOT001. Nunca levanta exceção.

    ``device_topico``: dispositivo dono do tópico (tópicos salvos em ``dispositivos.json``), usado quando o
    payload não traz ``device_id``. ``resposta``: o tópico é a confirmação (ack) de um dispositivo salvo.
    """
    cfg = cfg or MapeamentoConfig()
    dados = bytes(payload) if isinstance(payload, (bytes, bytearray)) else str(payload).encode("utf-8")
    recebido = _utc_sem_fuso(recebido_em)
    dono = device_topico
    device_topico = dono or dispositivo_do_topico(topico, assinatura)
    eh_resposta = resposta or topico_de_resposta(topico)

    if not dados.strip():
        if eh_resposta:  # ex.: limpeza de mensagem retida no tópico de resposta
            return ResultadoMapa("vazio", tipo=RESPOSTA,
                                 device_id=_device_da_mensagem(None, topico, assinatura, dono))
        return ResultadoMapa("vazio", erro="payload vazio")

    try:
        texto = dados.decode("utf-8-sig")
        binario = _parece_binario(texto)
    except UnicodeDecodeError:
        texto, binario = "", True

    if binario and eh_resposta:
        return ResultadoMapa("binário", tipo=RESPOSTA,
                             device_id=_device_da_mensagem(None, topico, assinatura, dono))
    if binario:
        m = _Montador(cfg, topico, "base64:" + base64.b64encode(dados).decode("ascii"), recebido, device_topico)
        m.rejeitar(f"payload binário (não é texto UTF-8): {hex_abreviado(dados)}")
        return ResultadoMapa("binário", m.linhas, m.avisos)

    m = _Montador(cfg, topico, texto, recebido, device_topico)
    try:
        t = texto.strip()
        if t[0] in "{[":
            try:
                obj = _json_loads(t)
            except _JSONDecodeError as exc:
                if eh_resposta:  # resposta ilegível (o manual tem exemplos com vírgula sobrando)
                    return ResultadoMapa("JSON", tipo=RESPOSTA, avisos=[f"resposta com JSON inválido ({exc})"],
                                         device_id=_device_da_mensagem(None, topico, assinatura, dono))
                m.rejeitar(f"JSON inválido ({exc})")
                return ResultadoMapa("JSON", m.linhas, m.avisos)
            tipo = classificar(topico, obj, eh_resposta)
            if tipo != LEITURA:
                return ResultadoMapa("JSON", tipo=tipo, objeto=obj,
                                     device_id=_device_da_mensagem(obj, topico, assinatura, dono))
            if isinstance(obj, dict):
                m.objeto(obj)
                return ResultadoMapa("JSON", m.linhas, m.avisos, objeto=obj)
            if not obj:
                m.rejeitar("lista JSON vazia")
            for i, item in enumerate(obj):
                if isinstance(item, dict):
                    m.objeto(item)
                else:
                    m.rejeitar(f"item {i} da lista JSON não é um objeto: {trecho(str(item), 30)!r}")
            return ResultadoMapa("JSON lista", m.linhas, m.avisos, objeto=obj)
        if eh_resposta:
            return ResultadoMapa("texto", tipo=RESPOSTA,
                                 device_id=_device_da_mensagem(None, topico, assinatura, dono))
        formato = _tabular(m, t)
        return ResultadoMapa(formato, m.linhas, m.avisos)
    except Exception as exc:  # noqa: BLE001 - payload estranho nunca derruba a thread
        m.linhas = []
        m.rejeitar(f"falha ao interpretar ({type(exc).__name__}: {exc})")
        return ResultadoMapa("desconhecido", m.linhas, m.avisos)
