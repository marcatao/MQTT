"""Auto-detecção e normalização dos payloads MQTT (JSON, tabular, texto, binário); usado por ``mapeamento``.

Cada mensagem vira uma lista de ``Registro`` (uma linha por campo medido). Ordem de detecção:

1. **Vazio** → descartado com aviso.
2. **Binário** (não decodifica como UTF-8, ou tem bytes de controle como NUL) → um registro
   ``Campo="payload_base64"`` com o conteúdo em base64 no ``ValorTexto``, para não perder dado; o log mostra um
   hex abreviado. Se o base64 passar de 4000 caracteres (limite da coluna, ~3 KB de payload), a mensagem é
   descartada com erro no log (o hex abreviado fica no log).
3. **JSON** (texto começa com ``{`` ou ``[``): objeto ou lista. JSON aninhado vira ``"a.b.c"``; itens de lista
   viram ``"a.0"``, ``"a.1"``; listas de objetos do tipo ``{"nome": "T1", "valor": 23.5}`` viram ``"a.T1"``.
   Se começar com ``{``/``[`` mas não for JSON válido, é **payload quebrado** (erro, nada é gravado).
4. **Valor simples** (uma linha, sem delimitador, ou um número com vírgula decimal como ``23,5``) →
   ``Campo="valor"``.
5. **Tabular** (linhas delimitadas por ``;``, ``\\t``, ``|`` ou ``,``): cabeçalho detectado quando a 1ª linha não
   tem números e alguma linha seguinte tem; sem cabeçalho as colunas viram ``col1``, ``col2``... Linhas no
   formato ``chave=valor`` ou ``chave:valor`` (ex.: ``T1=23,5;T2=40``) viram campos com esse nome.

**Dispositivo** (nesta ordem):
  a) chave do JSON / coluna do cabeçalho: ``device``, ``dispositivo``, ``serial``, ``id`` e variações
     (ver ``CHAVES_DISPOSITIVO``), sem diferenciar maiúsculas;
  b) o nível do tópico que casa com o primeiro curinga da assinatura (``NOVUS/Teste_Alumbra/#`` e tópico
     ``NOVUS/Teste_Alumbra/logger01/dados`` → ``logger01``);
  c) senão ``None``.

**DataHoraLeitura**: chave/coluna ``timestamp``, ``ts``, ``datahora``, ``time``... (ver ``CHAVES_TEMPO``).
Aceita ISO 8601 (com ou sem fuso; com fuso é convertido para hora local), ``dd/mm/aaaa hh:mm[:ss]`` e epoch em
segundos, milissegundos ou microssegundos (número ou texto de dígitos). Fica em hora local sem fuso, igual a
``RecebidoEm``. Valor de tempo inválido gera aviso e é guardado como campo de texto.

As chaves usadas como dispositivo/tempo não viram campos (já estão em colunas próprias).
"""
from __future__ import annotations

import base64
import binascii
import csv
import io
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar

try:  # orjson é bem mais rápido; json é o plano B
    import orjson as _orjson

    def _json_loads(texto: str) -> Any:
        return _orjson.loads(texto)

    _JSONDecodeError: tuple[type[Exception], ...] = (_orjson.JSONDecodeError, ValueError)
except ImportError:  # pragma: no cover
    import json as _json

    def _json_loads(texto: str) -> Any:
        return _json.loads(texto)

    _JSONDecodeError = (ValueError,)


# Limites das colunas da tabela
MAX_TOPICO = 255
MAX_DISPOSITIVO = 128
MAX_CAMPO = 128
MAX_TEXTO = 4000

CHAVES_DISPOSITIVO = (
    "dispositivo", "device", "deviceid", "device_id", "devid", "serial", "serialnumber", "serial_number",
    "numero_serie", "numeroserie", "sn", "id", "equipamento", "logger", "tag_dispositivo",
)
CHAVES_TEMPO = (
    "datahora", "data_hora", "datahoraleitura", "timestamp", "ts", "time", "datetime", "date_time", "hora",
    "data", "date", "tempo", "horario",
)
_CHAVES_NOME = ("nome", "name", "campo", "canal", "channel", "tag", "chave", "key", "variavel")
_CHAVES_VALOR = ("valor", "value", "val", "v", "leitura", "medida")

_DELIMITADORES = (";", "\t", "|", ",")
_RE_NUMERO = re.compile(r"^[+-]?(\d+([.,]\d*)?|[.,]\d+)([eE][+-]?\d+)?$")
_RE_NUMERO_MILHAR_BR = re.compile(r"^[+-]?\d{1,3}(\.\d{3})+(,\d+)?$")  # 1.234,5
_RE_PAR = re.compile(r"^\s*([A-Za-z_][\w .\-/]*?)\s*[=:]\s*(.*?)\s*$")
_FORMATOS_BR = ("%d/%m/%Y %H:%M:%S.%f", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%Y/%m/%d %H:%M:%S")


@dataclass
class Registro:
    """Um campo medido extraído do payload (o mapeamento monta as linhas da IOT001)."""

    RecebidoEm: datetime
    Topico: str
    Dispositivo: str | None
    DataHoraLeitura: datetime | None
    Campo: str
    ValorNumerico: float | None
    ValorTexto: str | None

    COLUNAS: ClassVar[tuple[str, ...]] = ("RecebidoEm", "Topico", "Dispositivo", "DataHoraLeitura", "Campo", "ValorNumerico", "ValorTexto")

    def como_tupla(self) -> tuple:
        return (self.RecebidoEm, self.Topico, self.Dispositivo, self.DataHoraLeitura, self.Campo,
                self.ValorNumerico, self.ValorTexto)

    def como_json(self) -> dict:
        d = asdict(self)
        for k in ("RecebidoEm", "DataHoraLeitura"):
            if d[k] is not None:
                d[k] = d[k].isoformat(timespec="milliseconds")
        return d


@dataclass
class ResultadoParse:
    formato: str                      # "JSON", "JSON lista", "tabular", "texto", "binário", "vazio"
    registros: list[Registro] = field(default_factory=list)
    avisos: list[str] = field(default_factory=list)
    erro: str | None = None           # preenchido quando a mensagem é descartada por ser inválida

    @property
    def ok(self) -> bool:
        return self.erro is None


# ------------------------------------------------------------------------------------------- utilidades


def trecho(payload: bytes | str, limite: int = 80) -> str:
    """Trecho legível do payload para mensagens de log."""
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    payload = payload.replace("\r", "\\r").replace("\n", "\\n")
    return payload if len(payload) <= limite else payload[:limite] + "…"


def hex_abreviado(dados: bytes, limite: int = 24) -> str:
    h = binascii.hexlify(dados[:limite], " ").decode()
    return h + (f" … ({len(dados)} bytes)" if len(dados) > limite else f" ({len(dados)} bytes)")


TIPOS_TOPICO_NOVUS = ("events", "config", "command", "ack")


def dispositivo_novus(topico: str) -> str | None:
    """``{id}`` no padrão NOVUS ``.../{id}/events|config|command|ack/config|ack/command``; senão None.

    O ``{id}`` é o nível imediatamente antes do primeiro nível de tipo (``events``, ``config``, ``command`` ou
    ``ack`` seguido de ``config``/``command``). Ex.: ``NOVUS/Teste_Alumbra/ack/command`` → ``Teste_Alumbra``.
    """
    niveis = (topico or "").split("/")
    for i in range(1, len(niveis)):
        nivel = niveis[i]
        if nivel not in TIPOS_TOPICO_NOVUS:
            continue
        if nivel == "ack" and (i + 1 >= len(niveis) or niveis[i + 1] not in ("config", "command")):
            continue
        if i + (2 if nivel == "ack" else 1) != len(niveis):
            continue  # o tipo precisa ser o fim do tópico
        return niveis[i - 1] or None
    return None


def dispositivo_do_topico(topico: str, assinatura: str | None) -> str | None:
    """Dispositivo deduzido do tópico.

    1. Padrão NOVUS (``NOVUS/{id}/events``, ``.../ack/command``...): o ``{id}`` (ver ``dispositivo_novus``).
    2. Senão, o nível do tópico que casa com o primeiro curinga (``+`` ou ``#``) da assinatura.
    """
    if not assinatura:
        return None
    novus = dispositivo_novus(topico)
    if novus is not None:
        return novus
    niveis_ass = assinatura.split("/")
    niveis_top = topico.split("/")
    for i, nivel in enumerate(niveis_ass):
        if nivel in ("+", "#"):
            if i < len(niveis_top) and niveis_top[i]:
                return niveis_top[i]
            return None
        if i >= len(niveis_top) or niveis_top[i] != nivel:
            return None
    return None


def _normalizar_chave(chave: str) -> str:
    return re.sub(r"[\s\-]", "_", str(chave).strip().lower())


def converter_numero(texto: str) -> float | None:
    """``"23.5"``, ``"23,5"``, ``"1.234,5"``, ``"1e3"`` → float; senão ``None``."""
    t = texto.strip()
    if not t:
        return None
    if _RE_NUMERO_MILHAR_BR.match(t):
        t = t.replace(".", "").replace(",", ".")
    elif _RE_NUMERO.match(t):
        t = t.replace(",", ".")
    else:
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    return v if math.isfinite(v) else None


def _local_sem_fuso(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


def _de_epoch(n: float) -> datetime | None:
    a = abs(n)
    if a >= 1e17:      # nanossegundos
        n /= 1e9
    elif a >= 1e14:    # microssegundos
        n /= 1e6
    elif a >= 1e11:    # milissegundos
        n /= 1e3
    if not (0 < n < 32503680000):  # até o ano 3000
        return None
    try:
        return _local_sem_fuso(datetime.fromtimestamp(n, tz=timezone.utc))
    except (OverflowError, OSError, ValueError):
        return None


def converter_data_hora(valor: Any) -> datetime | None:
    """ISO 8601, ``dd/mm/aaaa hh:mm[:ss]`` ou epoch (s/ms/µs) → ``datetime`` local sem fuso."""
    if isinstance(valor, bool) or valor is None:
        return None
    if isinstance(valor, datetime):
        return _local_sem_fuso(valor)
    if isinstance(valor, (int, float)):
        return _de_epoch(float(valor))
    t = str(valor).strip()
    if not t:
        return None
    if re.fullmatch(r"\d{9,19}(\.\d+)?", t):
        return _de_epoch(float(t))
    iso = t[:-1] + "+00:00" if t.endswith(("Z", "z")) else t
    try:
        return _local_sem_fuso(datetime.fromisoformat(iso))
    except ValueError:
        pass
    for fmt in _FORMATOS_BR:
        try:
            return _local_sem_fuso(datetime.strptime(t, fmt))
        except ValueError:
            continue
    return None


def _valor_para_colunas(valor: Any) -> tuple[float | None, str | None]:
    """Converte um valor em (ValorNumerico, ValorTexto)."""
    if valor is None:
        return None, None
    if isinstance(valor, bool):
        return (1.0 if valor else 0.0), ("true" if valor else "false")
    if isinstance(valor, (int, float)):
        f = float(valor)
        if math.isfinite(f):
            return f, None
        return None, str(valor)
    texto = str(valor)
    n = converter_numero(texto)
    if n is not None:
        return n, None
    baixo = texto.strip().lower()
    if baixo in ("true", "verdadeiro", "on", "ligado"):
        return 1.0, texto.strip()
    if baixo in ("false", "falso", "off", "desligado"):
        return 0.0, texto.strip()
    return None, texto


def _achar_chave(d: dict, candidatas: tuple[str, ...]) -> str | None:
    normal = {_normalizar_chave(k): k for k in d}
    for c in candidatas:
        if c in normal:
            return normal[c]
    return None


# ---------------------------------------------------------------------------------------- montagem


class _Montador:
    def __init__(self, topico: str, recebido_em: datetime, dispositivo_topico: str | None) -> None:
        self.topico = topico[:MAX_TOPICO]
        self.recebido_em = _local_sem_fuso(recebido_em)
        self.dispositivo_topico = dispositivo_topico
        self.registros: list[Registro] = []
        self.avisos: list[str] = []
        self._truncou_texto = False
        self._truncou_campo = False

    def adicionar(self, campo: str, valor: Any, dispositivo: str | None, data_hora: datetime | None) -> None:
        campo = str(campo).strip() or "valor"
        if len(campo) > MAX_CAMPO:
            campo = campo[:MAX_CAMPO]
            if not self._truncou_campo:
                self.avisos.append(f"Nome de campo com mais de {MAX_CAMPO} caracteres foi cortado.")
                self._truncou_campo = True
        num, texto = _valor_para_colunas(valor)
        if texto is not None and len(texto) > MAX_TEXTO:
            texto = texto[:MAX_TEXTO]
            if not self._truncou_texto:
                self.avisos.append(f"Valor de texto em '{campo}' com mais de {MAX_TEXTO} caracteres foi cortado.")
                self._truncou_texto = True
        disp = dispositivo if dispositivo not in (None, "") else self.dispositivo_topico
        if disp is not None:
            disp = str(disp)[:MAX_DISPOSITIVO]
        self.registros.append(Registro(self.recebido_em, self.topico, disp, data_hora, campo, num, texto))

    # --- JSON ------------------------------------------------------------------------------------

    def objeto(self, obj: dict, prefixo: str = "", dispositivo: str | None = None,
               data_hora: datetime | None = None) -> None:
        """Objeto de nível de leitura: extrai dispositivo/tempo e achata o resto."""
        obj = dict(obj)
        chave_d = _achar_chave(obj, CHAVES_DISPOSITIVO)
        if chave_d is not None and not isinstance(obj[chave_d], (dict, list)) and obj[chave_d] is not None:
            dispositivo = str(obj.pop(chave_d))
        chave_t = _achar_chave(obj, CHAVES_TEMPO)
        if chave_t is not None and not isinstance(obj[chave_t], (dict, list)):
            dt = converter_data_hora(obj[chave_t])
            if dt is not None:
                data_hora = dt
                obj.pop(chave_t)
            else:
                self.avisos.append(f"Data/hora inválida em '{chave_t}': {trecho(str(obj[chave_t]), 40)}"
                                   " (guardada como texto).")
        self._achatar(obj, prefixo, dispositivo, data_hora)

    def _achatar(self, valor: Any, prefixo: str, dispositivo: str | None, data_hora: datetime | None) -> None:
        if isinstance(valor, dict):
            if not valor:
                return
            for k, v in valor.items():
                self._achatar(v, f"{prefixo}.{k}" if prefixo else str(k), dispositivo, data_hora)
        elif isinstance(valor, list):
            for i, item in enumerate(valor):
                if isinstance(item, dict):
                    chave_n = _achar_chave(item, _CHAVES_NOME)
                    chave_v = _achar_chave(item, _CHAVES_VALOR)
                    if chave_n and chave_v and not isinstance(item[chave_n], (dict, list)):
                        # {"nome": "T1", "valor": 23.5, ...} → campo "<prefixo>.T1"
                        resto = {k: v for k, v in item.items() if k not in (chave_n, chave_v)}
                        nome = str(item[chave_n])
                        campo = f"{prefixo}.{nome}" if prefixo else nome
                        d_item, t_item = self._meta(resto, dispositivo, data_hora)
                        self.adicionar(campo, item[chave_v], d_item, t_item)
                        continue
                    self.objeto(item, f"{prefixo}.{i}" if prefixo else str(i), dispositivo, data_hora)
                else:
                    self._achatar(item, f"{prefixo}.{i}" if prefixo else f"valor.{i}", dispositivo, data_hora)
        else:
            self.adicionar(prefixo or "valor", valor, dispositivo, data_hora)

    def _meta(self, item: dict, dispositivo: str | None, data_hora: datetime | None):
        chave_d = _achar_chave(item, CHAVES_DISPOSITIVO)
        if chave_d is not None and item[chave_d] is not None and not isinstance(item[chave_d], (dict, list)):
            dispositivo = str(item[chave_d])
        chave_t = _achar_chave(item, CHAVES_TEMPO)
        if chave_t is not None:
            dt = converter_data_hora(item[chave_t])
            if dt is not None:
                data_hora = dt
        return dispositivo, data_hora


# ------------------------------------------------------------------------------------------ tabular


def _escolher_delimitador(linhas: list[str]) -> str | None:
    """Delimitador presente em todas as linhas com a mesma contagem (prefere ``;``, tab, ``|``, ``,``)."""
    amostra = linhas[:50]
    for d in _DELIMITADORES:
        contagens = {len(next(csv.reader([l], delimiter=d))) for l in amostra}
        if len(contagens) == 1 and contagens.pop() > 1:
            return d
    # tolera linhas com contagens diferentes, desde que todas tenham o delimitador
    for d in _DELIMITADORES:
        if all(d in l for l in amostra):
            return d
    return None


def _eh_numero_ou_data(celula: str) -> bool:
    return converter_numero(celula) is not None or (
        any(c.isdigit() for c in celula) and converter_data_hora(celula) is not None)


def _tabular(m: _Montador, linhas: list[str], delim: str | None) -> None:
    if delim is None:
        tabela = [[l.strip()] for l in linhas]
    else:
        tabela = [[c.strip() for c in row] for row in csv.reader(linhas, delimiter=delim)]
    tabela = [r for r in tabela if any(r)]

    # chave=valor em todas as células
    if all(all(_RE_PAR.match(c) for c in row if c) for row in tabela):
        for row in tabela:
            pares = {}
            for c in row:
                if c:
                    mt = _RE_PAR.match(c)
                    pares[mt.group(1)] = mt.group(2)
            m.objeto(pares)
        return

    primeira = tabela[0]
    tem_cabecalho = (
        len(tabela) > 1
        and all(c and not _eh_numero_ou_data(c) for c in primeira)
        and any(_eh_numero_ou_data(c) for row in tabela[1:] for c in row)
    )
    if tem_cabecalho:
        nomes = _nomes_unicos(primeira)
        for row in tabela[1:]:
            m.objeto({nomes[i] if i < len(nomes) else f"col{i + 1}": c for i, c in enumerate(row)})
    else:
        for row in tabela:
            if len(row) == 1:
                m.adicionar("valor", row[0], None, None)
            else:
                for i, c in enumerate(row):
                    m.adicionar(f"col{i + 1}", c, None, None)


def _nomes_unicos(nomes: list[str]) -> list[str]:
    vistos: dict[str, int] = {}
    saida = []
    for i, n in enumerate(nomes):
        n = n or f"col{i + 1}"
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 1
        saida.append(n)
    return saida


# --------------------------------------------------------------------------------------- entrada


def _parece_binario(texto: str) -> bool:
    return any(ord(c) < 32 and c not in "\t\r\n" for c in texto)


def analisar(topico: str, payload: bytes | bytearray | str, recebido_em: datetime | None = None,
             assinatura: str | None = None) -> ResultadoParse:
    """Detecta o formato do payload e devolve os registros normalizados. Nunca levanta exceção."""
    recebido_em = recebido_em or datetime.now()
    m = _Montador(topico, recebido_em, dispositivo_do_topico(topico, assinatura))
    dados = bytes(payload) if isinstance(payload, (bytes, bytearray)) else str(payload).encode("utf-8")

    try:
        if not dados.strip():
            return ResultadoParse("vazio", erro="payload vazio")

        try:
            texto = dados.decode("utf-8-sig")
            binario = _parece_binario(texto)
        except UnicodeDecodeError:
            binario = True

        if binario:
            b64 = base64.b64encode(dados).decode("ascii")
            if len(b64) > MAX_TEXTO:
                return ResultadoParse("binário", erro=f"binário grande demais para guardar "
                                                      f"({len(dados)} bytes): {hex_abreviado(dados)}")
            m.adicionar("payload_base64", None, None, None)
            m.registros[-1].ValorTexto = b64
            m.avisos.append(f"Payload binário (não é texto UTF-8): {hex_abreviado(dados)}; guardado em base64.")
            return ResultadoParse("binário", m.registros, m.avisos)

        texto = texto.strip()
        if texto[0] in "{[":
            try:
                obj = _json_loads(texto)
            except _JSONDecodeError as exc:
                return ResultadoParse("JSON", erro=f"JSON inválido ({exc}): {trecho(texto)}")
            if isinstance(obj, dict):
                m.objeto(obj)
                formato = "JSON"
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    if isinstance(item, dict):
                        m.objeto(item)
                    else:
                        m._achatar(item, f"valor.{i}", None, None)
                formato = "JSON lista"
            else:  # pragma: no cover - "{"/"[" sempre dão dict/list
                m.adicionar("valor", obj, None, None)
                formato = "JSON"
            if not m.registros:
                m.avisos.append("JSON sem campos de dados.")
            return ResultadoParse(formato, m.registros, m.avisos)

        if texto[0] == '"' and texto[-1] == '"' and "\n" not in texto:  # string JSON solta
            try:
                obj = _json_loads(texto)
                if isinstance(obj, str):
                    m.adicionar("valor", obj, None, None)
                    return ResultadoParse("texto", m.registros, m.avisos)
            except _JSONDecodeError:
                pass

        linhas = [l for l in texto.splitlines() if l.strip()]
        if len(linhas) == 1 and (converter_numero(linhas[0]) is not None):
            m.adicionar("valor", linhas[0], None, None)
            return ResultadoParse("texto", m.registros, m.avisos)
        delim = _escolher_delimitador(linhas)
        if delim is None and len(linhas) == 1:
            mt = _RE_PAR.match(linhas[0])
            if mt and not linhas[0].lower().startswith(("http:", "https:")):
                m.objeto({mt.group(1): mt.group(2)})
            else:
                m.adicionar("valor", linhas[0], None, None)
            return ResultadoParse("texto", m.registros, m.avisos)
        _tabular(m, linhas, delim)
        return ResultadoParse("tabular", m.registros, m.avisos)
    except Exception as exc:  # noqa: BLE001 - payload estranho nunca derruba a thread
        return ResultadoParse("desconhecido", erro=f"falha ao interpretar ({type(exc).__name__}: {exc}): "
                                                   f"{trecho(dados)}")
