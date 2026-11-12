"""Apontamentos reais pelo IoT: ``MQTT.dbo.IOT001`` → ``OEE.dbo.apontamentos_estado`` (portal OEE).

Especificação: ``docs/apontamentos_reais.md`` (fonte da verdade). Resumo:

- Máquinas: ``ativo = 1``, ``d_e_l_e_t_ <> '*'``, ``micro_parada_s``/``macro_parada_s`` preenchidos e pelo menos um
  canal (``iotN_dispositivo_id`` + ``iotN_canal``) com dispositivo ativo e tipo ``producao`` (ou NULL).
- **Produção** = leitura em que o contador do canal subiu em relação à leitura anterior do mesmo canal (valor menor =
  contador zerado = nova base). Com vários canais, vale a produção de qualquer um.
- Intervalo entre produções ``gap``: ``< micro`` → 2 Produção Normal; ``micro ≤ gap < macro`` → 99 Microparada;
  ``≥ macro`` → 98 Macro parada. A parada começa na última produção; o trecho em aberto usa ``agora − p``.
- Trechos iguais seguidos = um apontamento; nenhum passa da meia-noite local (``[apontamentos] fuso``); encadeados
  (``fim = próximo início − 1 s``); trecho < 2 s é absorvido; o último fica em aberto (``fim`` NULL).
- Reconciliação por máquina, numa transação: os do integrador (``observacao`` começando por ``[IoT]``) são
  reaproveitados (mesmo ``inicio`` primeiro), os que sobram são excluídos e os que faltam inseridos. O IoT prevalece
  sobre os manuais (aparados ou excluídos, AVISO no log). Legenda trocada pelo operador numa parada é mantida.
- Depois: situação da máquina (``SituacaoMaquina`` do portal) e status das OPs afetadas (``StatusOrdemProducao``).

Banco: a conexão é a de ``[sqlserver]`` (``sql_conexao.conectar``); as tabelas do OEE vão pelo nome em 3 partes
``[banco_oee].[dbo].[tabela]`` e a IOT001 por ``[sql.banco].[schema].[tabela]``. Tudo com parâmetros; os nomes vêm
de constantes ou da configuração validada. Datas para o SQL: ``datetime`` ingênuo em UTC, sem fração.

Sem banco/tabela/permissão/coluna do OEE: 1 INFO (por servidor/banco, até voltar a funcionar) e nova tentativa no
próximo ciclo; não é erro. Falha de SQL numa máquina: rollback dela, 1 erro categoria ``"apontamentos"`` e segue para a
próxima. Falha de conexão: 1 erro por incidente (até um ciclo dar certo).

As funções puras (sem banco) ficam no começo do módulo e são testadas em ``tests/test_apontamentos_oee.py``.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Iterable

from app.config import ApontamentosConfig, OeeConfig, SqlServerConfig
from app.core import sql_conexao
from app.core.sink_sqlserver import classificar_erro

logger = logging.getLogger("app.core.apontamentos")

Logar = Callable[[str, str], None]
AoErro = Callable[[str, str | None], None]          # (mensagem, detalhe)
AoResultado = Callable[[str], None]                 # JSON do ciclo
Relogio = Callable[[], float]

PRODUZINDO, MICRO, MACRO = "produzindo", "micro", "macro"
MARCA = "[IoT]"
OBSERVACAO = "[IoT] Apontamento automático do integrador MQTT"
CANAIS = tuple(f"chd{n}_value" for n in range(1, 7))
SLOTS = (1, 2, 3)                   # iot1..iot3 na tabela maquinas
MIN_TRECHO = 2                      # s: trecho fechado menor que isso é absorvido (fim > inicio)
MARGEM_LEITURAS = 60                # s antes de (início − macro): base do contador para a 1ª leitura do trecho
MIN_MINUTOS = 0.5


def _sem_log(nivel: str, msg: str) -> None:
    pass


def _nada(*_args) -> None:
    pass


# ============================================================================================ funções puras


def fuso_de(nome: str) -> tzinfo:
    """``ZoneInfo(nome)``; sem a base de fusos (Windows sem ``tzdata``), UTC−3 fixo (Brasília, sem horário de verão)."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(nome)
    except Exception:  # noqa: BLE001 - ZoneInfoNotFoundError, ValueError
        logger.warning("Fuso %r indisponível; usando UTC-3 fixo", nome)
        return timezone(timedelta(hours=-3), "UTC-3")


def dia_local(t: int, fuso: tzinfo) -> date:
    return datetime.fromtimestamp(t, fuso).date()


def inicio_do_dia(t: float, fuso: tzinfo, deslocamento_dias: int = 0) -> int:
    """Epoch da meia-noite local do dia de ``t`` (+ ``deslocamento_dias``)."""
    d = dia_local(int(t), fuso) + timedelta(days=deslocamento_dias)
    return int(datetime(d.year, d.month, d.day, tzinfo=fuso).timestamp())


def fim_do_dia(t: int, fuso: tzinfo) -> int:
    """23:59:59 local do dia de ``t`` (epoch)."""
    return inicio_do_dia(t, fuso, 1) - 1


def producoes(leituras: Iterable[tuple[int, Any]]) -> list[int]:
    """Instantes (ts_unix) em que o contador de UM canal subiu.

    ``leituras``: (ts_unix, valor) em qualquer ordem; valor ``None`` é ignorado (não muda a base). A 1ª leitura é só
    a base. Valor menor que o anterior = contador zerado/reiniciado: vira a nova base, não é produção.
    """
    saida: list[int] = []
    anterior: float | None = None
    for ts, valor in sorted(((int(t), v) for t, v in leituras if v is not None), key=lambda x: x[0]):
        try:
            v = float(valor)
        except (TypeError, ValueError):
            continue
        if anterior is not None and v > anterior:
            saida.append(ts)
        anterior = v
    return saida


def juntar_producoes(*listas: Iterable[int]) -> list[int]:
    """União ordenada, sem repetição (vale a produção de qualquer canal)."""
    return sorted({int(t) for lista in listas for t in lista})


def classificar(gap: float, micro: int, macro: int) -> str:
    if gap < micro:
        return PRODUZINDO
    if gap < macro:
        return MICRO
    return MACRO


def segmentos_continuos(prods: Iterable[int], ini: int, agora: int, micro: int, macro: int
                        ) -> list[tuple[int, int, str]]:
    """Trechos ``(inicio, fim_exclusivo, estado)`` que cobrem ``[ini, agora]`` sem buracos, já mesclados.

    ``prods``: produções (ts_unix), podendo incluir as anteriores a ``ini`` (a última delas define o 1º intervalo).
    Sem produção antes de ``ini``, o período até a 1ª produção é macroparada. Produções depois de ``agora`` (relógio
    do aparelho adiantado) são ignoradas. O último trecho termina em ``agora`` (em aberto) e pode ter duração zero
    (produção exatamente em ``agora``).
    """
    ordenadas = sorted({int(p) for p in prods if int(p) <= agora})
    antes = [p for p in ordenadas if p < ini]
    dentro = [p for p in ordenadas if p >= ini]
    anterior: int | None = antes[-1] if antes else None
    cursor = ini
    brutos: list[list] = []
    for t in dentro:
        gap = (t - anterior) if anterior is not None else float("inf")
        if t > cursor:
            brutos.append([cursor, t, classificar(gap, micro, macro)])
        anterior, cursor = t, t
    gap = (agora - anterior) if anterior is not None else float("inf")
    brutos.append([cursor, agora, classificar(gap, micro, macro)])
    mesclados: list[list] = []
    for seg in brutos:
        if mesclados and mesclados[-1][2] == seg[2] and mesclados[-1][1] == seg[0]:
            mesclados[-1][1] = seg[1]
        else:
            mesclados.append(seg)
    return [(a, b, e) for a, b, e in mesclados]


@dataclass(frozen=True)
class Trecho:
    """Apontamento desejado: ``fim`` inclusivo (= próximo início − 1 s) ou ``None`` (em aberto)."""
    inicio: int
    fim: int | None
    estado: str


def dividir_por_dia(segmentos: list[tuple[int, int, str]], fuso: tzinfo) -> list[Trecho]:
    """Quebra os trechos na meia-noite local, absorve os fechados com menos de 2 s e mescla vizinhos iguais do mesmo
    dia. Encadeados: ``fim = próximo início − 1 s``; o último fica em aberto."""
    if not segmentos:
        return []
    pecas: list[list] = []          # [inicio, fim_exclusivo, estado]
    for a, b, estado in segmentos:
        inicio = a
        while True:
            meia_noite = inicio_do_dia(inicio, fuso, 1)
            if b > meia_noite:
                pecas.append([inicio, meia_noite, estado])
                inicio = meia_noite
            else:
                pecas.append([inicio, b, estado])
                break

    def mesmo_dia(t1: int, t2: int) -> bool:
        return dia_local(t1, fuso) == dia_local(t2, fuso)

    mudou = True
    while mudou:
        mudou = False
        # 1) mescla vizinhos com o mesmo estado no mesmo dia
        i = 0
        while i < len(pecas) - 1:
            a, b = pecas[i], pecas[i + 1]
            if a[2] == b[2] and a[1] == b[0] and mesmo_dia(a[0], b[0]):
                a[1] = b[1]
                del pecas[i + 1]
                mudou = True
            else:
                i += 1
        # 2) absorve um trecho fechado curto (o último, em aberto, não tem fim e fica como está)
        for i in range(len(pecas) - 1):
            p = pecas[i]
            if p[1] - p[0] >= MIN_TRECHO:
                continue
            if i > 0 and mesmo_dia(pecas[i - 1][0], p[1] - 1):
                pecas[i - 1][1] = p[1]
                del pecas[i]
                mudou = True
                break
            seguinte = pecas[i + 1]
            if mesmo_dia(p[0], seguinte[0]):
                seguinte[0] = p[0]
                del pecas[i]
                mudou = True
                break
    trechos: list[Trecho] = []
    for i, (a, b, estado) in enumerate(pecas):
        fim = None if i == len(pecas) - 1 else pecas[i + 1][0] - 1
        trechos.append(Trecho(a, fim, estado))
    return trechos


@dataclass(frozen=True)
class LegendasIot:
    """Ids das legendas do integrador (pelos códigos ``2``, ``99``, ``98``)."""
    produzindo: int
    micro: int
    macro: int

    def de(self, estado: str) -> int:
        return {PRODUZINDO: self.produzindo, MICRO: self.micro, MACRO: self.macro}[estado]

    @property
    def todas(self) -> set[int]:
        return {self.produzindo, self.micro, self.macro}


def do_integrador(apontamento: dict) -> bool:
    return str(apontamento.get("observacao") or "").startswith(MARCA)


@dataclass
class Plano:
    """Ações da reconciliação de uma máquina. Datas em epoch (s, UTC).

    - ``inserir``: ``{"legenda_id", "inicio", "fim", "ordem_producao_id"}``
    - ``atualizar``: ``{"id", "inicio", "fim", "legenda_id", "ordem_producao_id", "manual", "antes"}`` (``antes`` =
      o apontamento existente como veio)
    - ``excluir``: o apontamento existente como veio + ``"manual"``
    """
    inserir: list[dict] = field(default_factory=list)
    atualizar: list[dict] = field(default_factory=list)
    excluir: list[dict] = field(default_factory=list)

    @property
    def vazio(self) -> bool:
        return not (self.inserir or self.atualizar or self.excluir)

    def ops_afetadas(self) -> set[int]:
        ops = {a.get("ordem_producao_id") for a in self.inserir}
        ops |= {a.get("ordem_producao_id") for a in self.atualizar}
        ops |= {a.get("ordem_producao_id") for a in self.excluir}
        return {int(o) for o in ops if o is not None}


def planejar(desejados: list[Trecho], existentes: list[dict], ini: int, agora: int, legendas: LegendasIot,
             op_maquina: int | None, fuso: tzinfo) -> Plano:
    """Compara os trechos desejados de ``[ini, agora]`` com os apontamentos existentes que cruzam a janela.

    ``existentes``: dicts com ``id, legenda_id, inicio, fim, observacao, ordem_producao_id`` (datas em epoch).
    Regras (docs/apontamentos_reais.md, "Reconciliação"):

    - começou antes de ``ini`` e invade a janela (ou está em aberto): ``fim`` = ``min(ini − 1, 23:59:59 do dia dele)``;
      se não sobrar nada, é excluído (manual ou do integrador; o do integrador fica sem aviso);
    - manual que começa dentro da janela: inteiramente coberto pelo IoT (a janela é toda coberta) → excluído;
    - do integrador dentro da janela: o de mesmo ``inicio`` é reaproveitado; os que sobram são reaproveitados na ordem
      (``inicio``, ``fim`` e legenda novos, OP mantida) e o excedente é excluído; os que faltam são inseridos com a OP
      atual da máquina;
    - legenda trocada pelo operador (não é 2/99/98) num trecho que continua parada é mantida, inclusive na
      continuação do mesmo trecho depois da meia-noite; se o trecho virou produção, volta para 2.
    """
    plano = Plano()
    dentro_iot: list[dict] = []
    for e in sorted(existentes, key=lambda x: (x["inicio"], x["id"])):
        manual = not do_integrador(e)
        if e["inicio"] > agora:
            continue
        if e["inicio"] < ini:
            if e["fim"] is not None and e["fim"] < ini:
                continue
            novo_fim = min(ini - 1, fim_do_dia(e["inicio"], fuso))
            if novo_fim <= e["inicio"]:
                plano.excluir.append({**e, "manual": manual})
            elif novo_fim != e["fim"]:
                plano.atualizar.append({"id": e["id"], "inicio": e["inicio"], "fim": novo_fim,
                                        "legenda_id": e["legenda_id"],
                                        "ordem_producao_id": e.get("ordem_producao_id"), "manual": manual,
                                        "antes": e})
            continue
        if manual:
            plano.excluir.append({**e, "manual": True})
        else:
            dentro_iot.append(e)

    por_inicio: dict[int, dict] = {}
    sobras: list[dict] = []
    for e in dentro_iot:
        if e["inicio"] in por_inicio:
            sobras.append(e)
        else:
            por_inicio[e["inicio"]] = e

    # 1ª passada: casa pelo mesmo início e decide a legenda de cada trecho, em ordem
    casados: list[tuple[Trecho, dict | None, int]] = []
    anterior: tuple[Trecho, int] | None = None
    for d in desejados:
        existente = por_inicio.pop(d.inicio, None)
        parada = d.estado != PRODUZINDO
        legenda = legendas.de(d.estado)
        if parada and existente is not None and existente["legenda_id"] not in legendas.todas:
            legenda = existente["legenda_id"]
        elif (parada and anterior is not None and anterior[0].estado == d.estado
              and anterior[0].fim is not None and anterior[0].fim + 1 == d.inicio
              and anterior[1] not in legendas.todas):
            legenda = anterior[1]          # continuação (meia-noite) de uma parada com o motivo do operador
        casados.append((d, existente, legenda))
        anterior = (d, legenda)

    sobras = sorted(sobras + list(por_inicio.values()), key=lambda x: (x["inicio"], x["id"]))
    for d, existente, legenda in casados:
        if existente is None and sobras:
            existente = sobras.pop(0)
        if existente is None:
            plano.inserir.append({"legenda_id": legenda, "inicio": d.inicio, "fim": d.fim,
                                  "ordem_producao_id": op_maquina})
        elif (existente["inicio"], existente["fim"], existente["legenda_id"]) != (d.inicio, d.fim, legenda):
            plano.atualizar.append({"id": existente["id"], "inicio": d.inicio, "fim": d.fim, "legenda_id": legenda,
                                    "ordem_producao_id": existente.get("ordem_producao_id"), "manual": False,
                                    "antes": existente})
    for e in sobras:
        plano.excluir.append({**e, "manual": False})
    return plano


def situacao_pela_legenda(tipo: str | None) -> str | None:
    """``SituacaoMaquina::pelaLegenda`` do portal: tipo ``produzindo`` → produzindo; outro tipo → parada."""
    if tipo is None:
        return None
    return "produzindo" if tipo == "produzindo" else "parada"


def status_op(datrf: Any, c2_status: Any, quje: Any, tipo_em_aberto: str | None, tem_apontamento: bool) -> str:
    """``StatusOrdemProducao::regra`` do portal (``tipo_em_aberto`` None = nenhum apontamento em aberto)."""
    if str(datrf or "").strip() != "":
        return "fechada"
    if str(c2_status or "").strip() == "U" or (tipo_em_aberto is not None and tipo_em_aberto != "produzindo"):
        return "parada"
    try:
        q = float(quje or 0)
    except (TypeError, ValueError):
        q = 0.0
    if tipo_em_aberto is not None or q > 0 or tem_apontamento:
        return "andamento"
    return "aberta"


def utc_sql(t: float) -> datetime:
    """Epoch → ``datetime`` UTC ingênuo, sem fração (colunas ``datetime`` do portal)."""
    return datetime.fromtimestamp(int(t), timezone.utc).replace(tzinfo=None)


def epoch_de(valor: Any) -> int | None:
    """``datetime`` do banco (ingênuo = UTC) → epoch inteiro (fração descartada)."""
    if valor is None:
        return None
    if isinstance(valor, datetime):
        dt = valor if valor.tzinfo is not None else valor.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    return int(valor)


def iso_local(t: float | None) -> str | None:
    return None if t is None else datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def canal_valido(canal: Any) -> str | None:
    c = str(canal or "").strip().lower()
    return c if c in CANAIS else None


# ============================================================================================== SQL


TABELAS_OEE = {
    "maquinas": {"id", "codigo", "nome", "ativo", "d_e_l_e_t_", "micro_parada_s", "macro_parada_s", "situacao",
                 "situacao_desde", "situacao_legenda_id", "ordem_producao_id",
                 *(f"iot{n}_{c}" for n in SLOTS for c in ("dispositivo_id", "canal", "tipo"))},
    "dispositivos_iot": {"id", "codigo", "ativo"},
    "legendas": {"id", "codigo", "descricao", "tipo", "ativo"},
    "apontamentos_estado": {"id", "maquina_id", "legenda_id", "ordem_producao_id", "inicio", "fim", "observacao",
                            "criado_em", "atualizado_em"},
    "ordens_producao": {"id", "c2_datrf", "c2_status", "c2_quje", "status"},
}
# (tabela, permissão) exigidas no banco do OEE
PERMISSOES_OEE = (("maquinas", "SELECT"), ("maquinas", "UPDATE"), ("dispositivos_iot", "SELECT"),
                  ("legendas", "SELECT"), ("ordens_producao", "SELECT"), ("ordens_producao", "UPDATE"),
                  ("apontamentos_estado", "SELECT"), ("apontamentos_estado", "INSERT"),
                  ("apontamentos_estado", "UPDATE"), ("apontamentos_estado", "DELETE"))


def sql_permissoes_oee(banco: str) -> str:
    """Consulta das permissões no contexto do banco do OEE (nomes constantes; banco validado e citado)."""
    itens = ", ".join(f"HAS_PERMS_BY_NAME(N''dbo.{t}'', N''OBJECT'', N''{p}'')" for t, p in PERMISSOES_OEE)
    return f"EXEC [{sql_conexao.validar_banco(banco)}].sys.sp_executesql N'SELECT {itens}'"


SQL_PERM_IOT = ("EXEC {banco}.sys.sp_executesql N'SELECT HAS_PERMS_BY_NAME(@obj, N''OBJECT'', N''SELECT'')', "
                "N'@obj nvarchar(400)', @obj = ?")


@dataclass
class Esquema:
    """Resultado da detecção: nomes citados e colunas opcionais existentes."""
    t: dict[str, str]                 # nome lógico → [banco].[dbo].[tabela]
    iot: str                          # [MQTT].[dbo].[IOT001]
    canais_iot: set[str]              # chdN_value existentes na IOT001
    maquinas_atualizado_em: bool
    op_status_em: bool
    apont_atualizado_por: bool = False


def detectar(cursor, oee: OeeConfig, sql: SqlServerConfig) -> tuple[Esquema | None, str]:
    """(esquema, "") se dá para apontar; (None, motivo) se falta banco, tabela, permissão ou coluna."""
    banco = sql_conexao.validar_banco(oee.banco)
    t = {nome: f"[{banco}].[dbo].[{nome}]" for nome in TABELAS_OEE}
    banco_iot = sql_conexao.validar_banco(sql.banco)
    schema_iot, tabela_iot = sql_conexao.validar_tabela(sql.tabela)
    iot = f"[{banco_iot}].[{schema_iot}].[{tabela_iot}]"
    cursor.execute("SELECT DB_ID(?)", banco)
    if cursor.fetchone()[0] is None:
        return None, f"banco {banco} inexistente"
    for nome, alvo in t.items():
        cursor.execute("SELECT OBJECT_ID(?, 'U')", alvo)
        if cursor.fetchone()[0] is None:
            return None, f"tabela {nome} inexistente ou sem acesso ao banco"
    cursor.execute(sql_permissoes_oee(banco))
    linha = cursor.fetchone()
    faltam = [f"{p} em {tb}" for (tb, p), v in zip(PERMISSOES_OEE, linha) if v != 1]
    if faltam:
        return None, f"sem permissão de {', '.join(faltam)}"
    colunas: dict[str, set[str]] = {}
    for nome, obrigatorias in TABELAS_OEE.items():
        colunas[nome] = sql_conexao.colunas_da_tabela(cursor, "dbo", nome, banco=banco)
        ausentes = sorted(obrigatorias - colunas[nome])
        if ausentes:
            return None, f"colunas ausentes em {nome}: {', '.join(ausentes)}"
    cursor.execute(SQL_PERM_IOT.format(banco=f"[{banco_iot}]"), f"[{schema_iot}].[{tabela_iot}]")
    if cursor.fetchone()[0] != 1:
        return None, f"sem permissão de SELECT em {banco_iot}.{schema_iot}.{tabela_iot}"
    col_iot = sql_conexao.colunas_da_tabela(cursor, schema_iot, tabela_iot, banco=banco_iot)
    if not {"device_id", "ts_unix"} <= col_iot:
        return None, f"tabela {banco_iot}.{schema_iot}.{tabela_iot} inexistente ou sem device_id/ts_unix"
    return Esquema(t=t, iot=iot, canais_iot={c for c in CANAIS if c in col_iot},
                   maquinas_atualizado_em="atualizado_em" in colunas["maquinas"],
                   op_status_em="status_em" in colunas["ordens_producao"]), ""


def sql_maquinas(e: Esquema) -> str:
    sel = ", ".join(f"m.[iot{n}_canal], m.[iot{n}_tipo], d{n}.[codigo], d{n}.[ativo]" for n in SLOTS)
    joins = " ".join(f"LEFT JOIN {e.t['dispositivos_iot']} d{n} ON d{n}.[id] = m.[iot{n}_dispositivo_id]"
                     for n in SLOTS)
    algum = " OR ".join(f"m.[iot{n}_dispositivo_id] IS NOT NULL" for n in SLOTS)
    return (f"SELECT m.[id], m.[codigo], m.[nome], m.[micro_parada_s], m.[macro_parada_s], m.[situacao], "
            f"m.[situacao_desde], m.[situacao_legenda_id], m.[ordem_producao_id], {sel} "
            f"FROM {e.t['maquinas']} m {joins} "
            f"WHERE m.[ativo] = 1 AND m.[d_e_l_e_t_] <> '*' AND ({algum}) ORDER BY m.[nome], m.[id]")


@dataclass
class Maquina:
    id: int
    codigo: str
    nome: str
    micro: int | None
    macro: int | None
    situacao: str | None
    situacao_desde: int | None
    situacao_legenda_id: int | None
    ordem_producao_id: int | None
    canais: list[tuple[str, str]]          # (device_id, chdN_value), sem repetição


def maquina_da_linha(linha) -> Maquina:
    canais: list[tuple[str, str]] = []
    for i, _n in enumerate(SLOTS):
        canal, tipo, device, ativo = linha[9 + 4 * i: 13 + 4 * i]
        canal = canal_valido(canal)
        tipo = str(tipo or "").strip().lower()
        device = str(device or "").strip()
        if canal and device and ativo and tipo in ("", "producao") and (device, canal) not in canais:
            canais.append((device, canal))

    def inteiro(v):
        return None if v is None else int(v)

    return Maquina(id=int(linha[0]), codigo=str(linha[1] or ""), nome=str(linha[2] or ""),
                   micro=inteiro(linha[3]), macro=inteiro(linha[4]), situacao=linha[5],
                   situacao_desde=epoch_de(linha[6]), situacao_legenda_id=inteiro(linha[7]),
                   ordem_producao_id=inteiro(linha[8]), canais=canais)


@dataclass
class Legenda:
    id: int
    codigo: str
    descricao: str
    tipo: str | None
    ativo: bool


# ============================================================================================ worker


class ApontamentosOee:
    """Ciclo periódico dos apontamentos reais, em thread própria e conexão própria. Nunca levanta exceção.

    ``executar_ciclo()`` roda um ciclo na thread de quem chama (testes); a thread (``iniciar``) chama o mesmo.
    ``conectar`` existe para os testes (conexão falsa).
    """

    def __init__(self, sql: SqlServerConfig | None, oee_cfg: OeeConfig | None, apont_cfg: ApontamentosConfig | None,
                 logar: Logar = _sem_log, ao_erro: AoErro = _nada, ao_resultado: AoResultado = _nada,
                 relogio: Relogio = time.time, conectar: Callable[[SqlServerConfig], object] | None = None) -> None:
        self._sql = replace(sql) if sql is not None else None
        self._oee = replace(oee_cfg) if oee_cfg is not None else OeeConfig()
        self._cfg = replace(apont_cfg) if apont_cfg is not None else ApontamentosConfig()
        self._logar = logar
        self._ao_erro = ao_erro
        self._ao_resultado = ao_resultado
        self._relogio = relogio
        self._conectar = conectar
        self._fuso = fuso_de(self._cfg.fuso)
        self._minutos = self._normalizar_minutos(self._cfg.minutos)
        self._trava = threading.Lock()
        self._evento = threading.Event()
        self._thread: threading.Thread | None = None
        self._parado = False
        self._pedido = False
        self._proxima: float | None = None          # epoch (relogio) do próximo ciclo
        self._ultimo_inicio: float | None = None
        self._ciclo_trava = threading.Lock()        # um ciclo por vez
        # estado da thread do ciclo
        self._conexao = None
        self._esquema: Esquema | None = None
        self._infos: set[tuple] = set()             # INFO "portal não encontrado" já dado (até funcionar)
        self._aviso_legendas = False
        self._sem_parametros: set[int] = set()      # máquinas sem micro/macro já avisadas (sessão)
        self._erro_conexao = False                  # incidente de conexão aberto (1 erro por incidente)
        self._trocou_sql = False                    # definir_sql: a próxima execução reconecta

    # -- API (qualquer thread) ---------------------------------------------------------------------------

    def iniciar(self) -> None:
        """Começa a thread (se habilitado). O 1º ciclo sai logo que o SQL Server estiver configurado."""
        if self._parado or not self._cfg.habilitado:
            return
        with self._trava:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._proxima is None:
                self._proxima = self._relogio()
            self._thread = threading.Thread(target=self._loop, name="oee-apontamentos", daemon=True)
            self._thread.start()

    def parar(self, timeout: float = 1.0) -> None:
        self._parado = True
        self._evento.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        if thread is None or not thread.is_alive():
            self._fechar_conexao()

    def definir_sql(self, sql: SqlServerConfig) -> None:
        with self._trava:
            estava = self._sql_ativo()
            self._sql = replace(sql) if sql is not None else None
            if not estava and self._sql_ativo():
                self._proxima = self._relogio()      # passou a ter senha: roda já
            self._trocou_sql = True
        self._evento.set()

    def definir_periodicidade(self, minutos: float) -> None:
        """Vale já: a próxima execução passa a ser a última + o novo intervalo (ou agora, se já passou)."""
        m = self._normalizar_minutos(minutos)
        with self._trava:
            self._minutos = m
            base = self._ultimo_inicio if self._ultimo_inicio is not None else self._relogio()
            self._proxima = max(self._relogio(), base + m * 60)
        self._logar("INFO", f"Apontamentos reais: periodicidade de {m:g} min.")
        self._evento.set()

    def executar_agora(self) -> None:
        """Não bloqueia: pede um ciclo já (inicia a thread, se preciso)."""
        if not self._cfg.habilitado:
            self._emitir(self._resultado_vazio(False, "Apontamentos reais desligados ([apontamentos] habilitado "
                                                      "= false no config.toml)."))
            return
        with self._trava:
            self._pedido = True
        self.iniciar()
        self._evento.set()

    @property
    def minutos(self) -> float:
        return self._minutos

    # -- thread -------------------------------------------------------------------------------------------

    @staticmethod
    def _normalizar_minutos(minutos: Any) -> float:
        try:
            m = float(minutos)
        except (TypeError, ValueError):
            m = 3.0
        if m != m or m <= 0:   # NaN ou não positivo
            m = 3.0
        return max(MIN_MINUTOS, min(m, 24 * 60))

    def _sql_ativo(self) -> bool:
        sql = self._sql
        return bool(sql is not None and sql.habilitado and (sql.autenticacao_windows or sql.senha))

    def _loop(self) -> None:
        while not self._parado:
            with self._trava:
                agora = self._relogio()
                devido = self._pedido or (self._sql_ativo() and self._proxima is not None
                                          and agora >= self._proxima)
                pedido = self._pedido
                self._pedido = False
                espera = 30.0 if self._proxima is None else min(30.0, max(0.05, self._proxima - agora))
            if devido:
                if pedido or self._sql_ativo():
                    self.executar_ciclo()
                continue
            self._evento.wait(espera)
            self._evento.clear()
        self._fechar_conexao()

    # -- ciclo --------------------------------------------------------------------------------------------

    def executar_ciclo(self) -> dict:
        """Roda um ciclo completo (bloqueante) e emite o resultado. Devolve o dicionário emitido."""
        with self._ciclo_trava:
            inicio = self._relogio()
            with self._trava:
                trocar, self._trocou_sql = self._trocou_sql, False
                self._ultimo_inicio = inicio
                self._proxima = inicio + self._minutos * 60
                proxima = self._proxima if self._thread is not None and self._thread.is_alive() else None
            if trocar:
                self._fechar_conexao()
            try:
                resultado = self._ciclo(inicio)
            except Exception as exc:  # noqa: BLE001 - nunca derruba a thread
                logger.exception("Falha inesperada no ciclo de apontamentos")
                resultado = self._resultado_vazio(False, f"Falha inesperada: {exc}")
            resultado["quando"] = iso_local(inicio)
            resultado["proxima"] = iso_local(proxima)
            resultado["minutos"] = self._minutos
            self._emitir(resultado)
            return resultado

    def _resultado_vazio(self, ok: bool, mensagem: str) -> dict:
        return {"quando": iso_local(self._relogio()), "proxima": None, "minutos": self._minutos, "ok": ok,
                "mensagem": mensagem, "criados": 0, "alterados": 0, "excluidos": 0, "maquinas": []}

    def _emitir(self, resultado: dict) -> None:
        try:
            self._ao_resultado(json.dumps(resultado, ensure_ascii=False))
        except Exception:  # noqa: BLE001
            logger.exception("Falha no callback de resultado dos apontamentos")

    def _ciclo(self, inicio: float) -> dict:
        if not self._cfg.habilitado:
            return self._resultado_vazio(False, "Apontamentos reais desligados no config.toml.")
        if not self._sql_ativo():
            return self._resultado_vazio(False, "SQL Server não configurado (falta a senha): apontamentos reais "
                                                "parados.")
        agora = int(inicio)
        # conexão + detecção
        try:
            if self._conexao is None:
                self._conexao = (self._conectar or sql_conexao.conectar)(self._sql)
                self._esquema = None
            if self._esquema is None:
                esquema, motivo = self._detectar()
                if esquema is None:
                    self._fechar_conexao()
                    self._info_portal_ausente(motivo)
                    return self._resultado_vazio(False, f"Portal OEE não disponível para os apontamentos reais: "
                                                        f"{motivo}.")
                self._esquema = esquema
            legendas, todas = self._ler_legendas()
            if legendas is None:
                return self._resultado_vazio(False, todas)
            maquinas = self._ler_maquinas()
        except Exception as exc:  # noqa: BLE001
            self._rollback()
            self._fechar_conexao()
            return self._falha_geral(exc)
        self._infos.clear()
        self._erro_conexao = False

        total = {"criados": 0, "alterados": 0, "excluidos": 0}
        linhas: list[dict] = []
        falhas = 0
        processadas = 0
        conexao_caiu = False
        for m in maquinas:
            linha = self._linha_maquina(m)
            linhas.append(linha)
            if not m.canais:
                continue
            if not m.micro or not m.macro or m.micro <= 0 or m.macro <= m.micro:
                linha["aviso"] = "Micro/macro parada não cadastrados (ou macro ≤ micro): máquina ignorada."
                if m.id not in self._sem_parametros:
                    self._sem_parametros.add(m.id)
                    self._logar("INFO", f"Apontamentos reais: máquina {self._nome(m)} sem micro/macro parada "
                                        "válidos no portal OEE; ignorada.")
                continue
            if conexao_caiu:
                linha["aviso"] = "Não processada: conexão com o SQL Server perdida neste ciclo."
                continue
            processadas += 1
            try:
                contagem, avisos = self._processar(m, linha, legendas, todas, agora)
            except Exception as exc:  # noqa: BLE001
                self._rollback()
                falhas += 1
                resumo = sql_conexao.traduzir_erro(exc).replace("\n", " ")
                linha["aviso"] = f"Falha: {resumo}"
                self._ao_erro(f"Falha nos apontamentos reais da máquina {self._nome(m)}; nada foi gravado "
                              "para ela neste ciclo.", resumo)
                if _erro_de_conexao(exc):
                    self._fechar_conexao()
                    conexao_caiu = True
                continue
            for k in total:
                total[k] += contagem[k]
            for aviso in avisos:
                self._logar("AVISO", aviso)
        n_maq = processadas
        resumo = (f"Apontamentos reais: {n_maq} {'máquina' if n_maq == 1 else 'máquinas'}, "
                  f"{total['criados']} criados, {total['alterados']} atualizados, {total['excluidos']} excluídos")
        if falhas:
            resumo += f", {falhas} com falha"
        self._logar("INFO", resumo)
        if not maquinas:
            mensagem = "Nenhuma máquina ativa com dispositivo IoT de produção no portal OEE."
        else:
            mensagem = resumo
        return {"ok": falhas == 0, "mensagem": mensagem, **total, "maquinas": linhas}

    # -- partes do ciclo ------------------------------------------------------------------------------------

    def _detectar(self) -> tuple[Esquema | None, str]:
        cursor = self._conexao.cursor()
        try:
            try:
                return detectar(cursor, self._oee, self._sql)
            except ValueError as exc:   # nome de banco/tabela inválido na configuração
                return None, str(exc)
            except Exception as exc:  # noqa: BLE001
                if _erro_de_conexao(exc):
                    raise
                return None, sql_conexao.traduzir_erro(exc).splitlines()[0]
        finally:
            _fechar(cursor)
            self._commit_silencioso()

    def _info_portal_ausente(self, motivo: str) -> None:
        chave = (getattr(self._sql, "servidor", ""), self._oee.banco)
        logger.info("Apontamentos reais desligados: %s", motivo)
        if chave not in self._infos:
            self._infos.add(chave)
            self._logar("INFO", f"Apontamentos reais: portal OEE não disponível neste SQL Server (banco "
                                f"{self._oee.banco}: {motivo}). Nova tentativa a cada ciclo.")

    def _falha_geral(self, exc: Exception) -> dict:
        resumo = sql_conexao.traduzir_erro(exc).replace("\n", " ")
        if not self._erro_conexao:
            self._erro_conexao = True
            self._ao_erro("Apontamentos reais: não foi possível acessar o SQL Server; nova tentativa a cada ciclo.",
                          resumo)
        else:
            logger.info("Apontamentos reais: SQL Server ainda inacessível: %s", resumo)
        return self._resultado_vazio(False, f"Falha ao acessar o SQL Server: {resumo}")

    def _ler_legendas(self) -> tuple[LegendasIot | None, Any]:
        e = self._esquema
        cursor = self._conexao.cursor()
        try:
            cursor.execute(f"SELECT [id], [codigo], [descricao], [tipo], [ativo] FROM {e.t['legendas']}")
            todas: dict[int, Legenda] = {}
            for l in cursor.fetchall() or []:
                todas[int(l[0])] = Legenda(int(l[0]), str(l[1] or "").strip(), str(l[2] or ""), l[3], bool(l[4]))
        finally:
            _fechar(cursor)
            self._commit_silencioso()
        codigos = {"produzindo": self._cfg.legenda_produzindo, "micro": self._cfg.legenda_micro,
                   "macro": self._cfg.legenda_macro}
        achadas: dict[str, int] = {}
        for chave, codigo in codigos.items():
            ids = [l.id for l in todas.values() if l.ativo and l.codigo == str(codigo).strip()]
            if ids:
                achadas[chave] = min(ids)
        faltam = [str(codigos[k]) for k in codigos if k not in achadas]
        if faltam:
            msg = (f"Apontamentos reais: legenda(s) {', '.join(faltam)} não cadastrada(s) ou inativa(s) no portal "
                   "OEE; nenhum apontamento foi feito. Cadastre-as no portal.")
            if not self._aviso_legendas:
                self._aviso_legendas = True
                self._logar("AVISO", msg)
            return None, msg
        self._aviso_legendas = False
        return LegendasIot(achadas["produzindo"], achadas["micro"], achadas["macro"]), todas

    def _ler_maquinas(self) -> list[Maquina]:
        cursor = self._conexao.cursor()
        try:
            cursor.execute(sql_maquinas(self._esquema))
            return [maquina_da_linha(l) for l in cursor.fetchall() or []]
        finally:
            _fechar(cursor)
            self._commit_silencioso()

    @staticmethod
    def _nome(m: Maquina) -> str:
        return f"{m.codigo} ({m.nome})" if m.nome and m.nome != m.codigo else (m.codigo or str(m.id))

    def _linha_maquina(self, m: Maquina) -> dict:
        return {"maquina_id": m.id, "codigo": m.codigo, "nome": m.nome,
                "canais": ", ".join(f"{d}/{c}" for d, c in m.canais), "micro_s": int(m.micro or 0),
                "macro_s": int(m.macro or 0), "legenda": "", "legenda_descricao": "",
                "desde": iso_local(m.situacao_desde), "ultima_producao": None, "apontamentos_hoje": 0,
                "ultima_leitura": None,
                "aviso": None if m.canais else "Sem canal IoT de produção com dispositivo ativo."}

    def _processar(self, m: Maquina, linha: dict, legendas: LegendasIot, todas: dict[int, Legenda], agora: int
                   ) -> tuple[dict, list[str]]:
        """Uma máquina, numa transação. Devolve (contagem, avisos para o log depois do commit)."""
        e = self._esquema
        A = e.t["apontamentos_estado"]
        hoje = inicio_do_dia(agora, self._fuso)
        ontem = inicio_do_dia(agora, self._fuso, -1)
        cur = self._conexao.cursor()
        try:
            cur.execute(f"SELECT TOP 1 1 FROM {A} WHERE [maquina_id] = ? AND [inicio] >= ? AND [inicio] < ? "
                        "AND LEFT([observacao], 5) = ?", m.id, utc_sql(ontem), utc_sql(hoje), MARCA)
            ini = ontem if cur.fetchone() is not None else hoje

            # leituras: por dispositivo, só as colunas dos canais da máquina
            desde = ini - m.macro - MARGEM_LEITURAS
            por_device: dict[str, list[str]] = {}
            ausentes: list[str] = []
            for device, canal in m.canais:
                if canal in e.canais_iot:
                    por_device.setdefault(device, []).append(canal)
                else:
                    ausentes.append(canal)
            if not por_device:
                # nenhum canal tem coluna na IOT001: não dá para saber se produziu (não aponta 98 por isso)
                linha["aviso"] = (f"Canal sem coluna na IOT001: {', '.join(ausentes)} "
                                  "(rode sql/canais_4_a_6_iot001.sql); máquina ignorada.")
                return {"criados": 0, "alterados": 0, "excluidos": 0}, []
            listas: list[list[int]] = []
            ultima_leitura: int | None = None
            for device, canais in por_device.items():
                cols = ", ".join(f"[{c}]" for c in canais)
                cur.execute(f"SELECT [ts_unix], {cols} FROM {e.iot} WHERE [device_id] = CAST(? AS varchar(100)) "
                            "AND [ts_unix] >= ? AND [ts_unix] <= ? ORDER BY [ts_unix]", device, desde, agora)
                leituras = cur.fetchall() or []
                for i in range(len(canais)):
                    listas.append(producoes((l[0], l[1 + i]) for l in leituras))
                if leituras:
                    ultima_leitura = max(ultima_leitura or 0, int(leituras[-1][0]))
            if ultima_leitura is None:
                for device in por_device:
                    cur.execute(f"SELECT MAX([ts_unix]) FROM {e.iot} WHERE [device_id] = CAST(? AS varchar(100))",
                                device)
                    r = cur.fetchone()
                    if r is not None and r[0] is not None:
                        ultima_leitura = max(ultima_leitura or 0, int(r[0]))
            prods = juntar_producoes(*listas)
            prods = [p for p in prods if p <= agora]

            desejados = dividir_por_dia(segmentos_continuos(prods, ini, agora, m.micro, m.macro), self._fuso)
            cur.execute(f"SELECT [id], [legenda_id], [inicio], [fim], [observacao], [ordem_producao_id] "
                        f"FROM {A} WITH (UPDLOCK) WHERE [maquina_id] = ? AND [inicio] <= ? "
                        "AND ([fim] IS NULL OR [fim] >= ?) ORDER BY [inicio], [id]",
                        m.id, utc_sql(agora), utc_sql(ini))
            existentes = [{"id": int(r[0]), "legenda_id": int(r[1]), "inicio": epoch_de(r[2]),
                           "fim": epoch_de(r[3]), "observacao": r[4],
                           "ordem_producao_id": None if r[5] is None else int(r[5])}
                          for r in cur.fetchall() or []]
            plano = planejar(desejados, existentes, ini, agora, legendas, m.ordem_producao_id, self._fuso)
            contagem, avisos = self._aplicar(cur, m, plano, todas, agora)

            # situação da máquina pelo apontamento mais recente (SituacaoMaquina do portal)
            L = e.t["legendas"]
            cur.execute(f"SELECT TOP 1 a.[inicio], a.[legenda_id], a.[ordem_producao_id], l.[tipo] FROM {A} a "
                        f"LEFT JOIN {L} l ON l.[id] = a.[legenda_id] WHERE a.[maquina_id] = ? "
                        "ORDER BY a.[inicio] DESC, a.[id] DESC", m.id)
            r = cur.fetchone()
            if r is not None:
                novo = (situacao_pela_legenda(r[3]), epoch_de(r[0]), int(r[1]),
                        None if r[2] is None else int(r[2]))
                atual = (m.situacao, m.situacao_desde, m.situacao_legenda_id, m.ordem_producao_id)
                if novo != atual:
                    sets = "[situacao] = ?, [situacao_desde] = ?, [situacao_legenda_id] = ?, [ordem_producao_id] = ?"
                    valores = [novo[0], utc_sql(novo[1]), novo[2], novo[3]]
                    if e.maquinas_atualizado_em:
                        sets += ", [atualizado_em] = ?"
                        valores.append(utc_sql(agora))
                    cur.execute(f"UPDATE {e.t['maquinas']} SET {sets} WHERE [id] = ?", *valores, m.id)
                    if novo[3] != m.ordem_producao_id and novo[3] is not None:
                        plano_ops = {novo[3]}
                    else:
                        plano_ops = set()
                else:
                    plano_ops = set()
                leg = todas.get(int(r[1]))
                linha["legenda"] = leg.codigo if leg else ""
                linha["legenda_descricao"] = leg.descricao if leg else ""
                linha["desde"] = iso_local(epoch_de(r[0]))
            else:
                plano_ops = set()
            for op in sorted(plano.ops_afetadas() | plano_ops):
                self._atualizar_status_op(cur, op, agora)

            cur.execute(f"SELECT COUNT(*) FROM {A} WHERE [maquina_id] = ? AND [inicio] >= ?", m.id, utc_sql(hoje))
            linha["apontamentos_hoje"] = int((cur.fetchone() or [0])[0] or 0)
            self._conexao.commit()
        except Exception:
            self._rollback()
            raise
        finally:
            _fechar(cur)
        linha["ultima_producao"] = iso_local(prods[-1]) if prods else None
        linha["ultima_leitura"] = iso_local(ultima_leitura)
        avisos_linha = []
        if ausentes:
            avisos_linha.append(f"Canal sem coluna na IOT001: {', '.join(ausentes)}")
        if ultima_leitura is None:
            avisos_linha.append("Nenhuma leitura do dispositivo na IOT001")
        linha["aviso"] = "; ".join(avisos_linha) + "." if avisos_linha else None
        return contagem, avisos

    def _aplicar(self, cur, m: Maquina, plano: Plano, todas: dict[int, Legenda], agora: int
                 ) -> tuple[dict, list[str]]:
        A = self._esquema.t["apontamentos_estado"]
        agora_sql = utc_sql(agora)
        contagem = {"criados": 0, "alterados": 0, "excluidos": 0}
        avisos: list[str] = []
        for x in plano.excluir:
            try:
                cur.execute(f"DELETE FROM {A} WHERE [id] = ?", x["id"])
            except Exception as exc:  # noqa: BLE001
                if classificar_erro(exc)[0] != "dados":
                    raise
                # FK (OS/SS ligada ao apontamento): o SQL Server só cancela o comando, a transação continua
                avisos.append(f"Apontamentos reais: o apontamento {self._descrever(x, todas)} da máquina "
                              f"{self._nome(m)} não pôde ser excluído (está ligado a outro registro do portal, "
                              "ex.: OS/SS); mantido.")
                continue
            contagem["excluidos"] += 1
            if x["manual"]:
                avisos.append(f"Apontamentos reais: apontamento manual {self._descrever(x, todas)} da máquina "
                              f"{self._nome(m)} excluído (coberto pelo IoT).")
        for x in plano.atualizar:
            cur.execute(f"UPDATE {A} SET [inicio] = ?, [fim] = ?, [legenda_id] = ?, [atualizado_em] = ? "
                        "WHERE [id] = ?", utc_sql(x["inicio"]), None if x["fim"] is None else utc_sql(x["fim"]),
                        x["legenda_id"], agora_sql, x["id"])
            contagem["alterados"] += 1
            if x["manual"]:
                avisos.append(f"Apontamentos reais: apontamento manual {self._descrever(x['antes'], todas)} da "
                              f"máquina {self._nome(m)} aparado pelo IoT (agora termina às "
                              f"{self._hora(x['fim'])}).")
        for x in plano.inserir:
            cur.execute(f"INSERT INTO {A} ([maquina_id], [legenda_id], [ordem_producao_id], [inicio], [fim], "
                        "[observacao], [criado_em], [atualizado_em]) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        m.id, x["legenda_id"], x["ordem_producao_id"], utc_sql(x["inicio"]),
                        None if x["fim"] is None else utc_sql(x["fim"]), OBSERVACAO, agora_sql, agora_sql)
            contagem["criados"] += 1
        return contagem, avisos

    def _atualizar_status_op(self, cur, op: int, agora: int) -> None:
        e = self._esquema
        O, A, L = e.t["ordens_producao"], e.t["apontamentos_estado"], e.t["legendas"]
        cur.execute(f"SELECT [c2_datrf], [c2_status], [c2_quje], [status] FROM {O} WHERE [id] = ?", op)
        r = cur.fetchone()
        if r is None:
            return
        cur.execute(f"SELECT TOP 1 l.[tipo] FROM {A} a LEFT JOIN {L} l ON l.[id] = a.[legenda_id] "
                    "WHERE a.[ordem_producao_id] = ? AND a.[fim] IS NULL ORDER BY a.[inicio] DESC, a.[id] DESC", op)
        aberto = cur.fetchone()
        tipo = None if aberto is None else (aberto[0] or "")
        if aberto is not None:
            tem = True
        else:
            cur.execute(f"SELECT TOP 1 1 FROM {A} WHERE [ordem_producao_id] = ?", op)
            tem = cur.fetchone() is not None
        novo = status_op(r[0], r[1], r[2], tipo, tem)
        if novo == r[3]:
            return
        if e.op_status_em:
            cur.execute(f"UPDATE {O} SET [status] = ?, [status_em] = ? WHERE [id] = ?", novo, utc_sql(agora), op)
        else:
            cur.execute(f"UPDATE {O} SET [status] = ? WHERE [id] = ?", novo, op)

    def _hora(self, t: int | None) -> str:
        if t is None:
            return "em aberto"
        return datetime.fromtimestamp(t, self._fuso).strftime("%d/%m/%Y %H:%M:%S")

    def _descrever(self, a: dict, todas: dict[int, Legenda]) -> str:
        leg = todas.get(a.get("legenda_id"))
        nome_leg = f"{leg.codigo} {leg.descricao}".strip() if leg else f"legenda {a.get('legenda_id')}"
        return f"«{nome_leg}» de {self._hora(a.get('inicio'))} a {self._hora(a.get('fim'))}"

    # -- conexão --------------------------------------------------------------------------------------------

    def _commit_silencioso(self) -> None:
        try:
            if self._conexao is not None:
                self._conexao.commit()
        except Exception:  # noqa: BLE001
            pass

    def _rollback(self) -> None:
        if self._conexao is not None:
            try:
                self._conexao.rollback()
            except Exception:  # noqa: BLE001
                pass

    def _fechar_conexao(self) -> None:
        conexao, self._conexao = self._conexao, None
        self._esquema = None
        if conexao is not None:
            try:
                conexao.close()
            except Exception:  # noqa: BLE001
                pass


def _fechar(cursor) -> None:
    try:
        cursor.close()
    except Exception:  # noqa: BLE001
        pass


def _erro_de_conexao(exc: BaseException) -> bool:
    return classificar_erro(exc)[0] == "conexao"
