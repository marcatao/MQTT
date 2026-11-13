"""Apontamentos reais pelo IoT: ``MQTT.dbo.IOT001`` → ``OEE.dbo.apontamentos_estado`` (portal OEE).

Especificação: ``docs/apontamentos_reais.md`` (fonte da verdade). Resumo:

- Máquinas: ``ativo = 1``, ``d_e_l_e_t_ <> '*'``, ``micro_parada_s``/``macro_parada_s`` preenchidos e pelo menos um
  canal (``iotN_dispositivo_id`` + ``iotN_canal``) com dispositivo ativo e tipo ``producao`` (ou NULL).
- **Produção** = leitura em que o contador do canal subiu em relação à leitura anterior do mesmo canal (valor menor =
  contador zerado = nova base). Com vários canais, vale a produção de qualquer um.
- Intervalo entre produções ``gap``: ``< micro`` → 2 Produção Normal; ``micro ≤ gap < macro`` → 99 Microparada;
  ``≥ macro`` → 98 Macro parada. A parada começa na última produção; o trecho em aberto usa ``agora − p``.
- **Falta de linhas na IOT001** (regra revista em 13/11/2026): não gera 97 nem desconta nada do intervalo; é máquina
  parada como qualquer outra (99/98; fora do turno, 96).
- **97 Sem apontamento IOT só vem do diagnóstico** (abaixo). Leituras que chegam depois dentro de um 97 do integrador
  (até ``dias_correcao`` dias) fazem a janela recomeçar no dia dele (correção retroativa). Sem a legenda 97 no
  portal: AVISO e o aparelho mudo não muda nada (99/98).
- **Fora de turno**: pela jornada da máquina (``horarios_semana`` ou ``dias_semana`` + ``jornada_*``) e as exceções
  do calendário (``calendario_dias``), as paradas (99/98/97) fora do turno viram 96; produção continua 2. Sem
  jornada, colunas/tabelas ou legenda 96: sem 96 (legenda ausente = 1 AVISO).
- **Diagnóstico**: antes de apontar, o ciclo pergunta aos devices (callback ``diagnosticar`` do pipeline); quem não
  responde tem a parada em aberto (99/98) apontada como 97 a partir da última notícia dele (última linha na IOT001,
  se depois da última produção) até agora; fora do turno, 96. Quem não mandou o ack mas mandou qualquer mensagem MQTT
  nos últimos 60 s (``DIAG_RECENTE``) conta como respondendo: "ok (dados recentes)".
- **Recalcular** (``recalcular``): refaz uma máquina desde 00:00 local de um dia (até ``dias_correcao`` atrás), na
  thread do ciclo; responde em ``ao_recalculo`` e reemite o último resultado com a linha dela atualizada.

JSON de cada ciclo (``ao_resultado``)::

    {"quando", "proxima" (ISO local | null), "minutos", "ok", "mensagem", "criados", "alterados", "excluidos",
     "maquinas": [{"maquina_id", "codigo", "nome", "canais", "micro_s", "macro_s", "legenda", "legenda_descricao",
                   "desde", "ultima_producao", "apontamentos_hoje", "ultima_leitura", "aviso",
                   "diag": "ok" | "ok (dados recentes)" | "sem resposta" | "sem MQTT" | null,
                   "turno": "dentro" | "fora" | "sem jornada",
                   "jornada": "Seg–Qui 05:00–14:48, Sex 05:00–13:48" | null}]}
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
AoRecalculo = Callable[[int, str, bool, str], None]  # (maquina_id, dia, sucesso, mensagem)
# (devices, timeout) → {device: True respondeu | DIAG_RECENTE sem ack, mas mandou mensagem MQTT há ≤ 60 s (conta como
# respondeu) | False sem resposta | None não deu para perguntar (sem MQTT)}
Diagnosticar = Callable[[list, float], dict]
DIAG_RECENTE = "recente"
DIAG_OK, DIAG_OK_RECENTE, DIAG_SEM_RESPOSTA, DIAG_SEM_MQTT = "ok", "ok (dados recentes)", "sem resposta", "sem MQTT"
Relogio = Callable[[], float]

PRODUZINDO, MICRO, MACRO, SEM_IOT, FORA_TURNO = "produzindo", "micro", "macro", "sem_iot", "fora_turno"
PARADAS_TURNO = (MICRO, MACRO, SEM_IOT)     # paradas que viram 96 fora do turno (produção continua 2)
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

    O intervalo é só o tempo sem o contador subir (regra revista em 13/11/2026): falta de linhas na IOT001 não
    desconta nada nem gera 97 — é máquina parada como qualquer outra (99/98; fora do turno, 96).
    """
    ordenadas = sorted({int(p) for p in prods if int(p) <= agora})
    antes = [p for p in ordenadas if p < ini]
    dentro = [p for p in ordenadas if p >= ini]
    anterior: int | None = antes[-1] if antes else None
    cursor = ini
    brutos: list[list] = []

    def gap(t: int) -> float:
        return float("inf") if anterior is None else float(t - anterior)

    for t in dentro:
        if t > cursor:
            brutos.append([cursor, t, classificar(gap(t), micro, macro)])
        anterior, cursor = t, t
    brutos.append([cursor, agora, classificar(gap(agora), micro, macro)])
    mesclados: list[list] = []
    for seg in brutos:
        if mesclados and mesclados[-1][2] == seg[2] and mesclados[-1][1] == seg[0]:
            mesclados[-1][1] = seg[1]
        else:
            mesclados.append(seg)
    return [(a, b, e) for a, b, e in mesclados]


# ---------------------------------------------------------------------------- turno (calendário + jornada)

DIAS_CURTOS = {1: "Seg", 2: "Ter", 3: "Qua", 4: "Qui", 5: "Sex", 6: "Sáb", 7: "Dom"}
TIPOS_UTEIS = {"compensacao": True, "util": True, "feriado": False, "ponte": False, "folga": False}


def _hora_valida(texto: Any) -> str | None:
    t = str(texto or "").strip()[:5]
    try:
        h, m = int(t[:2]), int(t[3:5])
    except ValueError:
        return None
    if len(t) != 5 or t[2] != ":" or not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    return t


def jornada_de(horarios_semana: Any, dias_semana: Any, jornada_inicio: Any, jornada_fim: Any
               ) -> dict[int, tuple[str, str]]:
    """``JornadaSemanal::de`` do portal: dia ISO (1 = segunda … 7 = domingo) → (início, fim) "HH:MM" local.

    ``horarios_semana`` (JSON ``{"1": ["05:00", "14:48"], …}``) manda quando existe; senão ``dias_semana``
    ("12345") + ``jornada_inicio``/``jornada_fim`` para todos esses dias. Horário inválido ou início = fim é ignorado.
    """
    horarios: dict[int, tuple[str, str]] = {}
    lido = None
    if isinstance(horarios_semana, str) and horarios_semana.strip():
        try:
            lido = json.loads(horarios_semana)
        except ValueError:
            lido = None
    if isinstance(lido, dict):
        for dia, h in lido.items():
            try:
                n = int(dia)
            except (TypeError, ValueError):
                continue
            if n in DIAS_CURTOS and isinstance(h, (list, tuple)) and len(h) >= 2:
                ini, fim = _hora_valida(h[0]), _hora_valida(h[1])
                if ini and fim and ini != fim:
                    horarios[n] = (ini, fim)
        return dict(sorted(horarios.items()))
    ini, fim = _hora_valida(jornada_inicio), _hora_valida(jornada_fim)
    if not ini or not fim or ini == fim:
        return {}
    for c in str(dias_semana or ""):
        if c.isdigit() and int(c) in DIAS_CURTOS:
            horarios[int(c)] = (ini, fim)
    return dict(sorted(horarios.items()))


def texto_jornada(horarios: dict[int, tuple[str, str]]) -> str | None:
    """Resumo como no portal (``JornadaSemanal::texto``), separado por vírgula: dias seguidos com o mesmo horário
    ficam juntos ("Seg–Qui 05:00–14:48, Sex 05:00–13:48"). Sem jornada: None."""
    grupos: list[list] = []
    for dia, h in sorted(horarios.items()):
        if grupos and grupos[-1][2] == h and grupos[-1][1] == dia - 1:
            grupos[-1][1] = dia
        else:
            grupos.append([dia, dia, h])
    partes = [f"{DIAS_CURTOS[de]}{'' if ate == de else '–' + DIAS_CURTOS[ate]} {h[0]}–{h[1]}"
              for de, ate, h in grupos]
    return ", ".join(partes) or None


@dataclass(frozen=True)
class Jornada:
    """Horário de trabalho da máquina + exceções do calendário (``DiasUteis::trabalha`` do portal)."""
    horarios: dict[int, tuple[str, str]]
    dias_semana: str = ""                       # dias de trabalho (padrão: os dias com horário; senão "12345")
    padrao: tuple[str, str] | None = None       # horário de dia útil por exceção sem horário próprio
    excecoes: dict[date, bool] = field(default_factory=dict)   # data → útil? (só com calendário)

    @property
    def vazia(self) -> bool:
        return not self.horarios

    def trabalha(self, dia: date) -> bool:
        if dia in self.excecoes:
            return self.excecoes[dia]
        dias = self.dias_semana or "".join(str(d) for d in self.horarios) or "12345"
        return str(dia.isoweekday()) in dias

    def horario(self, dia: date) -> tuple[str, str] | None:
        return self.horarios.get(dia.isoweekday()) or self.padrao or next(iter(self.horarios.values()), None)


def montar_jornada(horarios_semana: Any, dias_semana: Any, jornada_inicio: Any, jornada_fim: Any,
                   excecoes: dict[date, bool] | None = None) -> Jornada:
    horarios = jornada_de(horarios_semana, dias_semana, jornada_inicio, jornada_fim)
    ini, fim = _hora_valida(jornada_inicio), _hora_valida(jornada_fim)
    padrao = (ini, fim) if ini and fim and ini != fim else None
    dias = "".join(c for c in str(dias_semana or "") if c.isdigit())
    return Jornada(horarios, dias, padrao, dict(excecoes or {}))


def _instante(dia: date, hhmm: str, fuso: tzinfo) -> int:
    return int(datetime(dia.year, dia.month, dia.day, int(hhmm[:2]), int(hhmm[3:5]), tzinfo=fuso).timestamp())


def intervalos_turno(jornada: Jornada, ini: int, agora: int, fuso: tzinfo) -> list[tuple[int, int]]:
    """Turnos ``[início, fim)`` (epoch) que cruzam ``[ini, agora]``, sem cortar nas bordas da janela.

    O turno pertence ao dia em que começa (útil pelo calendário); fim antes do início = termina no dia seguinte.
    Sem jornada: lista vazia.
    """
    if jornada.vazia:
        return []
    saida: list[tuple[int, int]] = []
    dia = dia_local(ini, fuso) - timedelta(days=1)       # turno de ontem que vira o dia
    ultimo = dia_local(agora, fuso)
    while dia <= ultimo:
        h = jornada.horario(dia) if jornada.trabalha(dia) else None
        if h is not None:
            a = _instante(dia, h[0], fuso)
            b = _instante(dia + timedelta(days=1) if h[1] <= h[0] else dia, h[1], fuso)
            if b > ini and a <= agora:
                saida.append((a, b))
        dia += timedelta(days=1)
    mesclados: list[list[int]] = []
    for a, b in sorted(saida):
        if mesclados and a <= mesclados[-1][1]:
            mesclados[-1][1] = max(mesclados[-1][1], b)
        else:
            mesclados.append([a, b])
    return [(a, b) for a, b in mesclados]


def dentro_do_turno(t: int, turnos: list[tuple[int, int]]) -> bool:
    return any(a <= t < b for a, b in turnos)


def recortar_fora_turno(segmentos: list[tuple[int, int, str]], turnos: list[tuple[int, int]]
                        ) -> list[tuple[int, int, str]]:
    """Paradas (99/98/97) fora do turno viram ``FORA_TURNO``, cortadas nas bordas do turno; produção fica como
    está. A classificação 99/98 já veio do intervalo inteiro. O trecho de duração zero (em aberto em ``agora``)
    segue o instante dele. Mescla vizinhos iguais no fim."""
    saida: list[list] = []
    for a, b, estado in segmentos:
        if estado not in PARADAS_TURNO:
            saida.append([a, b, estado])
            continue
        if a == b:
            saida.append([a, b, estado if dentro_do_turno(a, turnos) else FORA_TURNO])
            continue
        cursor = a
        for ta, tb in turnos:
            if tb <= cursor or ta >= b:
                continue
            s, e = max(ta, cursor), min(tb, b)
            if s > cursor:
                saida.append([cursor, s, FORA_TURNO])
            saida.append([s, e, estado])
            cursor = e
        if cursor < b:
            saida.append([cursor, b, FORA_TURNO])
    mesclados: list[list] = []
    for seg in saida:
        if mesclados and mesclados[-1][2] == seg[2] and mesclados[-1][1] == seg[0]:
            mesclados[-1][1] = seg[1]
        else:
            mesclados.append(seg)
    return [(a, b, e) for a, b, e in mesclados]


def marcar_sem_resposta(segmentos: list[tuple[int, int, str]], prods: Iterable[int], ini: int,
                        ultima_leitura: int | None = None) -> list[tuple[int, int, str]]:
    """Aparelho não respondeu ao diagnóstico (única origem do 97 desde 13/11/2026): a parada em aberto (99/98, da
    última produção até agora) vira 97 **a partir do momento em que o aparelho ficou mudo** = a última notícia dele
    (``ultima_leitura``, último ``ts_unix`` na IOT001) se for depois da última produção; senão desde a última
    produção (sem produção na janela, desde ``ini``). O que vem antes desse momento continua 99/98. Produção e os
    outros estados ficam como estão; fora do turno o ``recortar_fora_turno`` (depois) troca por 96."""
    if not segmentos:
        return segmentos
    fim = segmentos[-1][1]
    if segmentos[-1][2] == PRODUZINDO:
        return segmentos         # está produzindo: não há parada em aberto
    ultimas = [int(p) for p in prods if ini <= int(p) <= fim]
    desde = max(ultimas) if ultimas else ini
    if ultima_leitura is not None and desde < int(ultima_leitura) <= fim:
        desde = int(ultima_leitura)
    # depois da última produção só existe o último trecho (a parada em aberto); os anteriores terminam antes dela
    *saida, (a, b, e) = segmentos
    if e not in (MICRO, MACRO):
        saida.append((a, b, e))
    elif a < desde:
        saida += [(a, desde, e), (desde, b, SEM_IOT)]   # 97 desde a última notícia (pode ser agora: duração zero)
    else:
        saida.append((a, b, SEM_IOT))
    mesclados: list[list] = []
    for a, b, e in saida:
        if mesclados and mesclados[-1][2] == e and mesclados[-1][1] == a:
            mesclados[-1][1] = b
        else:
            mesclados.append([a, b, e])
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
    """Ids das legendas do integrador (pelos códigos ``2``, ``99``, ``98`` e, se existir, ``97``)."""
    produzindo: int
    micro: int
    macro: int
    sem_iot: int | None = None      # None: legenda 97 ausente; o estado SEM_IOT não é gerado
    fora_turno: int | None = None   # None: legenda 96 ausente; o estado FORA_TURNO não é gerado

    def de(self, estado: str) -> int:
        if estado == SEM_IOT:
            if self.sem_iot is None:
                raise ValueError("legenda de sem leitura (97) ausente")
            return self.sem_iot
        if estado == FORA_TURNO:
            if self.fora_turno is None:
                raise ValueError("legenda de fora de turno (96) ausente")
            return self.fora_turno
        return {PRODUZINDO: self.produzindo, MICRO: self.micro, MACRO: self.macro}[estado]

    @property
    def todas(self) -> set[int]:
        ids = {self.produzindo, self.micro, self.macro}
        for opcional in (self.sem_iot, self.fora_turno):
            if opcional is not None:
                ids.add(opcional)
        return ids


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


COLUNAS_JORNADA = ("calendario_id", "horarios_semana", "dias_semana", "jornada_inicio", "jornada_fim")
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
    jornada_cols: frozenset = frozenset()     # colunas de jornada/calendário existentes em maquinas
    calendario_dias: bool = False             # [banco].[dbo].[calendario_dias] legível (exceções do calendário)


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
    # fora de turno (96): colunas de jornada e o calendário são opcionais (sem eles, não há 96)
    jornada_cols = frozenset(COLUNAS_JORNADA) & colunas["maquinas"]
    calendario = False
    t["calendario_dias"] = f"[{banco}].[dbo].[calendario_dias]"
    if "calendario_id" in jornada_cols:
        cursor.execute("SELECT OBJECT_ID(?, 'U')", t["calendario_dias"])
        if cursor.fetchone()[0] is not None:
            cursor.execute(SQL_PERM_IOT.format(banco=f"[{banco}]"), "[dbo].[calendario_dias]")
            if cursor.fetchone()[0] == 1:
                cols_cal = sql_conexao.colunas_da_tabela(cursor, "dbo", "calendario_dias", banco=banco)
                calendario = {"calendario_id", "data", "tipo"} <= cols_cal
    return Esquema(t=t, iot=iot, canais_iot={c for c in CANAIS if c in col_iot},
                   maquinas_atualizado_em="atualizado_em" in colunas["maquinas"],
                   op_status_em="status_em" in colunas["ordens_producao"],
                   jornada_cols=jornada_cols, calendario_dias=calendario), ""


def sql_maquinas(e: Esquema) -> str:
    sel = ", ".join(f"m.[iot{n}_canal], m.[iot{n}_tipo], d{n}.[codigo], d{n}.[ativo]" for n in SLOTS)
    joins = " ".join(f"LEFT JOIN {e.t['dispositivos_iot']} d{n} ON d{n}.[id] = m.[iot{n}_dispositivo_id]"
                     for n in SLOTS)
    algum = " OR ".join(f"m.[iot{n}_dispositivo_id] IS NOT NULL" for n in SLOTS)
    jornada = ", ".join(f"m.[{c}]" if c in e.jornada_cols else "NULL" for c in COLUNAS_JORNADA)
    return (f"SELECT m.[id], m.[codigo], m.[nome], m.[micro_parada_s], m.[macro_parada_s], m.[situacao], "
            f"m.[situacao_desde], m.[situacao_legenda_id], m.[ordem_producao_id], {sel}, {jornada} "
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
    calendario_id: int | None = None
    horarios_semana: str | None = None
    dias_semana: str | None = None
    jornada_inicio: str | None = None
    jornada_fim: str | None = None


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
                   ordem_producao_id=inteiro(linha[8]), canais=canais,
                   **dict(zip(("calendario_id", "horarios_semana", "dias_semana", "jornada_inicio", "jornada_fim"),
                              _jornada_da_linha(linha))))


def _jornada_da_linha(linha) -> tuple:
    extra = list(linha[9 + 4 * len(SLOTS): 9 + 4 * len(SLOTS) + len(COLUNAS_JORNADA)])
    extra += [None] * (len(COLUNAS_JORNADA) - len(extra))
    cal = extra[0]
    try:
        cal = None if cal is None else int(cal)
    except (TypeError, ValueError):
        cal = None
    return (cal, *(None if v is None else str(v) for v in extra[1:]))


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
                 relogio: Relogio = time.time, conectar: Callable[[SqlServerConfig], object] | None = None,
                 diagnosticar: Diagnosticar | None = None, timeout_diag: float = 15.0,
                 ao_recalculo: AoRecalculo = _nada) -> None:
        self._sql = replace(sql) if sql is not None else None
        self._oee = replace(oee_cfg) if oee_cfg is not None else OeeConfig()
        self._cfg = replace(apont_cfg) if apont_cfg is not None else ApontamentosConfig()
        self._logar = logar
        self._ao_erro = ao_erro
        self._ao_resultado = ao_resultado
        self._relogio = relogio
        self._conectar = conectar
        self._diagnosticar = diagnosticar
        self._timeout_diag = float(timeout_diag) if timeout_diag and timeout_diag > 0 else 15.0
        self._ao_recalculo = ao_recalculo
        self._recalculos: list[tuple[int, str, int]] = []   # (maquina_id, dia, epoch 00:00 local) na fila
        self._ultimo_resultado: dict | None = None
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
        self._aviso_sem_iot = False                 # AVISO da legenda 97 ausente já dado (até resolver)
        self._aviso_fora_turno = False              # AVISO da legenda 96 ausente já dado (até resolver)
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
                recalculo = self._recalculos.pop(0) if self._recalculos else None
            if recalculo is not None:
                self.executar_recalculo(*recalculo)
                continue
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
            self._ultimo_resultado = resultado
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

    def _preparar(self) -> tuple[dict | None, tuple | None]:
        """Conexão, detecção, legendas e máquinas. (resultado de falha, None) ou (None, (legendas, todas, máquinas))."""
        if not self._cfg.habilitado:
            return self._resultado_vazio(False, "Apontamentos reais desligados no config.toml."), None
        if not self._sql_ativo():
            return self._resultado_vazio(False, "SQL Server não configurado (falta a senha): apontamentos reais "
                                                "parados."), None
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
                                                        f"{motivo}."), None
                self._esquema = esquema
            legendas, todas = self._ler_legendas()
            if legendas is None:
                return self._resultado_vazio(False, todas), None
            maquinas = self._ler_maquinas()
        except Exception as exc:  # noqa: BLE001
            self._rollback()
            self._fechar_conexao()
            return self._falha_geral(exc), None
        self._infos.clear()
        self._erro_conexao = False
        return None, (legendas, todas, maquinas)

    def _elegivel(self, m: Maquina, linha: dict) -> bool:
        """Tem canal e micro/macro válidos? Sem micro/macro: aviso na linha e 1 INFO por sessão."""
        if not m.canais:
            return False
        if not m.micro or not m.macro or m.micro <= 0 or m.macro <= m.micro:
            linha["aviso"] = "Micro/macro parada não cadastrados (ou macro ≤ micro): máquina ignorada."
            if m.id not in self._sem_parametros:
                self._sem_parametros.add(m.id)
                self._logar("INFO", f"Apontamentos reais: máquina {self._nome(m)} sem micro/macro parada "
                                    "válidos no portal OEE; ignorada.")
            return False
        return True

    def _diagnostico(self, maquinas: list[Maquina]) -> dict[str, bool | None]:
        """Diag de todos os devices das máquinas (em paralelo, pelo pipeline). Sem callback: {}."""
        if self._diagnosticar is None or not maquinas:
            return {}
        devices = list(dict.fromkeys(d for m in maquinas for d, _c in m.canais))
        try:
            resposta = self._diagnosticar(devices, self._timeout_diag) or {}
        except Exception:  # noqa: BLE001 - sem diagnóstico, segue sem a regra
            logger.exception("Falha no diagnóstico dos dispositivos (apontamentos)")
            resposta = {}
        return {d: resposta.get(d) for d in devices}

    def _estado_diag(self, m: Maquina, diag: dict[str, bool | None]) -> str | None:
        if self._diagnosticar is None:
            return None
        valores = [diag.get(d) for d, _c in dict.fromkeys(m.canais)]
        if any(v is False for v in valores):
            return DIAG_SEM_RESPOSTA
        if all(v is None for v in valores):
            return DIAG_SEM_MQTT
        if any(v == DIAG_RECENTE for v in valores):
            return DIAG_OK_RECENTE       # sem o ack do diag, mas mandou mensagem há pouco: está na rede
        return DIAG_OK

    def _jornada(self, m: Maquina, excecoes: dict[int, dict[date, bool]]) -> Jornada:
        return montar_jornada(m.horarios_semana, m.dias_semana, m.jornada_inicio, m.jornada_fim,
                              excecoes.get(m.calendario_id, {}) if m.calendario_id is not None else {})

    def _ler_excecoes(self, maquinas: list[Maquina], agora: int) -> dict[int, dict[date, bool]]:
        """Exceções de ``calendario_dias`` dos calendários das máquinas, de ``dias_correcao`` + 2 dias atrás até
        amanhã. Sem a tabela (ou erro que não seja de conexão): {} (só os dias da semana)."""
        e = self._esquema
        ids = sorted({m.calendario_id for m in maquinas if m.calendario_id is not None})
        if not ids or e is None or not e.calendario_dias:
            return {}
        hoje = dia_local(agora, self._fuso)
        de = hoje - timedelta(days=max(0, int(self._cfg.dias_correcao or 0)) + 2)
        ate = hoje + timedelta(days=1)
        saida: dict[int, dict[date, bool]] = {}
        cursor = self._conexao.cursor()
        try:
            marcas = ", ".join("?" for _ in ids)
            cursor.execute(f"SELECT [calendario_id], [data], [tipo] FROM {e.t['calendario_dias']} "
                           f"WHERE [calendario_id] IN ({marcas}) AND [data] >= ? AND [data] <= ?", *ids, de, ate)
            for cal, data, tipo in cursor.fetchall() or []:
                util = TIPOS_UTEIS.get(str(tipo or "").strip().lower())
                if util is None or data is None:
                    continue
                dia = data.date() if isinstance(data, datetime) else (
                    data if isinstance(data, date) else date.fromisoformat(str(data)[:10]))
                saida.setdefault(int(cal), {})[dia] = util
        except Exception as exc:  # noqa: BLE001
            if _erro_de_conexao(exc):
                raise
            logger.info("Exceções do calendário indisponíveis: %s", exc)
            return {}
        finally:
            _fechar(cursor)
            self._commit_silencioso()
        return saida

    def _completar_linha(self, linha: dict, m: Maquina, jornada: Jornada, diag: str | None, agora: int) -> None:
        linha["diag"] = diag
        if jornada.vazia:
            linha["turno"], linha["jornada"] = "sem jornada", None
        else:
            linha["turno"] = "dentro" if intervalos_turno(jornada, agora, agora, self._fuso) else "fora"
            linha["jornada"] = texto_jornada(jornada.horarios)
        if diag == DIAG_SEM_MQTT:
            extra = "Sem MQTT conectado: diagnóstico do aparelho não enviado."
            linha["aviso"] = f"{linha['aviso']} {extra}" if linha.get("aviso") else extra

    def _ciclo(self, inicio: float) -> dict:
        agora = int(inicio)
        falha, dados = self._preparar()
        if falha is not None:
            return falha
        legendas, todas, maquinas = dados
        total = {"criados": 0, "alterados": 0, "excluidos": 0}
        linhas: list[dict] = []
        falhas = 0
        processadas = 0
        conexao_caiu = False
        elegiveis: list[tuple[Maquina, dict]] = []
        for m in maquinas:
            linha = self._linha_maquina(m)
            linhas.append(linha)
            if self._elegivel(m, linha):
                elegiveis.append((m, linha))
        diag = self._diagnostico([m for m, _l in elegiveis])
        try:
            excecoes = self._ler_excecoes([m for m, _l in elegiveis], agora)
        except Exception as exc:  # noqa: BLE001 - conexão caiu
            self._rollback()
            self._fechar_conexao()
            return self._falha_geral(exc)
        for m, linha in elegiveis:
            jornada = self._jornada(m, excecoes)
            estado_diag = self._estado_diag(m, diag)
            if conexao_caiu:
                linha["aviso"] = "Não processada: conexão com o SQL Server perdida neste ciclo."
                continue
            processadas += 1
            try:
                contagem, avisos = self._processar(m, linha, legendas, todas, agora, jornada,
                                                   mudo=estado_diag == DIAG_SEM_RESPOSTA)
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
            finally:
                self._completar_linha(linha, m, jornada, estado_diag, agora)
            for k in total:
                total[k] += contagem[k]
            for nivel, aviso in avisos:
                self._logar(nivel, aviso)
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

    # -- recalcular o dia de uma máquina -------------------------------------------------------------------

    def recalcular(self, maquina_id: int, dia: str) -> None:
        """Não bloqueia: põe na fila da thread do ciclo. Dia inválido ou fora do limite: responde falha já."""
        try:
            alvo = date.fromisoformat(str(dia).strip())
            if len(str(dia).strip()) != 10:
                raise ValueError
        except ValueError:
            self._responder_recalculo(maquina_id, dia, False, f"Dia inválido: {dia!r} (use AAAA-MM-DD).")
            return
        hoje = dia_local(int(self._relogio()), self._fuso)
        dias = max(0, int(self._cfg.dias_correcao or 0))
        limite = hoje - timedelta(days=dias)
        if alvo > hoje or alvo < limite:
            self._responder_recalculo(maquina_id, dia, False, f"Escolha um dia de {limite:%d/%m} a {hoje:%d/%m} "
                                                              f"(até {dias} dias atrás).")
            return
        if not self._cfg.habilitado or self._parado:
            self._responder_recalculo(maquina_id, dia, False, "Apontamentos reais desligados.")
            return
        ini = int(datetime(alvo.year, alvo.month, alvo.day, tzinfo=self._fuso).timestamp())
        with self._trava:
            self._recalculos.append((int(maquina_id), str(dia).strip(), ini))
        self.iniciar()
        self._evento.set()

    def _responder_recalculo(self, maquina_id: int, dia: str, ok: bool, mensagem: str) -> None:
        try:
            self._ao_recalculo(int(maquina_id), str(dia), bool(ok), mensagem)
        except Exception:  # noqa: BLE001
            logger.exception("Falha no callback de recálculo")

    def executar_recalculo(self, maquina_id: int, dia: str, ini: int) -> bool:
        """Bloqueante (thread do ciclo ou testes): refaz a máquina desde ``ini`` (00:00 local de ``dia``)."""
        with self._ciclo_trava:
            with self._trava:
                trocar, self._trocou_sql = self._trocou_sql, False
            if trocar:
                self._fechar_conexao()
            try:
                ok, mensagem = self._recalcular(int(maquina_id), ini)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Falha inesperada no recálculo")
                ok, mensagem = False, f"Falha inesperada: {exc}"
            nivel = "INFO" if ok else "AVISO"
            self._logar(nivel, f"Apontamentos reais: recálculo da máquina {maquina_id} desde "
                               f"{datetime.fromtimestamp(ini, self._fuso):%d/%m} — {mensagem}")
            self._responder_recalculo(maquina_id, dia, ok, mensagem)
            return ok

    def _recalcular(self, maquina_id: int, ini: int) -> tuple[bool, str]:
        agora = int(self._relogio())
        falha, dados = self._preparar()
        if falha is not None:
            return False, falha["mensagem"]
        legendas, todas, maquinas = dados
        m = next((x for x in maquinas if x.id == maquina_id), None)
        if m is None:
            return False, "Máquina não encontrada entre as máquinas ativas com dispositivo IoT de produção."
        linha = self._linha_maquina(m)
        if not self._elegivel(m, linha):
            return False, linha.get("aviso") or "Máquina sem canal IoT de produção com dispositivo ativo."
        diag = self._diagnostico([m])
        estado_diag = self._estado_diag(m, diag)
        try:
            jornada = self._jornada(m, self._ler_excecoes([m], agora))
            try:
                contagem, avisos = self._processar(m, linha, legendas, todas, agora, jornada,
                                                   mudo=estado_diag == DIAG_SEM_RESPOSTA, ini_forcado=ini)
            finally:
                self._completar_linha(linha, m, jornada, estado_diag, agora)
        except Exception as exc:  # noqa: BLE001
            self._rollback()
            resumo = sql_conexao.traduzir_erro(exc).replace("\n", " ")
            self._ao_erro(f"Falha no recálculo dos apontamentos reais da máquina {self._nome(m)}; nada foi "
                          "gravado.", resumo)
            if _erro_de_conexao(exc):
                self._fechar_conexao()
            return False, f"Falha: {resumo}"
        for nivel, aviso in avisos:
            self._logar(nivel, aviso)
        self._mesclar_resultado(linha, agora)
        manuais = contagem.get("manuais", 0)
        return True, (f"Recalculado desde {datetime.fromtimestamp(ini, self._fuso):%d/%m}: {contagem['criados']} "
                      f"criados, {contagem['alterados']} atualizados, {contagem['excluidos']} excluídos "
                      f"({manuais} {'manual apagado' if manuais == 1 else 'manuais apagados'}).")

    def _mesclar_resultado(self, linha: dict, agora: int) -> None:
        """Emite o último resultado do ciclo com a linha desta máquina atualizada (ou só ela, sem ciclo antes)."""
        base = dict(self._ultimo_resultado) if self._ultimo_resultado else {
            **self._resultado_vazio(True, "Recálculo de uma máquina."), "quando": iso_local(agora)}
        maquinas = [x for x in base.get("maquinas", []) if x.get("maquina_id") != linha["maquina_id"]]
        base["maquinas"] = sorted(maquinas + [linha], key=lambda x: (str(x.get("nome") or ""), x.get("maquina_id")))
        if self._ultimo_resultado:
            ordem = [x.get("maquina_id") for x in self._ultimo_resultado.get("maquinas", [])]
            if linha["maquina_id"] in ordem:
                base["maquinas"] = [linha if x.get("maquina_id") == linha["maquina_id"] else x
                                    for x in self._ultimo_resultado.get("maquinas", [])]
        self._ultimo_resultado = base
        self._emitir(base)

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
        codigo_97 = str(self._cfg.legenda_sem_iot or "").strip()
        ids_97 = [l.id for l in todas.values() if l.ativo and codigo_97 and l.codigo == codigo_97]
        sem_iot = min(ids_97) if ids_97 else None
        if sem_iot is None:
            if not self._aviso_sem_iot:
                self._aviso_sem_iot = True
                self._logar("AVISO", f"Apontamentos reais: legenda {codigo_97 or '(sem código)'} (Sem apontamento "
                                     "IOT) não cadastrada ou inativa no portal OEE; aparelho sem resposta ao "
                                     "diagnóstico continua como parada (99/98) até ela ser cadastrada.")
        else:
            self._aviso_sem_iot = False
        codigo_96 = str(self._cfg.legenda_fora_turno or "").strip()
        ids_96 = [l.id for l in todas.values() if l.ativo and codigo_96 and l.codigo == codigo_96]
        fora_turno = min(ids_96) if ids_96 else None
        if fora_turno is None:
            if not self._aviso_fora_turno:
                self._aviso_fora_turno = True
                self._logar("AVISO", f"Apontamentos reais: legenda {codigo_96 or '(sem código)'} (Fora de turno) não "
                                     "cadastrada ou inativa no portal OEE; paradas fora do horário de trabalho "
                                     "continuam 99/98/97 até ela ser cadastrada.")
        else:
            self._aviso_fora_turno = False
        return LegendasIot(achadas["produzindo"], achadas["micro"], achadas["macro"], sem_iot, fora_turno), todas

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
                "aviso": None if m.canais else "Sem canal IoT de produção com dispositivo ativo.",
                **self._turno_sem_calendario(m)}

    def _turno_sem_calendario(self, m: Maquina) -> dict:
        """diag/turno/jornada iniciais da linha (só pelos dias da semana); o ciclo refaz com o calendário."""
        jornada = montar_jornada(m.horarios_semana, m.dias_semana, m.jornada_inicio, m.jornada_fim)
        agora = int(self._relogio())
        if jornada.vazia:
            return {"diag": None, "turno": "sem jornada", "jornada": None}
        dentro = intervalos_turno(jornada, agora, agora, self._fuso)
        return {"diag": None, "turno": "dentro" if dentro else "fora", "jornada": texto_jornada(jornada.horarios)}

    def _processar(self, m: Maquina, linha: dict, legendas: LegendasIot, todas: dict[int, Legenda], agora: int,
                   jornada: Jornada | None = None, mudo: bool = False, ini_forcado: int | None = None
                   ) -> tuple[dict, list[tuple[str, str]]]:
        """Uma máquina, numa transação. Devolve (contagem, [(nível, mensagem)] para o log depois do commit).

        ``jornada``: horário de trabalho + calendário (96 fora do turno); ``mudo``: o aparelho não respondeu ao
        diagnóstico (parada em aberto vira 97); ``ini_forcado``: recálculo desde 00:00 local de um dia.
        """
        e = self._esquema
        A = e.t["apontamentos_estado"]
        hoje = inicio_do_dia(agora, self._fuso)
        ontem = inicio_do_dia(agora, self._fuso, -1)
        jornada = jornada or Jornada({})
        cur = self._conexao.cursor()
        try:
            if ini_forcado is not None:
                ini = int(ini_forcado)
            else:
                cur.execute(f"SELECT TOP 1 1 FROM {A} WHERE [maquina_id] = ? AND [inicio] >= ? AND [inicio] < ? "
                            "AND LEFT([observacao], 5) = ?", m.id, utc_sql(ontem), utc_sql(hoje), MARCA)
                ini = ontem if cur.fetchone() is not None else hoje

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
                return {"criados": 0, "alterados": 0, "excluidos": 0, "manuais": 0}, []
            logs: list[tuple[str, str]] = []
            if legendas.sem_iot is not None and ini_forcado is None:
                retro = self._correcao_retroativa(cur, m, legendas.sem_iot, list(por_device), ini, agora)
                if retro is not None:
                    inicio_97, device = retro
                    ini = inicio_do_dia(inicio_97, self._fuso)
                    logs.append(("INFO", f"Apontamentos reais: dados retroativos de {device} — recalculando a "
                                         f"máquina {self._nome(m)} desde "
                                         f"{datetime.fromtimestamp(ini, self._fuso):%d/%m}."))

            # leituras: por dispositivo, só as colunas dos canais da máquina (a última linha, mesmo com valores
            # NULL, é a última notícia do aparelho: início do 97 quando ele não responde ao diagnóstico)
            desde = ini - m.macro - MARGEM_LEITURAS
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

            # falta de linhas na IOT001 não gera 97 (regra revista em 13/11/2026): só o diagnóstico sem resposta
            segs = segmentos_continuos(prods, ini, agora, m.micro, m.macro)
            if mudo and legendas.sem_iot is not None:
                segs = marcar_sem_resposta(segs, prods, ini, ultima_leitura)
            if legendas.fora_turno is not None and not jornada.vazia:
                segs = recortar_fora_turno(segs, intervalos_turno(jornada, ini, agora, self._fuso))
            desejados = dividir_por_dia(segs, self._fuso)
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
            logs.extend(("AVISO", a) for a in avisos)

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
        return contagem, logs

    def _correcao_retroativa(self, cur, m: Maquina, legenda_97: int, devices: list[str], ini: int, agora: int
                             ) -> tuple[int, str] | None:
        """97 do integrador mais antigo dos últimos ``dias_correcao`` dias (e anterior a ``ini``) que já tem leitura
        na IOT001 dentro dele (``ts_unix > inicio`` e ``<= fim``; fim NULL = agora). Devolve (inicio, device).

        Lê os candidatos (poucos: um por queda e por dia) e testa cada um com ``SELECT TOP 1`` por device, usando o
        índice (device_id, ts_unix) da IOT001. Sem leitura nova, nada muda (a janela não volta à toa).
        """
        dias = max(0, int(self._cfg.dias_correcao or 0))
        limite = agora - dias * 86400
        if not dias or limite >= ini or not devices:
            return None
        e = self._esquema
        cur.execute(f"SELECT [inicio], [fim] FROM {e.t['apontamentos_estado']} WHERE [maquina_id] = ? "
                    "AND [legenda_id] = ? AND LEFT([observacao], 5) = ? AND [inicio] >= ? AND [inicio] < ? "
                    "ORDER BY [inicio], [id]", m.id, legenda_97, MARCA, utc_sql(limite), utc_sql(ini))
        candidatos = [(epoch_de(r[0]), epoch_de(r[1])) for r in cur.fetchall() or []]
        for inicio, fim in candidatos:
            fim = agora if fim is None else min(fim, agora)
            if fim <= inicio:
                continue
            for device in devices:
                cur.execute(f"SELECT TOP 1 1 FROM {e.iot} WHERE [device_id] = CAST(? AS varchar(100)) "
                            "AND [ts_unix] > ? AND [ts_unix] <= ?", device, inicio, fim)
                if cur.fetchone() is not None:
                    return inicio, device
        return None

    def _aplicar(self, cur, m: Maquina, plano: Plano, todas: dict[int, Legenda], agora: int
                 ) -> tuple[dict, list[str]]:
        A = self._esquema.t["apontamentos_estado"]
        agora_sql = utc_sql(agora)
        contagem = {"criados": 0, "alterados": 0, "excluidos": 0, "manuais": 0}
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
                contagem["manuais"] += 1
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
