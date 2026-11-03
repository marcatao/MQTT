"""Liga MQTT → fila → mapeamento (dbo.IOT001) → sink e expõe eventos (status, log, contadores) por callbacks.

Threads:
- rede do paho: só enfileira a mensagem (``queue.Queue`` limitada) e retorna;
- processamento (uma por conexão): mapeamento, lote e gravação; manda os contadores no máx. ~5x/s;
- controle (``ThreadPoolExecutor`` de 1 worker): executa conectar/desconectar em ordem, sem bloquear quem chama.

Os callbacks são chamados de qualquer uma dessas threads; o ``bridge`` os transforma em Signals do Qt.

Amostragem (``[gravacao] intervalo_gravacao_segundos``, padrão 60; 0 desliga): os ciclos são alinhados ao relógio
local (14:31:00, 14:32:00…). A thread de processamento guarda, por ``device_id``, a leitura **completa** mais recente
da janela (pela ``recebido_em``; empate: a última que chegou) e, na virada, manda só essas ao sink e descarrega. As
demais são descartadas (não são erro nem gravadas). Leituras incompletas vão na hora para o sink (contingência). Ao
desconectar/encerrar, o que estiver pendente da janela atual é gravado. Com 0, tudo vai ao sink como antes
(lote por ``lote_max``/``lote_segundos``).
"""
from __future__ import annotations

import logging
import queue
import threading
import math
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from app.config import Config, SqlServerConfig, pasta_base
from app.core.mqtt_client import ClienteMqtt, validar_parametros
from app.core.mapeamento import LinhaIot, mapear
from app.core.parser import trecho
from app.core.sink_sqlserver import Sink, criar_sink

logger = logging.getLogger("app.core")

_NIVEIS = {"INFO": logging.INFO, "AVISO": logging.WARNING, "ERRO": logging.ERROR}

AoStatus = Callable[[str, str], None]
AoLog = Callable[[str, str], None]
AoContadores = Callable[[int, int, int], None]
FabricaSink = Callable[[SqlServerConfig, "Config", Callable[[str, str], None]], Sink]
Relogio = Callable[[], float]  # epoch em segundos (time.time)

_FIM = object()  # sentinela da fila


def _nada(*_args) -> None:
    pass


def proximo_ciclo(agora: float, intervalo: float) -> float:
    """Próximo instante (epoch, estritamente depois de ``agora``) alinhado ao relógio **local**.

    Múltiplos de ``intervalo`` contados na hora local: 60 → hh:mm:00; 3600 → hh:00:00; 86400 → meia-noite.
    """
    deslocamento = datetime.fromtimestamp(agora).astimezone().utcoffset()
    off = deslocamento.total_seconds() if deslocamento is not None else 0.0
    local = agora + off
    return (math.floor(local / intervalo) + 1) * intervalo - off


def iso_local(t: float) -> str:
    """Epoch → "AAAA-MM-DDTHH:MM:SS" na hora local."""
    return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def _plural(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


class _Amostragem:
    """Leitura completa mais recente por device_id dentro da janela atual."""

    def __init__(self) -> None:
        self.pendentes: dict[str, LinhaIot] = {}
        self.recebidas = 0

    def adicionar(self, linhas: list[LinhaIot]) -> None:
        for linha in linhas:
            self.recebidas += 1
            atual = self.pendentes.get(linha.device_id)
            if atual is None or linha.recebido_em >= atual.recebido_em:  # empate: a última que chegou vence
                self.pendentes[linha.device_id] = linha

    def retirar(self) -> tuple[list[LinhaIot], int]:
        linhas, recebidas = list(self.pendentes.values()), self.recebidas
        self.pendentes, self.recebidas = {}, 0
        return linhas, recebidas


class Pipeline:
    TAMANHO_FILA = 20000
    INTERVALO_CONTADORES = 0.2      # s (≈5 por segundo)
    LOGS_POR_SEGUNDO = 20           # linhas "Mensagem de ..." por segundo; o excedente vira um resumo
    INTERVALO_ULTIMA_LEITURA = 5.0  # s: no máx. 1 aviso de "última leitura" a cada 5 s

    def __init__(
        self,
        config: Config,
        ao_status: AoStatus = _nada,
        ao_log: AoLog = _nada,
        ao_contadores: AoContadores = _nada,
        fabrica_sink: FabricaSink | None = None,
        pasta: Path | None = None,
        sessao_persistente: bool = True,
        ao_proxima_gravacao: Callable[[float], None] = _nada,
        ao_gravacao_ok: Callable[[str], None] = _nada,
        ao_leitura: Callable[[str], None] = _nada,
        relogio: Relogio = time.time,
        intervalo_gravacao: float | None = None,
        fabrica_cliente: Callable[..., ClienteMqtt] | None = None,
    ) -> None:
        """``relogio``, ``intervalo_gravacao`` (sobrepõe a config) e ``fabrica_cliente`` existem para os testes."""
        self.config = config
        self._ao_status = ao_status
        self._ao_log = ao_log
        self._ao_contadores = ao_contadores
        self._ao_proxima_gravacao = ao_proxima_gravacao
        self._ao_gravacao_ok = ao_gravacao_ok
        self._ao_leitura = ao_leitura
        self._relogio = relogio
        self._intervalo_fixo = intervalo_gravacao
        self._fabrica_cliente = fabrica_cliente or ClienteMqtt
        self._ultima_leitura_t: float | None = None
        self._fabrica_sink = fabrica_sink or self._sink_padrao
        self._pasta = pasta or pasta_base()
        self._sessao_persistente = sessao_persistente

        self._controle = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mqtt-controle")
        self._trava = threading.Lock()
        self._recebidas = 0
        self._gravados = 0
        self._erros = 0
        self._avisou_sem_sql = False
        self._sql_pendente: SqlServerConfig | None = None

        self._cliente: ClienteMqtt | None = None
        self._fila: queue.Queue | None = None
        self._processador: threading.Thread | None = None
        self._assinatura = ""
        self._encerrado = False
        self._amostrando = False

    # ------------------------------------------------------------------------------ API pública

    def conectar(self, broker: str, porta: int, topico: str, qos: int) -> Future:
        """Não bloqueia. Se já houver conexão, ela é encerrada antes (gravando o último lote)."""
        return self._controle.submit(self._conectar, broker, porta, topico, qos)

    def desconectar(self) -> Future:
        """Não bloqueia: para de receber, esvazia a fila, grava o último lote e desconecta."""
        return self._controle.submit(self._parar)

    def definir_sql(self, sql: SqlServerConfig) -> None:
        """Troca a configuração do SQL Server; se estiver conectado, vale a partir do próximo lote."""
        self.config.sqlserver = sql
        with self._trava:
            self._sql_pendente = sql

    def encerrar(self, timeout: float = 5.0) -> bool:
        """Bloqueia até ``timeout`` segundos. Devolve True se terminou limpo."""
        if self._encerrado:
            return True
        self._encerrado = True
        fut = self._controle.submit(self._parar)
        try:
            fut.result(timeout=timeout)
            ok = True
        except Exception:  # noqa: BLE001 - timeout ou erro ao parar
            ok = False
            self._log("AVISO", "Encerramento demorou mais que o esperado; alguns registros podem ter ficado "
                               "só na memória.")
        self._controle.shutdown(wait=False, cancel_futures=True)
        return ok

    @property
    def contadores(self) -> tuple[int, int, int]:
        with self._trava:
            return self._recebidas, self._gravados, self._erros

    # ------------------------------------------------------------------------------ internos

    def _log(self, nivel: str, msg: str) -> None:
        logger.log(_NIVEIS.get(nivel, logging.INFO), msg)
        try:
            self._ao_log(nivel, msg)
        except Exception:  # noqa: BLE001
            logger.exception("Falha no callback de log")

    def _status(self, estado: str, detalhe: str) -> None:
        try:
            self._ao_status(estado, detalhe)
        except Exception:  # noqa: BLE001
            logger.exception("Falha no callback de status")

    def _somar(self, recebidas: int = 0, gravados: int = 0, erros: int = 0) -> None:
        with self._trava:
            self._recebidas += recebidas
            self._gravados += gravados
            self._erros += erros

    def _avisar(self, callback, *args) -> None:
        try:
            callback(*args)
        except Exception:  # noqa: BLE001
            logger.exception("Falha em callback do pipeline")

    def _intervalo(self) -> float:
        valor = self._intervalo_fixo
        if valor is None:
            valor = getattr(self.config.gravacao, "intervalo_gravacao_segundos", 0)
        try:
            valor = float(valor)
        except (TypeError, ValueError):
            valor = 0.0
        return valor if valor > 0 else 0.0

    def _sink_padrao(self, sql: SqlServerConfig, config: Config, logar) -> Sink:
        return criar_sink(sql, config.gravacao, logar, self._pasta)

    def _novo_sink(self, sql: SqlServerConfig) -> Sink:
        sink = self._fabrica_sink(sql, self.config, self._log)
        if self._amostrando and hasattr(sink, "logar_sucesso"):
            sink.logar_sucesso = False  # um log por ciclo (no pipeline) em vez de um por lote
        if not sql.habilitado:
            if not self._avisou_sem_sql:
                self._avisou_sem_sql = True
                self._log("INFO", f"SQL Server não configurado: os registros vão para {sink.descricao}, "
                                  "em JSON Lines, para gravar depois.")
        else:
            self._log("INFO", f"Destino dos registros: {sink.descricao}")
        return sink

    def _conectar(self, broker: str, porta: int, topico: str, qos: int) -> None:
        self._parar(silencioso=True)
        try:
            broker, porta, topico, qos = validar_parametros(broker, porta, topico, qos)
            with self._trava:
                self._sql_pendente = None
            intervalo = self._intervalo()
            self._amostrando = intervalo > 0
            sink = self._novo_sink(self.config.sqlserver)
        except Exception as exc:  # noqa: BLE001 - ValueError de validação, tabela inválida etc.
            self._log("ERRO", str(exc))
            self._status("erro", str(exc))
            return

        self._assinatura = topico
        self._ultima_leitura_t = None
        proximo = proximo_ciclo(self._relogio(), intervalo) if intervalo > 0 else None
        if intervalo > 0:
            self._log("INFO", f"Amostragem: 1 leitura por dispositivo a cada {intervalo:g} s "
                              f"(próxima gravação às {datetime.fromtimestamp(proximo):%H:%M:%S}).")
        self._fila = queue.Queue(maxsize=self.TAMANHO_FILA)
        self._processador = threading.Thread(target=self._processar, args=(self._fila, sink, intervalo, proximo),
                                             name="mqtt-processamento", daemon=True)
        self._processador.start()
        if proximo is not None:
            self._avisar(self._ao_proxima_gravacao, proximo)
        try:
            self._cliente = self._fabrica_cliente(
                broker, porta, topico, qos,
                ao_mensagem=self._ao_mensagem,
                ao_status=self._status,
                logar=self._log,
                usuario=self.config.mqtt.usuario,
                senha=self.config.mqtt.senha,
                sessao_persistente=self._sessao_persistente,
            )
            self._cliente.iniciar()
        except Exception as exc:  # noqa: BLE001
            self._log("ERRO", f"Não foi possível iniciar a conexão com {broker}:{porta}: {exc}")
            self._status("erro", str(exc))
            self._cliente = None
            self._finalizar_processador()

    def _parar(self, silencioso: bool = False) -> None:
        cliente, self._cliente = self._cliente, None
        if cliente is None and self._processador is None:
            if not silencioso:
                self._status("desconectado", "Desconectado")
            return
        if cliente is not None:
            try:
                cliente.parar()
            except Exception as exc:  # noqa: BLE001
                self._log("AVISO", f"Erro ao desconectar: {exc}")
        self._finalizar_processador()
        self._status("desconectado", "Desconectado")

    def _finalizar_processador(self) -> None:
        fila, proc = self._fila, self._processador
        self._fila = self._processador = None
        if fila is not None:
            fila.put(_FIM)  # depois das mensagens já enfileiradas: elas são processadas antes
        if proc is not None:
            proc.join()

    # -- thread de rede do paho: só enfileira -------------------------------------------------------

    def _ao_mensagem(self, topico: str, payload: bytes, qos: int, retida: bool) -> None:
        fila = self._fila
        if fila is None:
            return
        self._somar(recebidas=1)
        agora = self._relogio()
        ultima = self._ultima_leitura_t
        if ultima is None or agora - ultima >= self.INTERVALO_ULTIMA_LEITURA or agora < ultima:
            self._ultima_leitura_t = agora
            self._avisar(self._ao_leitura, iso_local(agora))
        try:
            # bloqueia a rede um pouco se a fila encher (contrapressão) em vez de perder a mensagem
            fila.put((topico, payload, datetime.fromtimestamp(agora, timezone.utc), retida, agora), timeout=5)
        except queue.Full:
            self._somar(erros=1)
            self._log("ERRO", f"Fila de processamento cheia: mensagem de {topico} descartada "
                              f"({trecho(payload, 40)})")

    # -- thread de processamento ---------------------------------------------------------------------

    def _processar(self, fila: queue.Queue, sink: Sink, intervalo: float = 0.0,
                   proximo: float | None = None) -> None:
        amostra = _Amostragem() if proximo is not None else None
        ultimo_contador = 0.0
        enviado = (-1, -1, -1)
        janela_inicio = time.monotonic()
        logs_na_janela = 0
        suprimidos = 0
        terminar = False

        while True:
            try:
                item = fila.get(timeout=0.1)
            except queue.Empty:
                item = None

            if item is _FIM:
                terminar = True
            elif item is not None:
                topico, payload, recebido_em, retida, t_receb = item
                if amostra is not None and t_receb >= proximo:
                    # chegou depois da virada: fecha a janela anterior antes de contar esta leitura
                    proximo = self._ciclo(sink, amostra, intervalo, proximo)
                res = mapear(topico, payload, recebido_em, self._assinatura, self.config.mapeamento)
                agora = time.monotonic()
                if agora - janela_inicio >= 1.0:
                    if suprimidos:
                        self._log("INFO", f"… e mais {suprimidos} mensagens no último segundo")
                    janela_inicio, logs_na_janela, suprimidos = agora, 0, 0
                if res.ok:
                    rejeitadas = res.rejeitadas
                    if rejeitadas:  # sem pid/device_id/ts_unix: vão para a contingência, contam como erro
                        self._somar(erros=len(rejeitadas))
                    if logs_na_janela < self.LOGS_POR_SEGUNDO:
                        logs_na_janela += 1
                        n = len(res.linhas) - len(rejeitadas)
                        extra = " (retida)" if retida else ""
                        if n:
                            self._log("INFO", f"Mensagem de {topico}{extra}: {res.formato}, "
                                              f"{n} leitura{'s' if n != 1 else ''}")
                        for linha in rejeitadas:
                            self._log("ERRO", f"Mensagem de {topico}{extra} ({res.formato}) não vai para o banco: "
                                              f"{linha.motivo}. Guardada na contingência.")
                        for aviso in res.avisos:
                            self._log("AVISO", f"{topico}: {aviso}")
                    else:
                        suprimidos += 1
                    if amostra is None:
                        sink.adicionar(res.linhas)
                    else:
                        if rejeitadas:
                            sink.adicionar(rejeitadas)  # incompletas: contingência na hora, como sem amostragem
                        amostra.adicionar(res.completas)
                else:
                    self._somar(erros=1)
                    self._log("ERRO" if res.formato != "vazio" else "AVISO",
                              f"Mensagem de {topico} ignorada: {res.erro}")

            sink = self._trocar_sink_se_preciso(sink)
            if amostra is not None and not terminar:
                agora_rel = self._relogio()
                if agora_rel >= proximo:
                    proximo = self._ciclo(sink, amostra, intervalo, proximo)
                elif agora_rel < proximo - intervalo - 1:
                    # relógio do PC voltou para trás: realinha sem esperar até o horário antigo
                    proximo = proximo_ciclo(agora_rel, intervalo)
                    self._avisar(self._ao_proxima_gravacao, proximo)
            if terminar:
                if amostra is not None:
                    self._ciclo(sink, amostra, intervalo, None, fechar=True)
                else:
                    self._descarregar(sink, fechar=True)
            elif sink.deve_descarregar():
                self._descarregar(sink)

            agora = time.monotonic()
            if terminar or agora - ultimo_contador >= self.INTERVALO_CONTADORES:
                atual = self.contadores
                if atual != enviado:
                    enviado = atual
                    try:
                        self._ao_contadores(*atual)
                    except Exception:  # noqa: BLE001
                        logger.exception("Falha no callback de contadores")
                ultimo_contador = agora
            if terminar:
                if suprimidos:
                    self._log("INFO", f"… e mais {suprimidos} mensagens no último segundo")
                return

    def _ciclo(self, sink: Sink, amostra: _Amostragem, intervalo: float, momento: float | None,
               fechar: bool = False) -> float | None:
        """Grava a amostra da janela. ``momento`` = virada (epoch); None = desconexão/encerramento.

        Devolve o próximo ciclo (None no encerramento) e avisa ``ao_proxima_gravacao``.
        """
        linhas, recebidas = amostra.retirar()
        if momento is not None:
            rotulo = f"{datetime.fromtimestamp(momento):%H:%M}"
        else:
            rotulo = f"{datetime.fromtimestamp(self._relogio()):%H:%M:%S} (ao desconectar)"
        if linhas:
            sink.adicionar(linhas)
        n, n_sql = (self._descarregar(sink, fechar=True) if fechar
                    else self._descarregar(sink) if linhas else (0, 0))
        if linhas:
            total = len(linhas)
            if n_sql >= total:
                destino = f"{_plural(total, 'leitura gravada', 'leituras gravadas')} no SQL Server"
            elif n_sql > 0:
                destino = (f"{_plural(n_sql, 'leitura gravada', 'leituras gravadas')} no SQL Server e "
                           f"{total - n_sql} na contingência")
            elif n > 0:
                destino = f"{_plural(total, 'leitura guardada', 'leituras guardadas')} na contingência"
            else:
                destino = f"{_plural(total, 'leitura não gravada', 'leituras não gravadas')} (ver erro acima)"
            descartadas = recebidas - total
            self._log("INFO", f"Gravação {rotulo}: {destino} ({_plural(recebidas, 'recebida', 'recebidas')}, "
                              f"{_plural(descartadas, 'descartada', 'descartadas')} pela amostragem)")
        elif momento is not None:
            self._log("INFO", f"Gravação {rotulo}: nenhuma leitura completa recebida no período")
        if momento is None:
            return None
        proximo = proximo_ciclo(max(self._relogio(), momento), intervalo)
        self._avisar(self._ao_proxima_gravacao, proximo)
        return proximo

    def _descarregar(self, sink: Sink, fechar: bool = False) -> tuple[int, int]:
        """Devolve (persistidos no SQL ou na contingência, gravados no SQL Server com commit)."""
        antes = getattr(sink, "gravados_sql", 0)
        n = 0
        try:
            n = sink.fechar() if fechar else sink.descarregar()
            self._somar(gravados=n)
        except Exception as exc:  # noqa: BLE001 - contingência sem disco, por exemplo
            self._somar(erros=1)
            self._log("ERRO", f"Falha ao gravar registros: {exc}")
        falhas = getattr(sink, "falhas", None)
        if falhas is not None:
            self._somar(erros=falhas())
        n_sql = getattr(sink, "gravados_sql", 0) - antes
        if n_sql > 0:
            self._avisar(self._ao_gravacao_ok, iso_local(self._relogio()))
        return n, n_sql

    def _trocar_sink_se_preciso(self, sink: Sink) -> Sink:
        with self._trava:
            novo, self._sql_pendente = self._sql_pendente, None
        if novo is None:
            return sink
        try:
            substituto = self._novo_sink(replace(novo))
        except Exception as exc:  # noqa: BLE001 - tabela inválida
            self._log("ERRO", f"Configuração do SQL Server rejeitada: {exc}")
            return sink
        self._descarregar(sink, fechar=True)
        return substituto
