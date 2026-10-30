"""Liga MQTT → fila → parser → sink e expõe eventos (status, log, contadores) por callbacks.

Threads:
- rede do paho: só enfileira a mensagem (``queue.Queue`` limitada) e retorna;
- processamento (uma por conexão): parsing, lote e gravação; manda os contadores no máx. ~5x/s;
- controle (``ThreadPoolExecutor`` de 1 worker): executa conectar/desconectar em ordem, sem bloquear quem chama.

Os callbacks são chamados de qualquer uma dessas threads; o ``bridge`` os transforma em Signals do Qt.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Callable

from app.config import Config, SqlServerConfig, pasta_base
from app.core.mqtt_client import ClienteMqtt, validar_parametros
from app.core.parser import analisar, trecho
from app.core.sink_sqlserver import Sink, criar_sink

logger = logging.getLogger("app.core")

_NIVEIS = {"INFO": logging.INFO, "AVISO": logging.WARNING, "ERRO": logging.ERROR}

AoStatus = Callable[[str, str], None]
AoLog = Callable[[str, str], None]
AoContadores = Callable[[int, int, int], None]
FabricaSink = Callable[[SqlServerConfig, "Config", Callable[[str, str], None]], Sink]

_FIM = object()  # sentinela da fila


def _nada(*_args) -> None:
    pass


class Pipeline:
    TAMANHO_FILA = 20000
    INTERVALO_CONTADORES = 0.2      # s (≈5 por segundo)
    LOGS_POR_SEGUNDO = 20           # linhas "Mensagem de ..." por segundo; o excedente vira um resumo

    def __init__(
        self,
        config: Config,
        ao_status: AoStatus = _nada,
        ao_log: AoLog = _nada,
        ao_contadores: AoContadores = _nada,
        fabrica_sink: FabricaSink | None = None,
        pasta: Path | None = None,
        sessao_persistente: bool = True,
    ) -> None:
        self.config = config
        self._ao_status = ao_status
        self._ao_log = ao_log
        self._ao_contadores = ao_contadores
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

    def _sink_padrao(self, sql: SqlServerConfig, config: Config, logar) -> Sink:
        return criar_sink(sql, config.gravacao, logar, self._pasta)

    def _novo_sink(self, sql: SqlServerConfig) -> Sink:
        sink = self._fabrica_sink(sql, self.config, self._log)
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
            sink = self._novo_sink(self.config.sqlserver)
        except Exception as exc:  # noqa: BLE001 - ValueError de validação, tabela inválida etc.
            self._log("ERRO", str(exc))
            self._status("erro", str(exc))
            return

        self._assinatura = topico
        self._fila = queue.Queue(maxsize=self.TAMANHO_FILA)
        self._processador = threading.Thread(target=self._processar, args=(self._fila, sink),
                                             name="mqtt-processamento", daemon=True)
        self._processador.start()
        try:
            self._cliente = ClienteMqtt(
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
        try:
            # bloqueia a rede um pouco se a fila encher (contrapressão) em vez de perder a mensagem
            fila.put((topico, payload, datetime.now(), retida), timeout=5)
        except queue.Full:
            self._somar(erros=1)
            self._log("ERRO", f"Fila de processamento cheia: mensagem de {topico} descartada "
                              f"({trecho(payload, 40)})")

    # -- thread de processamento ---------------------------------------------------------------------

    def _processar(self, fila: queue.Queue, sink: Sink) -> None:
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
                topico, payload, recebido_em, retida = item
                res = analisar(topico, payload, recebido_em, self._assinatura)
                agora = time.monotonic()
                if agora - janela_inicio >= 1.0:
                    if suprimidos:
                        self._log("INFO", f"… e mais {suprimidos} mensagens no último segundo")
                    janela_inicio, logs_na_janela, suprimidos = agora, 0, 0
                if res.ok:
                    if logs_na_janela < self.LOGS_POR_SEGUNDO:
                        logs_na_janela += 1
                        n = len(res.registros)
                        extra = " (retida)" if retida else ""
                        self._log("INFO", f"Mensagem de {topico}{extra}: {res.formato}, "
                                          f"{n} campo{'s' if n != 1 else ''}")
                        for aviso in res.avisos:
                            self._log("AVISO", f"{topico}: {aviso}")
                    else:
                        suprimidos += 1
                    sink.adicionar(res.registros)
                else:
                    self._somar(erros=1)
                    self._log("ERRO" if res.formato != "vazio" else "AVISO",
                              f"Mensagem de {topico} ignorada: {res.erro}")

            sink = self._trocar_sink_se_preciso(sink)
            if terminar or sink.deve_descarregar():
                self._descarregar(sink, fechar=terminar)

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

    def _descarregar(self, sink: Sink, fechar: bool = False) -> None:
        try:
            n = sink.fechar() if fechar else sink.descarregar()
            self._somar(gravados=n)
        except Exception as exc:  # noqa: BLE001 - contingência sem disco, por exemplo
            self._somar(erros=1)
            self._log("ERRO", f"Falha ao gravar registros: {exc}")
        falhas = getattr(sink, "falhas", None)
        if falhas is not None:
            self._somar(erros=falhas())

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
