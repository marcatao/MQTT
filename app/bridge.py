"""Adaptador core ↔ GUI: os eventos do ``Pipeline`` viram Signals do Qt.

Único arquivo do lado do core que importa PySide6. Os sinais são emitidos de threads do core; como o
``PipelineBridge`` vive na thread da GUI, o Qt entrega os sinais lá (conexão enfileirada automática).
"""
from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime

from PySide6.QtCore import QObject, Signal

from app.config import Config, SqlServerConfig
from app.core import sql_conexao
from app.core.pipeline import Pipeline


class PipelineBridge(QObject):
    status_alterado = Signal(str, str)       # (estado, detalhe): "desconectado" | "conectando" | "conectado" | "erro"
    log = Signal(str, str, str)              # (hora "HH:MM:SS", nivel "INFO" | "AVISO" | "ERRO", mensagem)
    contadores = Signal(int, int, int)       # (mensagens recebidas, registros gravados, erros), máx. ~5x/s
    teste_sql_concluido = Signal(bool, str)  # (sucesso, mensagem multilinha em português)
    proxima_gravacao = Signal(float)         # epoch (s) do próximo ciclo de gravação; ao conectar e a cada ciclo
    ultima_gravacao_ok = Signal(str)         # "AAAA-MM-DDTHH:MM:SS" local do último INSERT com commit no SQL Server
    ultima_leitura = Signal(str)             # "AAAA-MM-DDTHH:MM:SS" local da última mensagem MQTT (máx. 1 a cada 5 s)
    dispositivo_visto = Signal(str, str, str)  # (device_id, tópico, ISO local) de leitura recebida; máx. 1/5 s por device
    erro_registrado = Signal(str)            # JSON de UM erro (app.core.erros); 1 sinal para cada +1 no contador de erros

    def __init__(self, config: Config, parent=None) -> None:
        super().__init__(parent)
        self._config = config
        self._pipeline = Pipeline(
            config,
            ao_status=self.status_alterado.emit,
            ao_log=self._emitir_log,
            ao_contadores=self.contadores.emit,
            ao_proxima_gravacao=self.proxima_gravacao.emit,
            ao_gravacao_ok=self.ultima_gravacao_ok.emit,
            ao_leitura=self.ultima_leitura.emit,
            ao_dispositivo=self.dispositivo_visto.emit,
            ao_erro=self.erro_registrado.emit,
        )

    def _emitir_log(self, nivel: str, mensagem: str) -> None:
        self.log.emit(datetime.now().strftime("%H:%M:%S"), nivel, mensagem)

    def definir_sql(self, sql: SqlServerConfig) -> None:
        """Troca a configuração do SQL Server; se conectado, vale a partir do próximo lote."""
        self._pipeline.definir_sql(replace(sql))

    def testar_sql(self, sql: SqlServerConfig) -> None:
        """Não bloqueia: testa numa thread e responde em ``teste_sql_concluido``."""
        copia = replace(sql)

        def trabalho() -> None:
            try:
                ok, msg = sql_conexao.testar_conexao(copia)
            except Exception as exc:  # noqa: BLE001
                ok, msg = False, sql_conexao.traduzir_erro(exc)
            self.teste_sql_concluido.emit(ok, msg)

        threading.Thread(target=trabalho, name="teste-sql", daemon=True).start()

    def conectar(self, broker: str, porta: int, topico: str, qos: int) -> None:
        self._pipeline.conectar(broker, porta, topico, qos)

    def desconectar(self) -> None:
        self._pipeline.desconectar()

    def encerrar(self, timeout: float = 5.0) -> None:
        self._pipeline.encerrar(timeout)
