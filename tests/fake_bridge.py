"""PipelineBridge falso para desenvolver e testar a GUI sem broker nem banco.

Segue exatamente o contrato de `app/bridge.py` descrito no CLAUDE.md e emite os sinais a partir de uma
thread própria, como o bridge real (o Qt entrega na thread da GUI por conexão enfileirada).
"""
from __future__ import annotations

import random
import threading
import time
from datetime import datetime

from PySide6.QtCore import QObject, Signal

from app.config import Config, SqlServerConfig

AMOSTRAS = [
    ("NOVUS/Teste_Alumbra/logbox01", "JSON", 6),
    ("NOVUS/Teste_Alumbra/logbox02", "JSON", 4),
    ("NOVUS/Teste_Alumbra/fieldlogger", "tabular (;)", 8),
]


class FakePipelineBridge(QObject):
    status_alterado = Signal(str, str)
    log = Signal(str, str, str)
    contadores = Signal(int, int, int)
    teste_sql_concluido = Signal(bool, str)

    def __init__(self, config: Config, parent=None, *, atraso_conexao: float = 0.2,
                 intervalo: float = 0.05, falhar_conexao: bool = False, sql_ok: bool = True,
                 semente: int | None = 7):
        super().__init__(parent)
        self.config = config
        self.atraso_conexao = atraso_conexao
        self.intervalo = intervalo
        self.falhar_conexao = falhar_conexao
        self.sql_ok = sql_ok
        self._rng = random.Random(semente)
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None
        self.chamadas: list[tuple] = []
        self.sql: SqlServerConfig | None = None
        self.recebidas = self.gravadas = self.erros = 0

    # ----- contrato -----
    def definir_sql(self, sql: SqlServerConfig) -> None:
        self.chamadas.append(("definir_sql", sql))
        self.sql = sql

    def testar_sql(self, sql: SqlServerConfig) -> None:
        self.chamadas.append(("testar_sql", sql))

        def trabalho():
            time.sleep(0.15)
            if self.sql_ok:
                self.teste_sql_concluido.emit(
                    True, f"Conectado a {sql.servidor}, banco {sql.banco}.\nSQL Server 2019 (15.0.4375.4)")
            else:
                self.teste_sql_concluido.emit(
                    False, "Falha no login do usuário.\nConfira o usuário e a senha (erro 18456).")

        threading.Thread(target=trabalho, daemon=True).start()

    def conectar(self, broker: str, porta: int, topico: str, qos: int) -> None:
        self.chamadas.append(("conectar", broker, porta, topico, qos))
        self._parar_thread()
        self._parar.clear()
        self._thread = threading.Thread(target=self._rodar, args=(broker, porta, topico, qos), daemon=True)
        self._thread.start()

    def desconectar(self) -> None:
        self.chamadas.append(("desconectar",))

        def trabalho():
            self._parar_thread()
            self._log("INFO", "Desconectado do broker.")
            self.status_alterado.emit("desconectado", "")

        threading.Thread(target=trabalho, daemon=True).start()

    def encerrar(self, timeout: float = 5.0) -> None:
        self.chamadas.append(("encerrar", timeout))
        self._parar_thread(timeout)

    # ----- simulação -----
    def _parar_thread(self, timeout: float = 2.0) -> None:
        self._parar.set()
        t = self._thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout)
        self._thread = None

    def _log(self, nivel: str, msg: str) -> None:
        self.log.emit(datetime.now().strftime("%H:%M:%S"), nivel, msg)

    def _rodar(self, broker: str, porta: int, topico: str, qos: int) -> None:
        self.status_alterado.emit("conectando", f"{broker}:{porta}")
        self._log("INFO", f"Conectando a {broker}:{porta}…")
        if self._parar.wait(self.atraso_conexao):
            return
        if self.falhar_conexao:
            self._log("ERRO", f"Não foi possível conectar a {broker}:{porta}: conexão recusada.")
            self.status_alterado.emit("erro", "Conexão recusada pelo broker")
            return
        self.status_alterado.emit("conectado", f"{broker}:{porta} · {topico}")
        self._log("INFO", f"Conectado. Assinando “{topico}” com QoS {qos}.")
        if self.sql and self.sql.habilitado:
            self._log("INFO", f"Gravando em {self.sql.servidor} / {self.sql.banco} ({self.sql.tabela}).")
        ultimo = 0.0
        n = 0
        while not self._parar.wait(self.intervalo):
            n += 1
            sub, formato, campos = AMOSTRAS[n % len(AMOSTRAS)]
            self.recebidas += 1
            self._log("INFO", f"Mensagem em {sub} · {formato} · {campos} campos")
            if n % 9 == 0:
                self.erros += 1
                self._log("AVISO", f"Campo “temp_3” sem valor numérico em {sub}; gravado como texto.")
            elif n % 23 == 0:
                self.erros += 1
                self._log("ERRO", "Payload inválido em NOVUS/Teste_Alumbra/logbox02: JSON truncado (posição 118).")
            else:
                self.gravadas += campos
            if n % 4 == 0:
                self._log("INFO", f"Lote gravado: {self._rng.randint(12, 40)} registros em {self._rng.randint(8, 30)} ms.")
            agora = time.monotonic()
            if agora - ultimo >= 0.2:
                ultimo = agora
                self.contadores.emit(self.recebidas, self.gravadas, self.erros)
        self.contadores.emit(self.recebidas, self.gravadas, self.erros)
