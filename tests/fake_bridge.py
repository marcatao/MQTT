"""PipelineBridge falso para desenvolver e testar a GUI sem broker nem banco.

Segue exatamente o contrato de `app/bridge.py` descrito no CLAUDE.md e emite os sinais a partir de uma
thread própria, como o bridge real (o Qt entrega na thread da GUI por conexão enfileirada).
"""
from __future__ import annotations

import json
import math
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
    proxima_gravacao = Signal(float)   # epoch do próximo ciclo (alinhado ao relógio)
    ultima_gravacao_ok = Signal(str)   # ISO local da última gravação com sucesso no SQL
    ultima_leitura = Signal(str)       # ISO local da última mensagem recebida
    dispositivo_visto = Signal(str, str, str)  # (device_id, tópico, ISO local); máx. 1 a cada 5 s por device
    erro_registrado = Signal(str)      # JSON de UM erro; 1 sinal para cada +1 no contador de erros

    def __init__(self, config: Config, parent=None, *, atraso_conexao: float = 0.2,
                 intervalo: float = 0.05, falhar_conexao: bool = False, sql_ok: bool = True,
                 semente: int | None = 7, ciclo: float = 60.0):
        super().__init__(parent)
        self.config = config
        self.atraso_conexao = atraso_conexao
        self.intervalo = intervalo
        self.falhar_conexao = falhar_conexao
        self.sql_ok = sql_ok
        self.ciclo = ciclo  # segundos entre gravações (0 = sem ciclo: não emite proxima_gravacao)
        self._rng = random.Random(semente)
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None
        self.chamadas: list[tuple] = []
        self.sql: SqlServerConfig | None = None
        self.recebidas = self.gravadas = self.erros = 0
        self._visto_em: dict[str, float] = {}

    # ----- ajudas para testes e capturas (emitem como o bridge real) -----
    def emitir_dispositivo(self, device_id: str, topico: str, iso: str | None = None) -> None:
        self.dispositivo_visto.emit(device_id, topico, iso or _iso_agora())

    def emitir_erro(self, categoria: str, mensagem: str, topico: str | None = None, detalhe: str | None = None,
                    payload: str | None = None, quando: str | None = None) -> None:
        self.erro_registrado.emit(json.dumps({
            "quando": quando or _iso_agora(), "categoria": categoria, "mensagem": mensagem, "topico": topico,
            "detalhe": detalhe, "payload": payload[:2000] if payload else None}, ensure_ascii=False))

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

    def _emitir_proxima(self) -> float | None:
        if self.ciclo <= 0:
            return None
        agora = time.time()
        proxima = math.floor(agora / self.ciclo + 1) * self.ciclo  # alinhado: hh:mm:00 com ciclo de 60 s
        self.proxima_gravacao.emit(float(proxima))
        return proxima

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
        proxima = self._emitir_proxima()
        while not self._parar.wait(self.intervalo):
            n += 1
            self.ultima_leitura.emit(_iso_agora())
            if proxima is not None and time.time() >= proxima:
                if self.sql and self.sql.habilitado and self.sql_ok:
                    self.ultima_gravacao_ok.emit(_iso_agora())
                proxima = self._emitir_proxima()
            sub, formato, campos = AMOSTRAS[n % len(AMOSTRAS)]
            self.recebidas += 1
            self._log("INFO", f"Mensagem em {sub} · {formato} · {campos} campos")
            device = sub.rsplit("/", 1)[-1]
            agora_m = time.monotonic()
            if agora_m - self._visto_em.get(device, -1e9) >= 5.0:  # como o bridge real: 1 a cada 5 s por device
                self._visto_em[device] = agora_m
                self.emitir_dispositivo(device, sub)
            if n % 9 == 0:
                self.erros += 1
                self._log("AVISO", f"Campo “temp_3” sem valor numérico em {sub}; gravado como texto.")
                self.emitir_erro("payload", f"Leitura incompleta em {sub}: falta ts_unix.", sub,
                                 "Campos obrigatórios: pid, device_id, ts_unix. Enviada para a contingência.",
                                 json.dumps({"pid": 51387400 + n, "device_id": device,
                                             "channels": {"chd1_value": 67, "chd2_value": 119}}))
            elif n % 23 == 0:
                self.erros += 1
                self._log("ERRO", "Payload inválido em NOVUS/Teste_Alumbra/logbox02: JSON truncado (posição 118).")
                self.emitir_erro("payload", "Payload inválido: JSON truncado (posição 118).",
                                 "NOVUS/Teste_Alumbra/logbox02", "Expecting ',' delimiter: line 1 column 118",
                                 '{"pid":51387408,"device_id":"logbox02","channels":{"timestamp":17933867')
            else:
                self.gravadas += campos
            if n % 4 == 0:
                self._log("INFO", f"Lote gravado: {self._rng.randint(12, 40)} registros em {self._rng.randint(8, 30)} ms.")
            agora = time.monotonic()
            if agora - ultimo >= 0.2:
                ultimo = agora
                self.contadores.emit(self.recebidas, self.gravadas, self.erros)
        self.contadores.emit(self.recebidas, self.gravadas, self.erros)


def _iso_agora() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
