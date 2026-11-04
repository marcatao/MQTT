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
    canais_vistos = Signal(str, str)   # (device_id, JSON lista ["chd1","chd2"]) quando o conjunto de canais muda
    diagnostico_dispositivo = Signal(str, str)  # (device_id, JSON do objeto "diag" do DigiRail)
    resposta_comando = Signal(str, str, bool, str)  # (device_id, "reset_counters"|"diag", sucesso, mensagem)

    def __init__(self, config: Config, parent=None, *, atraso_conexao: float = 0.2,
                 intervalo: float = 0.05, falhar_conexao: bool = False, sql_ok: bool = True,
                 semente: int | None = 7, ciclo: float = 60.0, responder_comandos: bool = True,
                 atraso_resposta: float = 0.3, comandos_ok: bool = True):
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
        # Comandos ao dispositivo: com responder_comandos=False os testes emitem a resposta à mão.
        self.responder_comandos = responder_comandos
        self.atraso_resposta = atraso_resposta
        self.comandos_ok = comandos_ok
        self._diag_enviado: set[str] = set()

    # ----- ajudas para testes e capturas (emitem como o bridge real) -----
    def emitir_dispositivo(self, device_id: str, topico: str, iso: str | None = None) -> None:
        self.dispositivo_visto.emit(device_id, topico, iso or _iso_agora())

    def emitir_erro(self, categoria: str, mensagem: str, topico: str | None = None, detalhe: str | None = None,
                    payload: str | None = None, quando: str | None = None) -> None:
        self.erro_registrado.emit(json.dumps({
            "quando": quando or _iso_agora(), "categoria": categoria, "mensagem": mensagem, "topico": topico,
            "detalhe": detalhe, "payload": payload[:2000] if payload else None}, ensure_ascii=False))

    def emitir_canais(self, device_id: str, canais: list[str]) -> None:
        self.canais_vistos.emit(device_id, json.dumps(list(canais)))

    def emitir_diag(self, device_id: str, diag: dict) -> None:
        self.diagnostico_dispositivo.emit(device_id, json.dumps(diag, ensure_ascii=False))

    def emitir_resposta(self, device_id: str, comando: str, sucesso: bool, mensagem: str) -> None:
        self.resposta_comando.emit(device_id, comando, sucesso, mensagem)

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

    def zerar_contadores(self, device_id: str, canais: list[int]) -> None:
        self.chamadas.append(("zerar_contadores", device_id, list(canais)))
        if not self.responder_comandos:
            return

        def trabalho():
            time.sleep(self.atraso_resposta)
            nomes = ", ".join(f"chd{n}" for n in canais)
            if self.comandos_ok:
                self.resposta_comando.emit(device_id, "reset_counters", True, f"Contadores zerados ({nomes}).")
            else:
                msg = "Sem resposta do dispositivo em 15 s."
                self.resposta_comando.emit(device_id, "reset_counters", False, msg)
                self.emitir_erro("comando", f"Falha ao zerar contadores em {device_id}: {msg}",
                                 f"NOVUS/{device_id}/command", f"Canais: {nomes}")

        threading.Thread(target=trabalho, daemon=True).start()

    def pedir_diagnostico(self, device_id: str) -> None:
        self.chamadas.append(("pedir_diagnostico", device_id))
        if not self.responder_comandos:
            return

        def trabalho():
            time.sleep(self.atraso_resposta)
            self.diagnostico_dispositivo.emit(device_id, json.dumps(diag_exemplo(device_id)))

        threading.Thread(target=trabalho, daemon=True).start()

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
                if device not in self._diag_enviado:  # como o core: canais e diag na primeira vez que vê o device
                    self._diag_enviado.add(device)
                    self.emitir_canais(device, ["chd1", "chd2", "chd3"])
                    self.diagnostico_dispositivo.emit(device, json.dumps(diag_exemplo(device)))
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


def diag_exemplo(device_id: str) -> dict:
    """Objeto "diag" no formato do manual do DigiRail (p. 20–21): RSSI em texto, ipv4 em lista."""
    n = sum(map(ord, device_id)) % 40
    return {"error": 0, "title": f"DigiRail {device_id}", "location": "Fábrica", "curr_timestamp": int(time.time()),
            "cfg_timestamp": int(time.time()) - 86400, "fw_v": "1.23", "mqtt_queue": 0, "sn": f"{21000000 + n:08d}",
            "curr_rssi": str(50 + n % 20), "min_rssi": "-46", "max_rssi": "-87", "avg_rssi": "-58",
            "ipv4": [192, 168, 100, 50 + n], "gateway": [192, 168, 100, 1], "mask": [255, 255, 255, 0]}


def _iso_agora() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
