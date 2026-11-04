"""Liga MQTT → fila → mapeamento (dbo.IOT001) → sink e expõe eventos (status, log, contadores) por callbacks.

Threads:
- rede do paho: só enfileira a mensagem (``queue.Queue`` limitada) e retorna;
- processamento (uma por conexão): mapeamento, lote e gravação; manda os contadores no máx. ~5x/s;
- controle (``ThreadPoolExecutor`` de 1 worker): executa conectar/desconectar em ordem, sem bloquear quem chama.

Os callbacks são chamados de qualquer uma dessas threads; o ``bridge`` os transforma em Signals do Qt.

Classificação (``app.core.mapeamento.classificar``): só **leituras** (``channels`` ou formatos genéricos) seguem
para amostragem/sink. **Eventos** (``events``) viram um log INFO resumido; **respostas** (``reported``/``desired`` ou
tópicos ``.../ack/command``/``.../ack/config``) não são leitura nem erro: ``diag`` e ``reset_counters`` são tratados
(ver "Comandos").

Amostragem pelo minuto da leitura (``[gravacao] intervalo_gravacao_segundos``, padrão 60; 0 desliga): as leituras
completas são agrupadas por ``(device_id, ts_unix // intervalo)``; em cada balde fica a de maior ``ts_unix`` (empate:
a última que chegou). Os ciclos são alinhados ao relógio local (14:31:00, 14:32:00…); em cada ciclo vão ao sink
todos os baldes **já encerrados** (fim do minuto ≤ ciclo), inclusive os antigos reenviados pela memória circular do
dispositivo depois de uma queda ("recuperadas": minutos anteriores ao ciclo anterior). O minuto corrente espera o
próximo ciclo; leitura mais de ``FUTURO_MAX`` s à frente do relógio do PC vai no próximo ciclo (com um AVISO por
dispositivo: relógio adiantado). Ao desconectar/encerrar, todos os baldes pendentes são gravados. Leituras
incompletas vão na hora para o sink (contingência). Com 0, toda leitura vai ao sink (lote por
``lote_max``/``lote_segundos``).

Sem duplicatas (``_MemoriaGravadas``, 24 h, tamanho limitado): não vão ao sink de novo o mesmo
``(device_id, ts_unix)`` nem (com amostragem) o mesmo ``(device_id, minuto de ts_unix)``. Mensagens retidas e
reenvios QoS 1 caem aqui e contam como "duplicadas ignoradas" (não são erro). A memória vale enquanto o programa
estiver aberto, entre conexões.

Dispositivos (``ao_dispositivo``): para cada leitura ou evento com ``device_id``, avisa (device_id, tópico,
hora local da recepção), no máx. 1 vez a cada ``INTERVALO_DISPOSITIVO`` s por device; a primeira de cada device
(desde a conexão) sai sempre. Respostas (``ack/*``) não avisam (o tópico mostrado na aba Clientes é o dos dados).
``ao_canais(device_id, json)``: quando o conjunto de canais em ``channels`` de um device muda (1ª vez inclusive).

Comandos (``zerar_contadores``, ``pedir_diagnostico``; ``[dispositivos]``): publicados com QoS 1 em
``topico_comando`` ({id} = device_id), com ``timestamp`` = agora (único por device entre os pendentes). A resposta é
casada pelo ``timestamp``. ``reset_counters``: ``error`` 0 = sucesso; outro valor ou sem resposta em
``timeout_comando_segundos`` = falha, que soma 1 erro "comando". ``diag``: pedido automático ao ver o dispositivo
pela 1ª vez em cada conexão e a cada ``diagnostico_minutos`` (0 desliga); timeout é só AVISO. Todo ``reported.diag``
(pedido ou periódico do próprio aparelho) sai em ``ao_diagnostico``. Antes de publicar, garante a assinatura do
tópico de resposta (se a assinatura do usuário não cobre, assina o filtro do modelo, ex. ``NOVUS/+/ack/command``).
O eco dos nossos próprios comandos (quando a assinatura cobre o tópico de comando) é ignorado sem contar.
Diagnóstico automático sem resposta: AVISO e ``ao_resposta_comando(device, "diag", False, msg)`` com os tópicos do
pedido e da resposta esperada (não é erro).

Tópicos por dispositivo (``app.core.topicos``, ``dispositivos.json``): os comandos vão para o ``command`` efetivo do
device; mensagem num tópico salvo ``ack_command``/``ack_config`` é resposta daquele device (mesmo fora do padrão
``NOVUS/{id}``, ex. ``NOÏUS/Teste_Alumbra/ack/command``) e no ``events`` salvo identifica o device quando o payload
não traz ``device_id``. Os ``events``/``ack_command``/``ack_config`` salvos que a assinatura do usuário não cobre
viram assinaturas extras (QoS 1): registradas ao conectar (o cliente refaz em cada reconexão) e recalculadas na
thread de controle depois de salvar/restaurar (assina os novos, desassina os que sobraram).

Erros (``ao_erro``): todo +1 no contador de erros passa por ``_erro``, que soma, monta o registro JSON
(``app.core.erros``), grava em ``<pasta_logs>/erros_AAAAMMDD.jsonl`` e avisa o callback. Assim contador e registros
batem sempre. Categorias: "payload" (inválido, binário, vazio, incompleto), "sql" (lote para a contingência por
falha do banco, linha recusada na bissecção), "contingencia" (falha ao gravar o arquivo), "fila" (fila cheia),
"mqtt" (queda do broker, conexão recusada, assinatura recusada), "comando" (zerar contadores recusado ou sem
resposta).

Erros "mqtt" contam **um por queda (incidente)**, não um por tentativa de reconexão (o paho tenta a cada 1–60 s):
- conexão perdida sem o usuário pedir, ou tentativa de conexão que falha (inacessível/CONNACK recusado) sem incidente
  aberto → abre o incidente e soma 1 erro; as tentativas seguintes que falham não somam (só log);
- ao conectar de novo, o incidente fecha com um INFO "Broker voltou após X";
- desconectar/encerrar (inclusive o reinício do vigia) não gera erro e descarta o incidente aberto;
- cada SUBACK recusado soma 1 erro, com o tópico.
"""
from __future__ import annotations

import json
import logging
import math
import queue
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import paho.mqtt.client as _paho

from app.config import Config, DispositivosConfig, SqlServerConfig, pasta_base
from app.core import digirail
from app.core import erros as _erros
from app.core import topicos as _topicos
from app.core.mqtt_client import ClienteMqtt, validar_parametros
from app.core.mapeamento import EVENTO, RESPOSTA, LinhaIot, ResultadoMapa, _device_da_mensagem, mapear
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


def duracao_texto(segundos: float) -> str:
    """3 → "3 s"; 192 → "3 min 12 s"; 3725 → "1 h 2 min 5 s"."""
    total = int(round(max(0.0, segundos)))
    h, resto = divmod(total, 3600)
    m, s = divmod(resto, 60)
    partes = ([f"{h} h"] if h else []) + ([f"{m} min"] if h or m else []) + [f"{s} s"]
    return " ".join(partes)


def _plural(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


FUTURO_MAX = 300.0  # s: ts_unix mais que isso à frente do relógio do PC = relógio do dispositivo adiantado


class _MemoriaGravadas:
    """Chaves já entregues ao sink, com expiração (``validade`` s) e limite de tamanho (descarta as mais antigas).

    Chaves: ``("ts", device_id, ts_unix)`` e ``("min", device_id, balde)``. Só a thread de processamento usa.
    """

    def __init__(self, validade: float = 24 * 3600, limite: int = 200_000) -> None:
        self.validade = validade
        self.limite = limite
        self._itens: OrderedDict[tuple, float] = OrderedDict()

    def __len__(self) -> int:
        return len(self._itens)

    def _limpar(self, agora: float) -> None:
        itens = self._itens
        while itens:
            chave, t = next(iter(itens.items()))
            if len(itens) > self.limite or t < agora - self.validade:
                itens.popitem(last=False)
            else:
                break

    def contem(self, chave: tuple, agora: float) -> bool:
        self._limpar(agora)
        return chave in self._itens

    def adicionar(self, chave: tuple, agora: float) -> None:
        self._itens[chave] = agora
        self._itens.move_to_end(chave)
        self._limpar(agora)


@dataclass
class _Balde:
    linha: LinhaIot
    recebidas: int = 1
    ts_vistos: set[int] = field(default_factory=set)


class _Amostragem:
    """Leitura completa mais recente por ``(device_id, minuto de ts_unix)``; grava os minutos já encerrados."""

    def __init__(self, intervalo: float = 60.0, memoria: _MemoriaGravadas | None = None) -> None:
        self.intervalo = float(intervalo)
        self.memoria = memoria if memoria is not None else _MemoriaGravadas()
        self.baldes: dict[tuple[str, int], _Balde] = {}
        self.duplicadas = 0   # desde o último ciclo

    def balde(self, ts: int) -> int:
        return int(ts // self.intervalo)

    def adicionar(self, linhas: list[LinhaIot], agora: float) -> int:
        """Devolve quantas eram duplicadas (já gravadas ou repetidas no balde) e foram ignoradas."""
        dup = 0
        for linha in linhas:
            dev, ts = linha.device_id, linha.ts_unix
            b = self.balde(ts)
            if self.memoria.contem(("ts", dev, ts), agora) or self.memoria.contem(("min", dev, b), agora):
                dup += 1
                continue
            balde = self.baldes.get((dev, b))
            if balde is None:
                self.baldes[(dev, b)] = _Balde(linha, 1, {ts})
                continue
            if ts in balde.ts_vistos:
                dup += 1
                continue
            balde.recebidas += 1
            balde.ts_vistos.add(ts)
            if ts >= balde.linha.ts_unix:     # empate de ts não acontece (ts_vistos); maior ts vence
                balde.linha = linha
        self.duplicadas += dup
        return dup

    def retirar(self, momento: float | None, agora: float, referencia: float | None = None
                ) -> tuple[list[_Balde], list[_Balde], int]:
        """Tira os baldes prontos e marca-os como gravados. Devolve (normais, recuperados, duplicadas no período).

        ``momento`` = virada do ciclo: prontos são os baldes encerrados (fim ≤ momento) e os do futuro distante
        (relógio do dispositivo adiantado). ``momento`` None (desconexão) = todos. Recuperados são os baldes que
        terminaram antes do ciclo anterior (``referencia - intervalo``; ``referencia`` padrão = ``momento``).
        """
        ref = referencia if referencia is not None else momento
        normais: list[_Balde] = []
        recuperados: list[_Balde] = []
        for chave in sorted(self.baldes, key=lambda k: (k[1], k[0])):    # cronológico
            dev, b = chave
            inicio, fim = b * self.intervalo, (b + 1) * self.intervalo
            if momento is not None and fim > momento and inicio <= agora + FUTURO_MAX:
                continue    # minuto corrente (ou pouco à frente): espera o próximo ciclo
            balde = self.baldes.pop(chave)
            self.memoria.adicionar(("min", dev, b), agora)
            for ts in balde.ts_vistos:
                self.memoria.adicionar(("ts", dev, ts), agora)
            if ref is not None and fim <= ref - self.intervalo:
                recuperados.append(balde)
            else:
                normais.append(balde)
        dup, self.duplicadas = self.duplicadas, 0
        return normais, recuperados, dup

    @property
    def pendentes(self) -> int:
        return len(self.baldes)


@dataclass
class _Comando:
    device_id: str
    comando: str            # "reset_counters" | "diag"
    timestamp: int
    inicio: float           # relógio do pipeline
    topico: str
    manual: bool = True     # diag automático só responde em ao_resposta_comando quando falha por falta de resposta
    canais: list[int] = field(default_factory=list)
    topico_resposta: str = ""   # ack_command efetivo do device (para a mensagem de "sem resposta")


class _Limitador:
    """No máx. ``por_segundo`` logs "por mensagem"; o excedente vira "… e mais N mensagens no último segundo"."""

    def __init__(self, por_segundo: int) -> None:
        self.por_segundo = por_segundo
        self.inicio = time.monotonic()
        self.n = 0
        self.suprimidos = 0

    def pode(self, logar) -> bool:
        agora = time.monotonic()
        if agora - self.inicio >= 1.0:
            self.fechar(logar)
            self.inicio, self.n = agora, 0
        if self.n < self.por_segundo:
            self.n += 1
            return True
        self.suprimidos += 1
        return False

    def fechar(self, logar) -> None:
        if self.suprimidos:
            logar("INFO", f"… e mais {self.suprimidos} mensagens no último segundo")
        self.suprimidos = 0


class Pipeline:
    TAMANHO_FILA = 20000
    INTERVALO_CONTADORES = 0.2      # s (≈5 por segundo)
    LOGS_POR_SEGUNDO = 20           # linhas "Mensagem de ..." por segundo; o excedente vira um resumo
    INTERVALO_ULTIMA_LEITURA = 5.0  # s: no máx. 1 aviso de "última leitura" a cada 5 s
    INTERVALO_DISPOSITIVO = 5.0     # s: no máx. 1 aviso de "dispositivo visto" a cada 5 s por device_id

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
        ao_dispositivo: Callable[[str, str, str], None] = _nada,
        ao_erro: Callable[[str], None] = _nada,
        ao_canais: Callable[[str, str], None] = _nada,
        ao_diagnostico: Callable[[str, str], None] = _nada,
        ao_resposta_comando: Callable[[str, str, bool, str], None] = _nada,
    ) -> None:
        """``relogio``, ``intervalo_gravacao`` (sobrepõe a config) e ``fabrica_cliente`` existem para os testes."""
        self.config = config
        self._ao_status = ao_status
        self._ao_log = ao_log
        self._ao_contadores = ao_contadores
        self._ao_proxima_gravacao = ao_proxima_gravacao
        self._ao_gravacao_ok = ao_gravacao_ok
        self._ao_leitura = ao_leitura
        self._ao_dispositivo = ao_dispositivo
        self._ao_erro = ao_erro
        self._ao_canais = ao_canais
        self._ao_diagnostico = ao_diagnostico
        self._ao_resposta_comando = ao_resposta_comando
        self._dispositivos_t: dict[str, float] = {}  # device_id → epoch do último aviso (só a thread de processamento)
        self._relogio = relogio
        self._intervalo_fixo = intervalo_gravacao
        self._fabrica_cliente = fabrica_cliente or ClienteMqtt
        self._ultima_leitura_t: float | None = None
        self._fabrica_sink = fabrica_sink or self._sink_padrao
        self._pasta = pasta or pasta_base()
        self._sessao_persistente = sessao_persistente
        self._gravador_erros: _erros.GravadorErros | None = None

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

        # incidentes do broker (protegidos por _trava; chegam pela thread de rede do paho)
        self._geracao = 0                      # muda a cada conectar/parar: eventos de cliente antigo são ignorados
        self._endereco = ""
        self._incidente_inicio: float | None = None
        self._ultima_msg_t: float | None = None  # epoch da última mensagem recebida nesta conexão (sem limitação)

        # DigiRail: duplicatas, canais, relógio e comandos
        self._memoria = _MemoriaGravadas()           # entre conexões; só a thread de processamento usa
        self._canais: dict[str, tuple[str, ...]] = {}  # device_id → canais vistos em channels (último conjunto)
        self._avisou_futuro: set[str] = set()        # devices com aviso de relógio adiantado
        self._trava_cmd = threading.Lock()
        self._comandos: dict[tuple[str, int], _Comando] = {}   # (device_id, timestamp) → pendente
        self._diag_t: dict[str, float] = {}          # device_id → último diag automático (por conexão)
        self._topicos_publicados: set[str] = set()   # eco dos nossos comandos é ignorado

        # tópicos por dispositivo (dispositivos.json) e as assinaturas extras que eles exigem
        self._topicos = _topicos.TopicosDispositivos(self._caminho_topicos(), self._cfg_dispositivos(),
                                                     logar=lambda n, m: logger.log(_NIVEIS.get(n, 20), m))
        self._avisos_topicos = list(self._topicos.avisos)   # repetidos no log da tela na 1ª conexão
        self._trava_topicos = threading.Lock()
        self._extras_dispositivos: list[str] = []    # tópicos salvos assinados como extra (exatos)

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

    def zerar_contadores(self, device_id: str, canais: list[int]) -> Future | None:
        """Não bloqueia: publica ``reset_counters`` e responde em ``ao_resposta_comando``."""
        return self._submeter(self._zerar, device_id, list(canais or []), comando="reset_counters",
                              device_id=device_id)

    def pedir_diagnostico(self, device_id: str) -> Future | None:
        """Não bloqueia: publica ``diag``; a resposta sai em ``ao_diagnostico`` e ``ao_resposta_comando``."""
        return self._submeter(self._pedir_diag, device_id, True, comando="diag", device_id=device_id)

    # -- tópicos por dispositivo ----------------------------------------------------------------------------

    def topicos_do_dispositivo(self, device_id: str) -> dict[str, str]:
        """Os 5 tópicos efetivos do device (salvos ou padrão). Não bloqueia."""
        return self._topicos.efetivos(str(device_id or "").strip())

    def salvar_topicos_dispositivo(self, device_id: str, topicos: dict[str, str]) -> Future | None:
        """Valida e grava ``dispositivos.json`` na hora (ValueError em português se inválido); a (des)assinatura
        dos extras vai para a thread de controle."""
        device = str(device_id or "").strip()
        try:
            self._topicos.salvar(device, dict(topicos or {}))
        except ValueError as exc:
            self._log("ERRO", f"Tópicos de {device or '?'} não salvos: {exc}")
            raise
        except OSError as exc:
            msg = f"Não foi possível gravar {self._topicos.caminho}: {exc}"
            self._log("ERRO", f"Tópicos de {device} não salvos: {msg}")
            raise ValueError(msg) from exc
        return self._aplicar_topicos_depois(device, "salvos")

    def restaurar_topicos_padrao(self, device_id: str) -> Future | None:
        """Apaga os tópicos salvos do device (volta ao padrão de ``[dispositivos]``) e refaz os extras."""
        device = str(device_id or "").strip()
        try:
            self._topicos.restaurar(device)
        except ValueError as exc:
            self._log("ERRO", f"Tópicos de {device or '?'} não restaurados: {exc}")
            raise
        except OSError as exc:
            msg = f"Não foi possível gravar {self._topicos.caminho}: {exc}"
            self._log("ERRO", f"Tópicos de {device} não restaurados: {msg}")
            raise ValueError(msg) from exc
        return self._aplicar_topicos_depois(device, "restaurados para o padrão")

    def topicos_ouvidos(self) -> list[str]:
        """Filtros assinados agora: a assinatura do usuário + extras. Vazio se não houver conexão."""
        cliente = self._cliente
        if cliente is None or not self._assinatura:
            return []
        extras = list(getattr(cliente, "assinaturas_extras", []) or [])
        return [self._assinatura] + [f for f in extras if f != self._assinatura]

    def topico_coberto(self, topico: str) -> bool:
        """O integrador recebe mensagens deste tópico? (assinatura do usuário + extras ativos, ``topic_matches_sub``)

        Desconectado, responde pela última assinatura usada (ou a de ``[mqtt]``) + os extras que seriam assinados.
        """
        if not isinstance(topico, str) or not topico or "+" in topico or "#" in topico:
            return False
        assinatura = self._assinatura or self.config.mqtt.topico
        filtros = [assinatura] if assinatura else []
        cliente = self._cliente
        if cliente is not None:
            filtros += list(getattr(cliente, "assinaturas_extras", []) or [])
        with self._trava_topicos:
            filtros += self._extras_dispositivos
        if cliente is None:
            filtros += self._extras_desejados(assinatura)
        for f in filtros:
            try:
                if _paho.topic_matches_sub(f, topico):
                    return True
            except Exception:  # noqa: BLE001 - filtro estranho: não cobre
                continue
        return False

    def _caminho_topicos(self) -> Path:
        nome = getattr(self._cfg_dispositivos(), "arquivo_topicos", "") or "dispositivos.json"
        caminho = Path(nome)
        return caminho if caminho.is_absolute() else self._pasta / caminho

    def _extras_desejados(self, assinatura: str) -> list[str]:
        """Tópicos salvos (events/ack_command/ack_config) que a assinatura do usuário não cobre, sem repetição."""
        desejados: list[str] = []
        for device, tops in sorted(self._topicos.todos_salvos().items()):
            for chave in _topicos.PUBLICADOS_PELO_APARELHO:
                t = tops[chave]
                coberto = False
                if assinatura:
                    try:
                        coberto = _paho.topic_matches_sub(assinatura, t)
                    except Exception:  # noqa: BLE001
                        coberto = False
                if not coberto and t not in desejados:
                    desejados.append(t)
        return desejados

    def _aplicar_topicos_depois(self, device: str, acao: str) -> Future | None:
        try:
            return self._controle.submit(self._aplicar_topicos, device, acao)
        except RuntimeError:   # programa encerrando
            return None

    def _aplicar_topicos(self, device: str | None = None, acao: str = "") -> None:
        """Thread de controle: assina os extras novos e desassina os que não servem mais (cliente atual)."""
        cliente = self._cliente
        assinatura = self._assinatura if cliente is not None else (self._assinatura or self.config.mqtt.topico)
        desejados = self._extras_desejados(assinatura) if assinatura else []
        with self._trava_topicos:
            antigos = list(self._extras_dispositivos)
            self._extras_dispositivos = desejados if cliente is not None else []
        novos = [t for t in desejados if t not in antigos]
        sobrando = [t for t in antigos if t not in desejados]
        if cliente is not None:
            assinar = getattr(cliente, "assinar_extra", None)
            desassinar = getattr(cliente, "desassinar_extra", None)
            for t in sobrando:
                if desassinar is not None:
                    desassinar(t)
            for t in novos:
                if assinar is not None:
                    try:
                        assinar(t, 1)
                    except ValueError as exc:
                        self._log("ERRO", f"Não foi possível assinar {t!r}: {exc}")
        if device is None:
            return
        do_device = set(self._topicos.efetivos(device).values())
        partes = []
        meus_novos = [t for t in novos if t in do_device]
        if meus_novos:
            verbo = "assinando" if cliente is not None else "serão assinados ao conectar"
            partes.append(f"{verbo} {', '.join(repr(t) for t in meus_novos)}")
        if sobrando:
            verbo = "assinatura cancelada de" if cliente is not None else "não serão mais assinados"
            partes.append(f"{verbo} {', '.join(repr(t) for t in sobrando)}")
        if not partes:
            partes.append("a assinatura atual já cobre os tópicos do dispositivo" if cliente is not None
                          else "valem na próxima conexão")
        self._log("INFO", f"Tópicos de {device} {acao}; {'; '.join(partes)}.")

    def _submeter(self, fn, *args, comando: str, device_id: str) -> Future | None:
        try:
            return self._controle.submit(fn, *args)
        except RuntimeError:   # já encerrado
            self._avisar(self._ao_resposta_comando, str(device_id or ""), comando, False, "Programa encerrando.")
            return None

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

    def _erro(self, categoria: str, mensagem: str, topico: str | None = None, detalhe: str | None = None,
              payload: bytes | str | None = None) -> None:
        """Soma 1 no contador de erros e registra o erro (callback + arquivo do dia). Único jeito de somar erro."""
        self._somar(erros=1)
        try:
            agora = self._relogio()
            registro = _erros.montar_registro(iso_local(agora), categoria, mensagem, topico, detalhe, payload)
            texto = _erros.para_json(registro)
        except Exception:  # noqa: BLE001 - nunca derruba o pipeline por causa do registro
            logger.exception("Falha ao montar o registro de erro")
            return
        gravador = self._gravador_erros
        if gravador is None:
            gravador = self._gravador_erros = _erros.GravadorErros(self._pasta_logs(), self._log)
        try:
            gravador.gravar(texto, datetime.fromtimestamp(agora).date())
        except Exception:  # noqa: BLE001
            logger.exception("Falha ao gravar o registro de erro")
        self._avisar(self._ao_erro, texto)

    def _evento_mqtt(self, geracao: int, tipo: str, motivo: str, topico: str | None = None) -> None:
        """Eventos de conexão do ``ClienteMqtt`` (thread de rede): um erro por queda, INFO na volta."""
        agora = self._relogio()
        erro: tuple[str, str] | None = None
        voltou: float | None = None
        with self._trava:
            if geracao != self._geracao or self._encerrado:
                return  # cliente antigo, desconexão pedida ou encerramento: nunca é erro
            endereco = self._endereco
            if tipo == "conectado":
                self._diag_t = {}   # cada (re)conexão ao broker pede o diagnóstico de novo ao ver o device
                if self._incidente_inicio is not None:
                    voltou = max(0.0, agora - self._incidente_inicio)
                    self._incidente_inicio = None
            elif tipo in ("perdida", "falha_conexao"):
                if self._incidente_inicio is None:
                    self._incidente_inicio = agora
                    if tipo == "perdida":
                        ultima = self._ultima_msg_t
                        quando = (f"última mensagem recebida em {iso_local(ultima)}" if ultima is not None
                                  else "nenhuma mensagem recebida nesta conexão")
                        erro = (f"Conexão com o broker {endereco} perdida", f"Motivo: {motivo}; {quando}.")
                    else:
                        erro = (f"Não foi possível conectar ao broker {endereco}", f"Motivo: {motivo}.")
        if tipo == "assinatura_recusada":
            filtro = topico or self._assinatura
            self._erro("mqtt", f"Assinatura recusada pelo broker para {filtro}.", filtro,
                       f"Código do SUBACK: {motivo}. Verifique as permissões (ACL) do usuário no broker.")
        elif erro is not None:
            self._erro("mqtt", erro[0], detalhe=erro[1])
        elif voltou is not None:
            self._log("INFO", f"Broker voltou após {duracao_texto(voltou)}")

    def _pasta_logs(self) -> Path:
        pasta = getattr(self.config.gravacao, "pasta_logs", "logs") or "logs"
        return _erros.resolver_pasta(pasta, self._pasta)

    def _avisar_dispositivos(self, devices: list[str | None], topico: str, t_receb: float) -> None:
        vistos: set[str] = set()
        for device in devices:
            if not device or device in vistos:
                continue
            vistos.add(device)
            ultimo = self._dispositivos_t.get(device)
            if ultimo is None or t_receb - ultimo >= self.INTERVALO_DISPOSITIVO or t_receb < ultimo:
                self._dispositivos_t[device] = t_receb
                self._avisar(self._ao_dispositivo, device, topico, iso_local(t_receb))

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
        with self._trava:
            self._geracao += 1
            geracao = self._geracao
            self._endereco = f"{broker}:{porta}"
            self._incidente_inicio = None
            self._ultima_msg_t = None
        self._dispositivos_t = {}
        self._diag_t = {}
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
                ao_evento=lambda tipo, motivo, topico=None, g=geracao: self._evento_mqtt(g, tipo, motivo, topico),
            )
            self._registrar_extras_ao_conectar()
            self._cliente.iniciar()
        except Exception as exc:  # noqa: BLE001
            self._log("ERRO", f"Não foi possível iniciar a conexão com {broker}:{porta}: {exc}")
            self._status("erro", str(exc))
            self._cliente = None
            self._finalizar_processador()

    def _registrar_extras_ao_conectar(self) -> None:
        """Antes de ``iniciar``: os extras dos tópicos salvos entram no cliente (assinados em cada CONNACK)."""
        for aviso in self._avisos_topicos:
            self._log("AVISO", aviso)
        self._avisos_topicos = []
        desejados = self._extras_desejados(self._assinatura)
        assinar = getattr(self._cliente, "assinar_extra", None)
        with self._trava_topicos:
            self._extras_dispositivos = list(desejados)
        if not desejados or assinar is None:
            return
        for t in desejados:
            try:
                assinar(t, 1)
            except ValueError as exc:
                self._log("ERRO", f"Não foi possível assinar {t!r}: {exc}")
        self._log("INFO", "Assinando também os tópicos salvos fora da assinatura: "
                          f"{', '.join(repr(t) for t in desejados)}.")

    def _parar(self, silencioso: bool = False) -> None:
        with self._trava:
            self._geracao += 1          # a partir daqui, eventos do cliente que está saindo não contam
            self._incidente_inicio = None
        cliente, self._cliente = self._cliente, None
        with self._trava_topicos:
            self._extras_dispositivos = []
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
        self._cancelar_comandos("Conexão encerrada antes da resposta do dispositivo.")
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
        if topico in self._topicos_publicados:
            return  # eco do nosso próprio comando (a assinatura cobre o tópico de comando): não é mensagem recebida
        self._somar(recebidas=1)
        agora = self._relogio()
        self._ultima_msg_t = agora
        ultima = self._ultima_leitura_t
        if ultima is None or agora - ultima >= self.INTERVALO_ULTIMA_LEITURA or agora < ultima:
            self._ultima_leitura_t = agora
            self._avisar(self._ao_leitura, iso_local(agora))
        try:
            # bloqueia a rede um pouco se a fila encher (contrapressão) em vez de perder a mensagem
            fila.put((topico, payload, datetime.fromtimestamp(agora, timezone.utc), retida, agora), timeout=5)
        except queue.Full:
            self._erro("fila", "Fila de processamento cheia: mensagem descartada", topico,
                       f"{self.TAMANHO_FILA} mensagens aguardando processamento", payload)
            self._log("ERRO", f"Fila de processamento cheia: mensagem de {topico} descartada "
                              f"({trecho(payload, 40)})")

    # -- thread de processamento ---------------------------------------------------------------------

    def _processar(self, fila: queue.Queue, sink: Sink, intervalo: float = 0.0,
                   proximo: float | None = None) -> None:
        amostra = _Amostragem(intervalo, self._memoria) if proximo is not None else None
        ultimo_contador = 0.0
        enviado = (-1, -1, -1)
        limitador = _Limitador(self.LOGS_POR_SEGUNDO)
        terminar = False

        while True:
            try:
                item = fila.get(timeout=0.1)
            except queue.Empty:
                item = None

            if item is _FIM:
                terminar = True
            elif item is not None:
                try:
                    self._tratar_mensagem(item, sink, amostra, limitador)
                except Exception:  # noqa: BLE001 - uma mensagem estranha nunca derruba a thread
                    logger.exception("Falha ao tratar mensagem de %s", item[0])

            sink = self._trocar_sink_se_preciso(sink)
            if not terminar:
                self._verificar_comandos()
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
                    self._ciclo(sink, amostra, intervalo, None, fechar=True, referencia=proximo)
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
                limitador.fechar(self._log)
                return

    def _tratar_mensagem(self, item: tuple, sink: Sink, amostra: _Amostragem | None, limitador: _Limitador) -> None:
        topico, payload, recebido_em, retida, t_receb = item
        dono = self._topicos.por_topico(topico)    # tópico salvo de algum device (exato), ex. NOÏUS/.../ack/command
        dev_dono, chave_dono = dono if dono is not None else (None, None)
        eh_ack = chave_dono in _topicos.ACKS
        res = mapear(topico, payload, recebido_em, self._assinatura, self.config.mapeamento,
                     device_topico=dev_dono, resposta=eh_ack)
        if eh_ack:
            res.device_id = dev_dono    # resposta no ack daquele device é dele, diga o payload o que disser
        extra = " (retida)" if retida else ""
        if res.tipo == EVENTO:
            self._avisar_dispositivos([res.device_id], topico, t_receb)
            self._visto_para_diagnostico(res.device_id)
            if limitador.pode(self._log):
                eventos = res.objeto.get("events") if isinstance(res.objeto, dict) else None
                self._log("INFO", digirail.texto_evento(eventos, res.device_id) + extra)
            return
        if res.tipo == RESPOSTA:
            self._tratar_resposta(res, topico, limitador)
            return
        if not res.ok:
            self._erro("payload", f"Mensagem ignorada ({res.formato})", topico, res.erro, payload)
            self._log("ERRO" if res.formato != "vazio" else "AVISO", f"Mensagem de {topico} ignorada: {res.erro}")
            return

        rejeitadas = res.rejeitadas
        completas = res.completas
        for linha in rejeitadas:  # sem pid/device_id/ts_unix: vão para a contingência, contam como erro
            self._erro("payload", f"Leitura não vai para o banco ({res.formato}); guardada na "
                                  "contingência", topico, linha.motivo, payload)
        self._avisar_dispositivos([l.device_id for l in res.linhas], topico, t_receb)
        for linha in completas:
            self._visto_para_diagnostico(linha.device_id)
            self._conferir_relogio(linha, t_receb)
        self._conferir_canais(res, topico, dev_dono)

        if amostra is None:
            novas, dup = self._sem_duplicatas(completas, t_receb)
            if rejeitadas or novas:
                sink.adicionar(rejeitadas + novas)
        else:
            if rejeitadas:
                sink.adicionar(rejeitadas)  # incompletas: contingência na hora, como sem amostragem
            dup = amostra.adicionar(completas, t_receb)

        if limitador.pode(self._log):
            n = len(completas) - dup
            partes = []
            if n:
                partes.append(f"{n} leitura{'s' if n != 1 else ''}")
            if dup:
                partes.append(f"{_plural(dup, 'duplicada ignorada', 'duplicadas ignoradas')} "
                              f"({'já gravada' if dup == 1 else 'já gravadas'} ou repetida{'s' if dup != 1 else ''})")
            if partes:
                self._log("INFO", f"Mensagem de {topico}{extra}: {res.formato}, {', '.join(partes)}")
            for linha in rejeitadas:
                self._log("ERRO", f"Mensagem de {topico}{extra} ({res.formato}) não vai para o banco: "
                                  f"{linha.motivo}. Guardada na contingência.")
            for aviso in res.avisos:
                self._log("AVISO", f"{topico}: {aviso}")

    def _sem_duplicatas(self, linhas: list[LinhaIot], agora: float) -> tuple[list[LinhaIot], int]:
        """Sem amostragem: tira as leituras (device_id, ts_unix) já entregues ao sink e marca as novas."""
        novas: list[LinhaIot] = []
        for linha in linhas:
            chave = ("ts", linha.device_id, linha.ts_unix)
            if self._memoria.contem(chave, agora):
                continue
            self._memoria.adicionar(chave, agora)
            novas.append(linha)
        return novas, len(linhas) - len(novas)

    def _conferir_relogio(self, linha: LinhaIot, t_receb: float) -> None:
        """L8: leitura muito à frente do relógio do PC = relógio do dispositivo adiantado (1 AVISO por device)."""
        if linha.ts_unix is None or linha.ts_unix <= t_receb + FUTURO_MAX or linha.device_id in self._avisou_futuro:
            return
        self._avisou_futuro.add(linha.device_id)
        adiantado = duracao_texto(linha.ts_unix - t_receb)
        self._log("AVISO", f"Relógio do dispositivo {linha.device_id} adiantado {adiantado}: leitura com horário "
                           f"{iso_local(linha.ts_unix).replace('T', ' ')} chegou às "
                           f"{datetime.fromtimestamp(t_receb):%H:%M:%S}. A leitura é gravada assim mesmo; confira "
                           "o relógio/NTP no NXperience. Este aviso não se repete.")

    def _conferir_canais(self, res: ResultadoMapa, topico: str, dev_dono: str | None = None) -> None:
        obj = res.objeto
        itens = [obj] if isinstance(obj, dict) else obj if isinstance(obj, list) else []
        for item in itens:
            if not isinstance(item, dict):
                continue
            channels = next((v for k, v in item.items() if str(k).lower() == "channels"), None)
            if not isinstance(channels, dict):
                continue
            device = _device_da_mensagem(item, topico, self._assinatura, dev_dono)
            if not device:
                continue
            canais = tuple(digirail.canais_presentes(channels))
            if self._canais.get(device) != canais:
                self._canais[device] = canais
                self._avisar(self._ao_canais, device, json.dumps(list(canais)))

    # -- comandos aos dispositivos (DigiRail) ---------------------------------------------------------

    def _cfg_dispositivos(self) -> DispositivosConfig:
        return getattr(self.config, "dispositivos", None) or DispositivosConfig()

    def _visto_para_diagnostico(self, device: str | None) -> None:
        """1ª vez que o device aparece nesta conexão: pede o diagnóstico (se ``diagnostico_minutos`` > 0)."""
        if not device or device in self._diag_t:
            return
        self._diag_t[device] = self._relogio()
        if self._minutos_diag() > 0:
            self._pedir_diag(device, False)

    def _minutos_diag(self) -> float:
        try:
            return max(0.0, float(self._cfg_dispositivos().diagnostico_minutos or 0))
        except (TypeError, ValueError):
            return 0.0

    def _timeout_comando(self) -> float:
        try:
            valor = float(self._cfg_dispositivos().timeout_comando_segundos)
        except (TypeError, ValueError):
            valor = 15.0
        return valor if valor > 0 else 15.0

    def _garantir_resposta(self, cliente, device: str) -> None:
        """Assina o tópico de resposta se a assinatura do usuário não cobrir o deste device.

        Device com tópicos salvos: o ``ack_command`` exato já é extra (``_aplicar_topicos``); garante de novo aqui.
        Sem tópicos salvos: o filtro do modelo (ex. ``NOVUS/+/ack/command``), que serve a todos os devices.
        """
        salvos = self._topicos.salvos(device)
        if salvos is not None:
            ack = salvos["ack_command"]
            if _paho.topic_matches_sub(self._assinatura, ack):
                return
            assinar = getattr(cliente, "assinar_extra", None)
            if assinar is not None and ack not in getattr(cliente, "assinaturas_extras", []):
                try:
                    assinar(ack, 1)
                    self._log("INFO", f"Assinando {ack!r} para receber as respostas de {device}.")
                except ValueError as exc:
                    self._log("ERRO", f"Tópico de confirmação de comando de {device} inválido: {exc}")
            return
        modelo = self._cfg_dispositivos().topico_resposta
        if _paho.topic_matches_sub(self._assinatura, digirail.topico_do_modelo(modelo, device)):
            return
        assinar = getattr(cliente, "assinar_extra", None)
        if assinar is None:
            return
        filtro = digirail.filtro_do_modelo(modelo)
        try:
            if filtro not in getattr(cliente, "assinaturas_extras", []):
                self._log("INFO", f"Assinando {filtro} para receber as respostas dos dispositivos.")
            assinar(filtro, 1)
        except ValueError as exc:
            self._log("ERRO", f"Tópico de resposta inválido em [dispositivos] topico_resposta: {exc}")

    def _zerar(self, device_id: str, canais: list[int]) -> None:
        try:
            device = digirail.validar_device_id(device_id)
            lista = digirail.validar_canais_reset(canais)
        except ValueError as exc:
            self._log("ERRO", f"Zerar contadores: {exc}")
            self._avisar(self._ao_resposta_comando, str(device_id or ""), "reset_counters", False, str(exc))
            return
        nomes = ", ".join(f"chd{c}" for c in lista)
        cmd = self._enviar(device, "reset_counters", digirail.desejado_reset(lista), manual=True, canais=lista)
        if cmd is not None:
            self._log("INFO", f"Comando enviado a {device}: zerar contadores ({nomes}) em {cmd.topico}, "
                              f"timestamp {cmd.timestamp}. Aguardando resposta…")

    def _pedir_diag(self, device_id: str, manual: bool) -> None:
        try:
            device = digirail.validar_device_id(device_id)
        except ValueError as exc:
            if manual:
                self._log("ERRO", f"Diagnóstico: {exc}")
                self._avisar(self._ao_resposta_comando, str(device_id or ""), "diag", False, str(exc))
            return
        with self._trava_cmd:
            if any(c.device_id == device and c.comando == "diag" for c in self._comandos.values()):
                if not manual:
                    return
        cmd = self._enviar(device, "diag", {}, manual=manual)
        if cmd is not None:
            self._log("INFO", f"Pedido de diagnóstico enviado a {device} ({'manual' if manual else 'automático'}).")

    def _enviar(self, device: str, comando: str, desejado: dict, manual: bool,
                canais: list[int] | None = None) -> _Comando | None:
        """Publica o comando e registra como pendente. Em falha, avisa e devolve None."""
        efetivos = self._topicos.efetivos(device)
        topico, ack = efetivos["command"], efetivos["ack_command"]
        cliente = self._cliente
        publicar = getattr(cliente, "publicar", None)
        if cliente is None or publicar is None:
            self._falha_comando(_Comando(device, comando, 0, self._relogio(), topico, manual, canais or [], ack),
                                "Não conectado ao broker.", registrar=False)
            return None
        agora = self._relogio()
        with self._trava_cmd:
            ts = int(agora)
            while (device, ts) in self._comandos:   # o timestamp é o id de correlação: único por device
                ts += 1
            cmd = _Comando(device, comando, ts, agora, topico, manual, canais or [], ack)
            self._comandos[(device, ts)] = cmd
        self._garantir_resposta(cliente, device)
        self._topicos_publicados.add(topico)
        try:
            ok, motivo = publicar(topico, digirail.payload_comando(ts, comando, desejado), 1)
        except Exception as exc:  # noqa: BLE001
            ok, motivo = False, f"{type(exc).__name__}: {exc}"
        if not ok:
            with self._trava_cmd:
                self._comandos.pop((device, ts), None)
            self._falha_comando(cmd, f"Não foi possível publicar em {topico}: {motivo}.",
                                registrar=motivo != "não conectado ao broker")
            return None
        return cmd

    def _falha_comando(self, cmd: _Comando, mensagem: str, registrar: bool = True, sem_resposta: bool = False
                       ) -> None:
        """Falha de comando. ``reset_counters``: ERRO, erro "comando" (se ``registrar``) e resposta; ``diag``: AVISO.

        ``diag`` manual sempre responde em ``ao_resposta_comando``; o automático só quando ``sem_resposta`` (timeout),
        com a mensagem citando os tópicos do pedido e da resposta esperada (dica na aba Clientes).
        """
        if cmd.comando == "reset_counters":
            self._log("ERRO", f"Zerar contadores em {cmd.device_id} falhou: {mensagem}")
            if registrar:
                nomes = ", ".join(f"chd{c}" for c in cmd.canais) or "-"
                self._erro("comando", f"Zerar contadores em {cmd.device_id} falhou", cmd.topico,
                           f"{mensagem} Canais: {nomes}; timestamp do pedido: {cmd.timestamp}.")
            self._avisar(self._ao_resposta_comando, cmd.device_id, cmd.comando, False, mensagem)
        elif cmd.manual:
            self._log("AVISO", f"Diagnóstico de {cmd.device_id}: {mensagem}")
            self._avisar(self._ao_resposta_comando, cmd.device_id, cmd.comando, False, mensagem)
        elif sem_resposta:
            detalhada = (f"{mensagem.rstrip('.')} (pedido em '{cmd.topico}', resposta esperada em "
                         f"'{cmd.topico_resposta}').")
            self._log("AVISO", f"Diagnóstico de {cmd.device_id}: {detalhada}")
            self._avisar(self._ao_resposta_comando, cmd.device_id, cmd.comando, False, detalhada)
        else:
            self._log("AVISO", f"Diagnóstico de {cmd.device_id}: {mensagem}")

    def _verificar_comandos(self) -> None:
        """Na thread de processamento: timeouts e diagnóstico periódico."""
        agora = self._relogio()
        limite = self._timeout_comando()
        with self._trava_cmd:
            vencidos = [c for c in self._comandos.values() if agora - c.inicio >= limite]
            for c in vencidos:
                self._comandos.pop((c.device_id, c.timestamp), None)
        for c in vencidos:
            self._falha_comando(c, f"Sem resposta do dispositivo em {limite:g} s.", sem_resposta=True)
        minutos = self._minutos_diag()
        if minutos > 0:
            for device, t in list(self._diag_t.items()):
                if agora - t >= minutos * 60 or agora < t:
                    self._diag_t[device] = agora
                    self._pedir_diag(device, False)

    def _cancelar_comandos(self, mensagem: str) -> None:
        """Desconexão: os pendentes falham sem contar erro (não foi culpa do dispositivo)."""
        with self._trava_cmd:
            pendentes, self._comandos = list(self._comandos.values()), {}
        for c in pendentes:
            self._falha_comando(c, mensagem, registrar=False)

    def _pendente(self, device: str | None, ts: int | None, comando: str) -> _Comando | None:
        """Tira o comando pendente que casa com a resposta (device + timestamp; ou só timestamp, se único)."""
        if ts is None:
            return None
        with self._trava_cmd:
            cmd = self._comandos.get((device or "", ts))
            if cmd is None or cmd.comando != comando:
                candidatos = [c for c in self._comandos.values() if c.timestamp == ts and c.comando == comando]
                cmd = candidatos[0] if len(candidatos) == 1 else None
            if cmd is not None:
                self._comandos.pop((cmd.device_id, cmd.timestamp), None)
            return cmd

    def _tratar_resposta(self, res: ResultadoMapa, topico: str, limitador: _Limitador) -> None:
        """Respostas e pedidos (``reported``/``desired``/``ack/*``): nunca são leitura nem erro."""
        obj = res.objeto
        device = res.device_id
        reported = obj.get("reported") if isinstance(obj, dict) else None
        if not isinstance(reported, dict):
            if isinstance(obj, dict) and "desired" in obj:
                logger.debug("Pedido (desired) de outra aplicação em %s ignorado", topico)
            elif limitador.pode(self._log):
                motivo = "; ".join(res.avisos) or f"formato {res.formato} não reconhecido"
                self._log("AVISO", f"Resposta de {device or topico} ignorada ({motivo}); não é leitura nem erro.")
            return
        ts = digirail.timestamp_resposta(obj)
        for comando, dados in reported.items():
            if comando == "diag":
                diag = dados if isinstance(dados, dict) else {}
                cmd = self._pendente(device, ts, "diag")
                quem = cmd.device_id if cmd is not None else device
                if quem:
                    self._avisar(self._ao_diagnostico, quem, json.dumps(diag, ensure_ascii=False))
                origem = "" if cmd is not None else " (enviado pelo próprio dispositivo)"
                self._log("INFO", f"Diagnóstico de {quem or topico}{origem}: {digirail.resumo_diag(diag)}")
                if cmd is not None and cmd.manual:
                    self._avisar(self._ao_resposta_comando, cmd.device_id, "diag", True, "Diagnóstico recebido.")
            elif comando == "reset_counters":
                cmd = self._pendente(device, ts, "reset_counters")
                erro = dados.get("error") if isinstance(dados, dict) else None
                if cmd is None:
                    self._log("INFO", f"Resposta de zerar contadores de {device or topico} (error={erro}) sem "
                                      "pedido pendente deste programa; ignorada.")
                    continue
                ok, msg = digirail.mensagem_reset(erro, cmd.canais)
                if ok:
                    self._log("INFO", f"{cmd.device_id}: {msg}")
                    self._avisar(self._ao_resposta_comando, cmd.device_id, "reset_counters", True, msg)
                else:
                    self._falha_comando(cmd, msg)
            elif limitador.pode(self._log):
                erro = dados.get("error") if isinstance(dados, dict) else None
                self._log("INFO", f"Resposta '{comando}' de {device or topico}"
                                  f"{f' (error={erro})' if erro not in (None, 0) else ''}; não é leitura.")

    def _ciclo(self, sink: Sink, amostra: _Amostragem, intervalo: float, momento: float | None,
               fechar: bool = False, referencia: float | None = None) -> float | None:
        """Grava os minutos de ts_unix encerrados. ``momento`` = virada (epoch); None = desconexão (grava tudo).

        Devolve o próximo ciclo (None no encerramento) e avisa ``ao_proxima_gravacao``.
        """
        agora = self._relogio()
        normais, recuperados, dup = amostra.retirar(momento, agora, momento if momento is not None else referencia)
        linhas = sorted((b.linha for b in normais + recuperados), key=lambda l: l.ts_unix)   # ordem cronológica
        if momento is not None:
            rotulo = f"{datetime.fromtimestamp(momento):%H:%M}"
        else:
            rotulo = f"{datetime.fromtimestamp(agora):%H:%M:%S} (ao desconectar)"
        if linhas:
            sink.adicionar(linhas)
        n, n_sql = (self._descarregar(sink, fechar=True) if fechar
                    else self._descarregar(sink) if linhas else (0, 0))
        sufixo_dup = f"; {_plural(dup, 'duplicada ignorada', 'duplicadas ignoradas')}" if dup else ""
        if linhas:
            total = len(linhas)
            if n_sql >= total:
                verbo, onde = ("gravada", "gravadas"), " no SQL Server"
            elif n_sql > 0:
                verbo, onde = ("gravada", "gravadas"), f" ({n_sql} no SQL Server e {total - n_sql} na contingência)"
            elif n > 0:
                verbo, onde = ("guardada", "guardadas"), " na contingência"
            else:
                verbo, onde = ("não gravada", "não gravadas"), " (ver erro acima)"
            n_norm, n_rec = len(normais), len(recuperados)
            r_norm = sum(b.recebidas for b in normais)
            r_rec = sum(b.recebidas for b in recuperados)
            if n_norm:
                texto = (f"{_plural(n_norm, 'leitura ' + verbo[0], 'leituras ' + verbo[1])}{onde} "
                         f"({_plural(r_norm, 'recebida', 'recebidas')}, "
                         f"{_plural(r_norm - n_norm, 'descartada', 'descartadas')} pela amostragem)")
                if n_rec:
                    texto += f" + {_plural(n_rec, 'recuperada', 'recuperadas')} da memória do dispositivo"
                    if r_rec > n_rec:
                        texto += f" ({r_rec} recebidas, {_plural(r_rec - n_rec, 'descartada', 'descartadas')})"
            else:
                texto = (f"{_plural(n_rec, 'leitura ' + verbo[0], 'leituras ' + verbo[1])}{onde}, "
                         f"{'recuperada' if n_rec == 1 else 'recuperadas'} da memória do dispositivo "
                         f"({_plural(r_rec, 'recebida', 'recebidas')}, "
                         f"{_plural(r_rec - n_rec, 'descartada', 'descartadas')} pela amostragem)")
            self._log("INFO", f"Gravação {rotulo}: {texto}{sufixo_dup}")
        elif momento is not None:
            if dup:
                self._log("INFO", f"Gravação {rotulo}: nada novo para gravar "
                                  f"({_plural(dup, 'duplicada ignorada', 'duplicadas ignoradas')})")
            else:
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
            if isinstance(exc, OSError):
                self._erro("contingencia", "Falha ao gravar a contingência; os registros ficam na memória e "
                                           "serão tentados de novo", detalhe=f"{type(exc).__name__}: {exc}")
            else:
                self._erro("sql", "Falha ao gravar registros", detalhe=f"{type(exc).__name__}: {exc}")
            self._log("ERRO", f"Falha ao gravar registros: {exc}")
        retirar = getattr(sink, "retirar_erros", None)
        if retirar is not None:
            for e in retirar():
                self._erro("sql", e.get("mensagem") or "Falha ao gravar no SQL Server", e.get("topico"),
                           e.get("detalhe"), e.get("payload"))
        else:
            falhas = getattr(sink, "falhas", None)
            for _ in range(falhas() if falhas is not None else 0):
                self._erro("sql", "Falha ao gravar no SQL Server")
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
