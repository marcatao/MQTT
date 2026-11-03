"""Configuração do app: lê config.toml (se existir) sobre os valores padrão."""
from __future__ import annotations

import sys
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path


def pasta_base() -> Path:
    """Pasta do .exe quando empacotado, ou a raiz do projeto em desenvolvimento."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parent.parent


@dataclass
class MqttConfig:
    broker: str = "192.168.100.162"
    porta: int = 1883
    topico: str = "NOVUS/Teste_Alumbra/#"
    qos: int = 2
    usuario: str = ""
    senha: str = ""


@dataclass
class SqlServerConfig:
    servidor: str = "TERRA\\SOLO"
    banco: str = "MQTT"
    tabela: str = "dbo.IOT001"
    driver: str = "ODBC Driver 18 for SQL Server"
    autenticacao_windows: bool = False
    usuario: str = "mqtt"
    senha: str = ""  # nunca no git: vem da tela (Gerenciador de Credenciais do Windows) ou do config.toml local
    criptografar: str = "Optional"  # Optional | Mandatory | Strict (ODBC Driver 18)
    trust_server_certificate: bool = False
    timeout_conexao: int = 5

    @property
    def habilitado(self) -> bool:
        return bool(self.servidor and self.banco)


@dataclass
class GravacaoConfig:
    lote_max: int = 500
    lote_segundos: float = 1.0
    pasta_contingencia: str = "contingencia"
    # amostragem: a cada ciclo (alinhado ao relógio, ex. hh:mm:00) grava só a leitura completa mais recente de
    # cada device_id; 0 desliga e grava todas as leituras (em lote, por lote_max/lote_segundos)
    intervalo_gravacao_segundos: int = 60


@dataclass
class MapeamentoConfig:
    """Chaves aceitas no payload para cada coluna da dbo.IOT001 (sem diferenciar maiúsculas).

    Um ``.`` indica caminho aninhado no JSON (``chd1.value`` = ``{"chd1": {"value": ...}}``). A primeira chave
    encontrada vence. Ver app/core/mapeamento.py.
    """
    pid: list[str] = field(default_factory=lambda: ["pid", "packet_id", "id_pacote", "seq", "sequencia"])
    device_id: list[str] = field(default_factory=lambda: [
        "device_id", "deviceid", "device", "dispositivo", "id_dispositivo", "serial", "serial_number", "sn"])
    ts_unix: list[str] = field(default_factory=lambda: [
        "ts_unix", "ts", "timestamp", "time", "epoch", "unix", "datahora", "data_hora", "datetime"])
    chd1: list[str] = field(default_factory=lambda: ["chd1_value", "chd1.value", "chd1"])
    chd2: list[str] = field(default_factory=lambda: ["chd2_value", "chd2.value", "chd2"])
    chd3: list[str] = field(default_factory=lambda: ["chd3_value", "chd3.value", "chd3"])
    # chave que traz uma lista de leituras dentro de um objeto (campos de fora valem para todos os itens)
    leituras: list[str] = field(default_factory=lambda: ["leituras", "readings", "dados", "data", "registros"])
    device_do_topico: bool = True    # sem device_id no payload: usa o nível do tópico após o prefixo assinado
    fuso_sem_offset: str = "local"   # data/hora ISO sem fuso: "local" (do PC) ou "utc"
    colunas_sem_cabecalho: list[str] = field(default_factory=list)  # tabular sem cabeçalho: nomes das colunas


@dataclass
class VigiaConfig:
    """Vigia (watchdog): reinicia o app se ficar `minutos` sem leitura (ou sem gravação no SQL). 0 desliga."""
    minutos: float = 10


@dataclass
class InicioConfig:
    """O que fazer ao abrir o programa."""
    # testa o banco e conecta no broker sozinho, com os valores salvos (--auto-conectar força mesmo com False)
    conectar_automaticamente: bool = True


@dataclass
class MosquittoConfig:
    """Broker Mosquitto local rodando como processo filho do app (aba Mosquitto).

    `comando`: o primeiro token é o programa (".exe" é acrescentado se faltar), o resto são os argumentos.
    O prompt mostrado no console é sempre ``<pasta>><comando>``.
    """
    pasta: str = "C:\\Program Files\\mosquitto168"
    comando: str = "mosquitto -v"
    iniciar_automaticamente: bool = True


@dataclass
class Config:
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    sqlserver: SqlServerConfig = field(default_factory=SqlServerConfig)
    gravacao: GravacaoConfig = field(default_factory=GravacaoConfig)
    mapeamento: MapeamentoConfig = field(default_factory=MapeamentoConfig)
    vigia: VigiaConfig = field(default_factory=VigiaConfig)
    inicio: InicioConfig = field(default_factory=InicioConfig)
    mosquitto: MosquittoConfig = field(default_factory=MosquittoConfig)


def _preencher(destino, dados: dict) -> None:
    validos = {f.name for f in fields(destino)}
    for chave, valor in dados.items():
        if chave in validos:
            setattr(destino, chave, valor)


def carregar(caminho: Path | None = None) -> Config:
    caminho = caminho or pasta_base() / "config.toml"
    cfg = Config()
    if caminho.exists():
        with caminho.open("rb") as f:
            dados = tomllib.load(f)
        for secao in ("mqtt", "sqlserver", "gravacao", "mapeamento", "vigia", "inicio", "mosquitto"):
            _preencher(getattr(cfg, secao), dados.get(secao, {}))
    return cfg
