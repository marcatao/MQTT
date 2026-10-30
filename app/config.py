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
    tabela: str = "dbo.LeiturasMqtt"
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


@dataclass
class Config:
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    sqlserver: SqlServerConfig = field(default_factory=SqlServerConfig)
    gravacao: GravacaoConfig = field(default_factory=GravacaoConfig)


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
        for secao in ("mqtt", "sqlserver", "gravacao"):
            _preencher(getattr(cfg, secao), dados.get(secao, {}))
    return cfg
