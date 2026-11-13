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
    lote_max: int = 500           # teto de registros por INSERT em lote
    # só com amostragem (ou sem SQL): espera máxima para juntar registros; com intervalo 0 a gravação é imediata
    lote_segundos: float = 1.0
    pasta_contingencia: str = "contingencia"
    # registros da janela de erros (erros_AAAAMMDD.jsonl); relativa à pasta do programa, como a contingência
    pasta_logs: str = "logs"
    # 0 (padrão): grava cada leitura completa assim que chega (as que chegam juntas, como o reenvio da memória do
    # dispositivo, viram um lote de até lote_max). Amostragem só se configurada (> 0): a cada ciclo alinhado ao
    # relógio (60 = hh:mm:00) grava 1 leitura por device_id por minuto de ts_unix
    intervalo_gravacao_segundos: int = 0


@dataclass
class MapeamentoConfig:
    """Chaves aceitas no payload para cada coluna da dbo.IOT001 (sem diferenciar maiúsculas).

    Um ``.`` indica caminho aninhado no JSON (``chd1.value`` = ``{"chd1": {"value": ...}}``). A primeira chave
    encontrada vence. Ver app/core/mapeamento.py.
    """
    # No NOVUS DigiRail, ``pid`` é o código do modelo do produto (constante: ETH 51584019, WRL 51518482), não um
    # número de pacote nem de sequência: não serve para deduplicar (a chave da leitura é device_id + ts_unix).
    # "pid_id" aparece num exemplo do manual (p. 20).
    pid: list[str] = field(default_factory=lambda: ["pid", "pid_id", "product_id", "id_produto"])
    device_id: list[str] = field(default_factory=lambda: [
        "device_id", "deviceid", "device", "dispositivo", "id_dispositivo", "serial", "serial_number", "sn"])
    ts_unix: list[str] = field(default_factory=lambda: [
        "ts_unix", "ts", "timestamp", "time", "epoch", "unix", "datahora", "data_hora", "datetime"])
    chd1: list[str] = field(default_factory=lambda: ["chd1_value", "chd1.value", "chd1"])
    chd2: list[str] = field(default_factory=lambda: ["chd2_value", "chd2.value", "chd2"])
    chd3: list[str] = field(default_factory=lambda: ["chd3_value", "chd3.value", "chd3"])
    # chd4..chd6: gravados nas colunas chd4_value..chd6_value (sql/canais_4_a_6_iot001.sql); enquanto o script não
    # for rodado, o sink detecta que as colunas não existem e os valores ficam só no payload_json
    chd4: list[str] = field(default_factory=lambda: ["chd4_value", "chd4.value", "chd4"])
    chd5: list[str] = field(default_factory=lambda: ["chd5_value", "chd5.value", "chd5"])
    chd6: list[str] = field(default_factory=lambda: ["chd6_value", "chd6.value", "chd6"])
    # chave que traz uma lista de leituras dentro de um objeto (campos de fora valem para todos os itens)
    leituras: list[str] = field(default_factory=lambda: ["leituras", "readings", "dados", "data", "registros"])
    # sem device_id no payload: usa o {id} de NOVUS/{id}/events|config|command|ack/...; nos outros tópicos, o nível
    # que casa com o primeiro curinga da assinatura
    device_do_topico: bool = True
    fuso_sem_offset: str = "local"   # data/hora ISO sem fuso: "local" (do PC) ou "utc"
    colunas_sem_cabecalho: list[str] = field(default_factory=list)  # tabular sem cabeçalho: nomes das colunas


@dataclass
class VigiaConfig:
    """Vigia (watchdog): reinicia o app se ficar `minutos` sem gravação com sucesso no SQL (ou sem leitura, quando o
    SQL não está configurado). 0 desliga. A aba Atividade mostra a contagem regressiva."""
    minutos: float = 60


@dataclass
class InicioConfig:
    """O que fazer ao abrir o programa."""
    # testa o banco e conecta no broker sozinho, com os valores salvos (--auto-conectar força mesmo com False)
    conectar_automaticamente: bool = True


@dataclass
class MosquittoConfig:
    """Broker Mosquitto local rodando como processo filho do app (aba Mosquitto).

    `comando`: o primeiro token é o programa (".exe" é acrescentado se faltar), o resto são os argumentos.
    O prompt mostrado no console é sempre ``<pasta>><comando>``. `pasta` é só o padrão: o campo "Endereço
    Mosquitto" da aba salva o valor escolhido em QSettings (``mosquitto/pasta``), que vale mais que este.
    """
    pasta: str = "C:\\Program Files\\mosquitto168"
    comando: str = "mosquitto -v"
    iniciar_automaticamente: bool = True


@dataclass
class DispositivosConfig:
    """Comandos aos dispositivos NOVUS DigiRail (o integrador publica no broker). ``{id}`` = device_id."""
    # padrões dos 5 tópicos de cada device; os de um device específico podem ser trocados na aba Clientes
    # ("Tópicos…") e ficam em ``arquivo_topicos`` (ver app/core/topicos.py)
    topico_dados: str = "NOVUS/{id}/events"
    topico_config: str = "NOVUS/{id}/config"
    topico_ack_config: str = "NOVUS/{id}/ack/config"
    topico_comando: str = "NOVUS/{id}/command"
    topico_resposta: str = "NOVUS/{id}/ack/command"   # confirmação de comando (ack_command)
    arquivo_topicos: str = "dispositivos.json"        # relativo à pasta do programa (pasta_base())
    diagnostico_minutos: float = 1          # pede "diag" ao ver o device em cada conexão e a cada N min; 0 desliga
    timeout_comando_segundos: float = 15    # sem resposta nesse tempo: "sem resposta do dispositivo"


@dataclass
class ReinicioConfig:
    """Botão Reiniciar da aba Clientes: liga a saída digital ``outN`` do DigiRail (manual 6.5.1 OUTPUT), que aciona
    o relé ligado pelo usuário e corta a alimentação do próprio aparelho. Ver ``Pipeline.reiniciar_dispositivo``."""
    saida: int = 1                          # 1 (out1) ou 2 (out2)
    pulso_segundos: float = 5               # depois disso: se o aparelho ainda responde, o relé não funcionou
    timeout_volta_segundos: float = 180     # sem nenhuma mensagem do aparelho nesse tempo: falha "Não voltou"


@dataclass
class OeeConfig:
    """Cadastro dos dispositivos no portal OEE (Laravel, outro repositório), no mesmo SQL Server do integrador.

    Usa a mesma conexão/login de ``[sqlserver]`` e o nome em 3 partes ``[banco].[schema].[tabela]``. Se o banco, a
    tabela ou a permissão (INSERT/UPDATE) faltarem, a etapa se desliga em silêncio (1 INFO). O integrador nunca cria
    nem altera a tabela do OEE; ver app/core/oee_dispositivos.py e sql/permissoes_oee.sql.
    """
    habilitado: bool = True
    banco: str = "OEE"                      # no .env do OEE pode ser outro (ex.: OEE_Alumbra)
    tabela: str = "dbo.dispositivos_iot"
    unidade: str = "manutencao"             # manutencao | tecnologia | ambos (CHECK no portal)
    tipo: str = "contador"                  # sensor | clp | gateway | contador | outro


@dataclass
class ApontamentosConfig:
    """Apontamentos reais pelo IoT no portal OEE (``apontamentos_estado``), ver docs/apontamentos_reais.md.

    Usa o login de ``[sqlserver]`` e o banco do OEE de ``[oee] banco``. ``minutos`` é só o padrão: a periodicidade
    escolhida na aba "Apontamentos reais" fica em QSettings e vale mais. As legendas são pelo ``codigo`` (já
    cadastradas no portal; o integrador não cria legendas).
    """
    habilitado: bool = True
    minutos: float = 3                      # periodicidade do ciclo
    legenda_produzindo: str = "2"           # Produção Normal
    legenda_micro: str = "99"               # Microparada
    legenda_macro: str = "98"               # Macro parada
    legenda_sem_iot: str = "97"             # Sem apontamento IOT (aparelho sem leitura); ausente = como antes
    legenda_fora_turno: str = "96"          # Fora de turno (parada fora da jornada da máquina); ausente = sem 96
    sem_leitura_segundos: int = 120         # mais que isso sem nenhuma linha na IOT001 = sem leitura (97)
    dias_correcao: int = 7                  # 97 com leitura nova até N dias atrás é recalculado (dados retroativos)
    fuso: str = "America/Sao_Paulo"         # dia local (um apontamento não passa da meia-noite)


@dataclass
class Config:
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    sqlserver: SqlServerConfig = field(default_factory=SqlServerConfig)
    gravacao: GravacaoConfig = field(default_factory=GravacaoConfig)
    mapeamento: MapeamentoConfig = field(default_factory=MapeamentoConfig)
    vigia: VigiaConfig = field(default_factory=VigiaConfig)
    inicio: InicioConfig = field(default_factory=InicioConfig)
    mosquitto: MosquittoConfig = field(default_factory=MosquittoConfig)
    dispositivos: DispositivosConfig = field(default_factory=DispositivosConfig)
    reinicio: ReinicioConfig = field(default_factory=ReinicioConfig)
    oee: OeeConfig = field(default_factory=OeeConfig)
    apontamentos: ApontamentosConfig = field(default_factory=ApontamentosConfig)


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
        for secao in ("mqtt", "sqlserver", "gravacao", "mapeamento", "vigia", "inicio", "mosquitto",
                      "dispositivos", "reinicio", "oee", "apontamentos"):
            _preencher(getattr(cfg, secao), dados.get(secao, {}))
    return cfg
