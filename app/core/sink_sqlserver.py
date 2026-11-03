"""Destinos das linhas da ``dbo.IOT001``: SQL Server (em lote) e arquivo de contingência (JSON Lines).

Recebe ``LinhaIot`` (``app.core.mapeamento``). Linhas incompletas (``motivo`` preenchido: sem pid, device_id ou
ts_unix) nunca vão ao banco: seguem direto para a contingência e não contam como gravadas (o pipeline já as contou
como erro). Só as colunas ``pid, device_id, ts_unix, chd1..3_value, payload_json, recebido_em`` são inseridas.

``SinkSqlServer`` junta registros e grava quando o lote chega a ``lote_max`` ou passa ``lote_segundos`` (o que vier
primeiro), numa transação, com ``executemany`` parametrizado e ``fast_executemany``. Erros são classificados pelo
SQLSTATE (``classificar_erro``):

- conexão (08xxx, HYT00/HYT01, driver ausente): reconecta e repete com backoff (na 1ª queda 08xxx, repete na hora);
- esquema (42xxx: tabela/permissão): não repete;
- dados (22xxx/23xxx): bissecção do lote; só as linhas ruins vão para a contingência, sem pausar o banco.

Se o banco continuar falhando (ou erro de esquema), o lote vai para a contingência (nada é descartado) e o sink
espera ``pausa_apos_falha`` segundos antes de tentar o banco de novo (para não travar a fila a cada lote). No
fechamento há uma única tentativa, para caber no ``encerrar(timeout)``.

A conexão é aberta por ``app.core.sql_conexao.conectar`` (dono: agente sqlserver); o pyodbc só é importado lá.
"""
from __future__ import annotations

import os
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Protocol

from app.config import GravacaoConfig, SqlServerConfig
from app.core import sql_conexao
from app.core.sql_conexao import DriverOdbcAusente
from app.core.mapeamento import LinhaIot

try:
    import orjson as _orjson

    def _linha_json(d: dict) -> bytes:
        return _orjson.dumps(d) + b"\n"
except ImportError:  # pragma: no cover
    import json as _json

    def _linha_json(d: dict) -> bytes:
        return (_json.dumps(d, ensure_ascii=False) + "\n").encode("utf-8")


Logar = Callable[[str, str], None]  # (nivel "INFO"|"AVISO"|"ERRO", mensagem)


def _sem_log(nivel: str, msg: str) -> None:
    pass


class Sink(Protocol):
    descricao: str

    def adicionar(self, registros: Iterable[LinhaIot]) -> None: ...
    def pendentes(self) -> int: ...
    def deve_descarregar(self) -> bool: ...
    def descarregar(self) -> int: ...   # devolve quantos registros foram persistidos (SQL ou contingência)
    def fechar(self) -> int: ...        # descarrega o que restou e libera recursos


def montar_insert(tabela: str) -> str:
    """INSERT parametrizado; o nome da tabela é validado (só ``[A-Za-z0-9_]``, ``.`` e colchetes)."""
    alvo = sql_conexao.tabela_citada(tabela)
    colunas = ", ".join(f"[{c}]" for c in LinhaIot.COLUNAS)
    marcadores = ", ".join("?" for _ in LinhaIot.COLUNAS)
    return f"INSERT INTO {alvo} ({colunas}) VALUES ({marcadores})"


# ------------------------------------------------------------------------------------------ contingência


class Contingencia:
    """Grava registros em ``<pasta>/leituras_AAAAMMDD.jsonl`` (um JSON por linha, append)."""

    def __init__(self, pasta: str | Path) -> None:
        self.pasta = Path(pasta)
        self._trava = threading.Lock()

    def arquivo_do_dia(self) -> Path:
        return self.pasta / f"leituras_{datetime.now():%Y%m%d}.jsonl"

    def gravar(self, registros: list[LinhaIot]) -> Path:
        if not registros:
            return self.arquivo_do_dia()
        dados = b"".join(_linha_json(r.como_json()) for r in registros)
        with self._trava:
            self.pasta.mkdir(parents=True, exist_ok=True)
            caminho = self.arquivo_do_dia()
            with caminho.open("ab") as f:
                f.write(dados)
                f.flush()
                os.fsync(f.fileno())
        return caminho


class _Lote:
    """Buffer com gatilho por quantidade ou tempo."""

    def __init__(self, lote_max: int, lote_segundos: float) -> None:
        self.lote_max = max(1, int(lote_max))
        self.lote_segundos = max(0.05, float(lote_segundos))
        self.itens: list[LinhaIot] = []
        self._inicio: float | None = None

    def adicionar(self, registros: Iterable[LinhaIot]) -> None:
        antes = len(self.itens)
        self.itens.extend(registros)
        if antes == 0 and self.itens:
            self._inicio = time.monotonic()

    def pronto(self) -> bool:
        if not self.itens:
            return False
        return len(self.itens) >= self.lote_max or (time.monotonic() - (self._inicio or 0)) >= self.lote_segundos

    def retirar(self) -> list[LinhaIot]:
        itens, self.itens, self._inicio = self.itens, [], None
        return itens


class SinkContingencia:
    """Sem SQL Server: os registros vão direto para a contingência (em lote, para não escrever a cada mensagem)."""

    def __init__(self, gravacao: GravacaoConfig, logar: Logar = _sem_log,
                 pasta_base: Path | None = None) -> None:
        pasta = Path(gravacao.pasta_contingencia)
        if not pasta.is_absolute() and pasta_base is not None:
            pasta = pasta_base / pasta
        self.contingencia = Contingencia(pasta)
        self.descricao = f"contingência ({pasta})"
        self._lote = _Lote(gravacao.lote_max, gravacao.lote_segundos)
        self._logar = logar
        self.logar_sucesso = True  # False: o pipeline (amostragem) faz um log por ciclo no lugar deste

    def adicionar(self, registros: Iterable[LinhaIot]) -> None:
        self._lote.adicionar(registros)

    def pendentes(self) -> int:
        return len(self._lote.itens)

    def deve_descarregar(self) -> bool:
        return self._lote.pronto()

    def descarregar(self) -> int:
        """Grava tudo na contingência; devolve quantas linhas **completas** foram guardadas."""
        itens = self._lote.retirar()
        if not itens:
            return 0
        try:
            caminho = self.contingencia.gravar(itens)
        except OSError as exc:
            self._lote.adicionar(itens)  # mantém em memória e tenta no próximo ciclo
            self._logar("ERRO", f"Falha ao gravar a contingência: {exc}")
            raise
        if self.logar_sucesso:
            self._logar("INFO", f"Guardados {len(itens)} registros na contingência ({caminho.name})")
        return sum(1 for i in itens if i.completa)

    def fechar(self) -> int:
        return self.descarregar()

    def falhas(self) -> int:
        return 0

    def definir_sql(self, sql: SqlServerConfig) -> None:  # sem banco: nada a trocar
        pass


# --------------------------------------------------------------------------------------------- SQL Server


class SinkSqlServer:
    """Grava em lote no SQL Server com retry/backoff e contingência como último recurso."""

    def __init__(
        self,
        sql: SqlServerConfig,
        gravacao: GravacaoConfig,
        logar: Logar = _sem_log,
        pasta_base: Path | None = None,
        tentativas: int = 3,
        backoff_inicial: float = 0.5,
        pausa_apos_falha: float = 30.0,
        conectar: Callable[[SqlServerConfig], object] | None = None,
        dormir: Callable[[float], None] = time.sleep,
    ) -> None:
        self.sql_insert = montar_insert(sql.tabela)  # valida a tabela já na criação
        self._sql = sql
        self._sql_novo: SqlServerConfig | None = None
        self._trava_cfg = threading.Lock()
        self._logar = logar
        self._conectar = conectar  # None → sql_conexao.conectar (resolvido na hora, facilita testes)
        self._dormir = dormir
        self.tentativas = max(1, tentativas)
        self.backoff_inicial = backoff_inicial
        self.pausa_apos_falha = pausa_apos_falha
        self._lote = _Lote(gravacao.lote_max, gravacao.lote_segundos)
        self._conexao = None
        self._indisponivel_ate = 0.0
        self._erros_lote = 0
        self._fallback = SinkContingencia(gravacao, logar, pasta_base)
        self.gravados_sql = 0      # total de linhas com commit no SQL Server (o pipeline usa para "última gravação ok")
        self._logar_sucesso = True
        self.descricao = f"SQL Server {sql.servidor}/{sql.banco} ({sql.tabela})"

    @property
    def logar_sucesso(self) -> bool:
        return self._logar_sucesso

    @logar_sucesso.setter
    def logar_sucesso(self, valor: bool) -> None:
        """False: não loga "Gravados N registros" por lote (o pipeline faz um log por ciclo de amostragem)."""
        self._logar_sucesso = valor
        self._fallback.logar_sucesso = valor

    def _log_gravados(self, n: int) -> None:
        if self._logar_sucesso:
            self._logar("INFO", f"Gravados {n} registros no SQL Server")

    # -- configuração --------------------------------------------------------------------------------

    def definir_sql(self, sql: SqlServerConfig) -> None:
        """Troca a configuração; vale a partir do próximo lote (a conexão atual é fechada lá)."""
        montar_insert(sql.tabela)  # valida antes de aceitar
        with self._trava_cfg:
            self._sql_novo = sql

    def _aplicar_config_nova(self) -> None:
        with self._trava_cfg:
            novo, self._sql_novo = self._sql_novo, None
        if novo is None:
            return
        self._fechar_conexao()
        self._sql = novo
        self.sql_insert = montar_insert(novo.tabela)
        self.descricao = f"SQL Server {novo.servidor}/{novo.banco} ({novo.tabela})"
        self._indisponivel_ate = 0.0
        self._logar("INFO", f"Configuração do SQL Server atualizada: {self.descricao}")

    # -- interface Sink --------------------------------------------------------------------------------

    def adicionar(self, registros: Iterable[LinhaIot]) -> None:
        self._lote.adicionar(registros)

    def pendentes(self) -> int:
        return len(self._lote.itens)

    def deve_descarregar(self) -> bool:
        return self._lote.pronto()

    def descarregar(self, ultima: bool = False) -> int:
        """Grava o lote pendente. ``ultima=True`` (fechamento): uma única tentativa, para caber no timeout."""
        self._aplicar_config_nova()
        todos = self._lote.retirar()
        incompletas = [i for i in todos if not i.completa]
        if incompletas:
            self._para_contingencia(incompletas)  # sem pid/device_id/ts_unix: não vão ao banco
        itens = [i for i in todos if i.completa]
        if not itens:
            return 0
        if time.monotonic() < self._indisponivel_ate:
            return self._para_contingencia(itens)

        tentativas = 1 if ultima else self.tentativas
        espera = self.backoff_inicial
        tentou_na_hora = False
        tentativa = 0
        while True:
            tentativa += 1
            try:
                self._inserir(itens)
                self._log_gravados(len(itens))
                return len(itens)
            except Exception as exc:  # noqa: BLE001 - pyodbc.Error, DriverOdbcAusente etc.
                classe, sqlstate = classificar_erro(exc)
                if classe == "dados":
                    return self._isolar_linhas_ruins(itens, exc)
                self._fechar_conexao()
                if classe == "esquema":
                    return self._pausar_e_contingencia(itens, exc, "tabela/permissão")
                # conexão (08xxx, HYT00/HYT01, driver ausente) ou erro desconhecido: reconecta e repete
                if sqlstate.startswith("08") and not tentou_na_hora and not ultima:
                    tentou_na_hora = True  # conexão velha caiu: reconecta e tenta já, sem esperar
                    tentativa -= 1
                    self._logar("AVISO", "Conexão com o SQL Server caiu; reconectando…")
                    continue
                if tentativa >= tentativas:
                    return self._pausar_e_contingencia(itens, exc, "indisponível")
                resumo = sql_conexao.traduzir_erro(exc).splitlines()[0]
                self._logar("AVISO", f"Falha ao gravar no SQL Server (tentativa {tentativa}/{tentativas}): "
                                     f"{resumo} Nova tentativa em {espera:.1f}s.")
                self._dormir(espera)
                espera *= 2

    def fechar(self) -> int:
        n = 0
        try:
            n = self.descarregar(ultima=True)
        finally:
            self._fechar_conexao()
            try:
                n += self._fallback.fechar()
            except OSError:
                pass  # já logado pela contingência
        return n

    # -- internos -------------------------------------------------------------------------------------

    def _pausar_e_contingencia(self, itens: list[LinhaIot], exc: Exception, motivo: str) -> int:
        self._indisponivel_ate = time.monotonic() + self.pausa_apos_falha
        self._erros_lote += 1
        self._logar("ERRO", f"SQL Server {motivo}: {len(itens)} registros enviados para a contingência; "
                            f"nova tentativa no banco em {self.pausa_apos_falha:.0f}s. "
                            + sql_conexao.traduzir_erro(exc).replace("\n", " "))
        return self._para_contingencia(itens)

    def _para_contingencia(self, itens: list[LinhaIot]) -> int:
        self._fallback.adicionar(itens)
        self._fallback.descarregar()
        return len(itens)

    def _isolar_linhas_ruins(self, itens: list[LinhaIot], exc: Exception) -> int:
        """Erro de dados (22xxx/23xxx): bissecção para gravar as linhas boas e mandar só as ruins à contingência."""
        self._logar("AVISO", f"Lote de {len(itens)} registros recusado por erro de dados; isolando as linhas "
                             f"com problema. {sql_conexao.traduzir_erro(exc).splitlines()[-1]}")
        boas = 0
        ruins: list[LinhaIot] = []
        pilha = [itens[len(itens) // 2:], itens[:len(itens) // 2]] if len(itens) > 1 else []
        if len(itens) == 1:
            ruins.append(itens[0])
        while pilha:
            parte = pilha.pop()
            if not parte:
                continue
            try:
                self._inserir(parte)
                boas += len(parte)
            except Exception as exc_parte:  # noqa: BLE001
                classe, _ = classificar_erro(exc_parte)
                if classe != "dados":
                    # o banco caiu no meio: o que falta vai inteiro para a contingência
                    self._fechar_conexao()
                    resto = parte + [r for p in reversed(pilha) for r in p]
                    if boas:
                        self._log_gravados(boas)
                    self._para_contingencia(ruins)
                    self._pausar_e_contingencia(resto, exc_parte, "indisponível")
                    return len(itens)
                if len(parte) == 1:
                    ruins.append(parte[0])
                    self._logar("ERRO", f"Registro recusado pelo SQL Server ({parte[0].descricao()}, "
                                        f"tópico {parte[0].topico}): "
                                        + sql_conexao.traduzir_erro(exc_parte).replace("\n", " "))
                else:
                    meio = len(parte) // 2
                    pilha += [parte[meio:], parte[:meio]]
        if boas:
            self._log_gravados(boas)
        if ruins:
            self._erros_lote += len(ruins)
            self._logar("AVISO", f"{len(ruins)} registro(s) com erro de dados enviados para a contingência")
            self._para_contingencia(ruins)
        return len(itens)

    def falhas(self) -> int:
        """Erros desde a última consulta (lotes que foram à contingência por falha do banco + linhas ruins)."""
        n, self._erros_lote = self._erros_lote, 0
        return n

    def _inserir(self, itens: list[LinhaIot]) -> None:
        if self._conexao is None:
            self._conexao = (self._conectar or sql_conexao.conectar)(self._sql)
        cursor = self._conexao.cursor()
        try:
            cursor.fast_executemany = True
            tipos = _tipos_parametros()
            if tipos is not None and hasattr(cursor, "setinputsizes"):
                cursor.setinputsizes(tipos)
            cursor.executemany(self.sql_insert, [r.como_tupla() for r in itens])
            self._conexao.commit()
            self.gravados_sql += len(itens)
        except Exception:
            try:
                self._conexao.rollback()
            except Exception:  # noqa: BLE001
                pass
            raise
        finally:
            try:
                cursor.close()
            except Exception:  # noqa: BLE001
                pass

    def _fechar_conexao(self) -> None:
        if self._conexao is not None:
            try:
                self._conexao.close()
            except Exception:  # noqa: BLE001
                pass
            self._conexao = None


def classificar_erro(exc: BaseException) -> tuple[str, str]:
    """Classe do erro pelo SQLSTATE (``exc.args[0]`` no pyodbc): "conexao" | "esquema" | "dados" | "outro"."""
    if isinstance(exc, (DriverOdbcAusente, ImportError)):
        return "conexao", ""
    args = getattr(exc, "args", ()) or ()
    sqlstate = args[0] if args and isinstance(args[0], str) and re.fullmatch(r"[0-9A-Z]{5}", args[0]) else ""
    if sqlstate.startswith("08") or sqlstate in ("HYT00", "HYT01"):
        return "conexao", sqlstate
    if sqlstate.startswith("42"):
        return "esquema", sqlstate
    if sqlstate.startswith(("22", "23")):
        return "dados", sqlstate
    return "outro", sqlstate


def _tipos_parametros():
    """Tipos explícitos para o fast_executemany, na ordem de ``LinhaIot.COLUNAS``.

    Evita conversões implícitas (ex.: NVARCHAR → VARCHAR no device_id, que impede uso de índice) e erro quando a
    1ª linha do lote tem NULL. Só quando o pyodbc já foi carregado por ``sql_conexao.conectar``; com conexões
    falsas (testes) devolve None.
    """
    pyodbc = sys.modules.get("pyodbc")
    if pyodbc is None or not hasattr(pyodbc, "SQL_VARCHAR"):
        return None
    return [
        (pyodbc.SQL_BIGINT, 0, 0),            # pid bigint
        (pyodbc.SQL_VARCHAR, 100, 0),         # device_id varchar(100)
        (pyodbc.SQL_BIGINT, 0, 0),            # ts_unix bigint
        (pyodbc.SQL_DECIMAL, 18, 4),          # chd1_value decimal(18,4)
        (pyodbc.SQL_DECIMAL, 18, 4),          # chd2_value
        (pyodbc.SQL_DECIMAL, 18, 4),          # chd3_value
        (pyodbc.SQL_WVARCHAR, 0, 0),          # payload_json nvarchar(max) (tamanho 0 = max)
        (pyodbc.SQL_TYPE_TIMESTAMP, 23, 3),   # recebido_em datetime2(3), UTC
    ]


def criar_sink(sql: SqlServerConfig, gravacao: GravacaoConfig, logar: Logar = _sem_log,
               pasta_base: Path | None = None) -> Sink:
    """SQL Server quando configurado; senão só contingência."""
    if sql.habilitado:
        return SinkSqlServer(sql, gravacao, logar, pasta_base)
    return SinkContingencia(gravacao, logar, pasta_base)
