"""Conexão com o SQL Server: string de conexão ODBC, teste de conexão e tradução de erros.

Usado pelo bridge (botão "Testar conexão") e pelo sink de gravação. O ``pyodbc`` só é importado dentro de
``conectar`` (import preguiçoso), para o app abrir mesmo sem o driver ODBC instalado.

Regra de ouro: a senha nunca vai para log. Use ``mascarar`` antes de registrar qualquer string de conexão.
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from app.config import SqlServerConfig

if TYPE_CHECKING:  # pragma: no cover
    import pyodbc

log = logging.getLogger(__name__)

NOME_APP = "MQTT SQL Server"
DRIVER_18 = "ODBC Driver 18 for SQL Server"
DRIVER_17 = "ODBC Driver 17 for SQL Server"

_CRIPTOGRAFIA = {
    "optional": "Optional", "opcional": "Optional", "no": "Optional", "nao": "Optional", "não": "Optional",
    "false": "Optional",
    "mandatory": "Mandatory", "obrigatorio": "Mandatory", "obrigatório": "Mandatory", "yes": "Mandatory",
    "sim": "Mandatory", "true": "Mandatory",
    "strict": "Strict", "estrito": "Strict",
}

_NOME_SQL = re.compile(r"^[A-Za-z0-9_]+$")


class DriverOdbcAusente(RuntimeError):
    """Nenhum driver ODBC do SQL Server (18 ou 17) está instalado, ou o pyodbc não carrega."""


# --------------------------------------------------------------------------------------------- utilidades


def _valor_odbc(valor: str, sempre_chaves: bool = False) -> str:
    """Escapa um valor de atributo ODBC: entre chaves, com ``}`` duplicado, quando necessário."""
    valor = "" if valor is None else str(valor)
    precisa = sempre_chaves or any(c in valor for c in ";{}=") or valor != valor.strip()
    if not precisa:
        return valor
    return "{" + valor.replace("}", "}}") + "}"


def normalizar_criptografia(valor: str | None) -> str:
    """Converte o valor da configuração/tela em ``Optional`` | ``Mandatory`` | ``Strict``."""
    return _CRIPTOGRAFIA.get(str(valor or "").strip().lower(), "Optional")


def validar_tabela(nome: str) -> tuple[str, str]:
    """Valida ``schema.tabela`` (ou só ``tabela``, schema ``dbo``) e devolve as partes.

    Só aceita ``[A-Za-z0-9_]`` em cada parte; colchetes externos são tolerados (``[dbo].[Leituras]``).
    """
    partes = [p.strip().removeprefix("[").removesuffix("]") for p in str(nome or "").split(".")]
    if len(partes) == 1:
        partes = ["dbo", partes[0]]
    if len(partes) != 2 or not all(_NOME_SQL.match(p) for p in partes):
        raise ValueError(f"Nome de tabela inválido: {nome!r}. Use schema.tabela só com letras, números e _.")
    return partes[0], partes[1]


def tabela_citada(nome: str) -> str:
    """``dbo.LeiturasMqtt`` -> ``[dbo].[LeiturasMqtt]`` (já validado)."""
    schema, tabela = validar_tabela(nome)
    return f"[{schema}].[{tabela}]"


# ------------------------------------------------------------------------------------ string de conexão


def montar_connection_string(sql: SqlServerConfig, driver: str | None = None) -> str:
    """Monta a string de conexão ODBC a partir da configuração.

    ``driver`` sobrepõe ``sql.driver`` (usado no fallback do Driver 18 para o 17).
    """
    driver = driver or sql.driver or DRIVER_18
    cripto = normalizar_criptografia(sql.criptografar)
    if "17" in driver or "13" in driver or driver.strip().lower() == "sql server":
        # Drivers antigos só entendem yes/no; Strict não existe neles.
        encrypt = "no" if cripto == "Optional" else "yes"
    else:
        encrypt = cripto

    partes = [
        f"DRIVER={_valor_odbc(driver, sempre_chaves=True)}",
        f"SERVER={_valor_odbc(sql.servidor)}",
        f"DATABASE={_valor_odbc(sql.banco)}",
    ]
    if sql.autenticacao_windows:
        partes.append("Trusted_Connection=yes")
    else:
        partes.append(f"UID={_valor_odbc(sql.usuario)}")
        partes.append(f"PWD={_valor_odbc(sql.senha, sempre_chaves=True)}")
    partes += [
        f"Encrypt={encrypt}",
        f"TrustServerCertificate={'yes' if sql.trust_server_certificate else 'no'}",
        f"APP={_valor_odbc(NOME_APP)}",
    ]
    return ";".join(partes) + ";"


_RE_PWD = re.compile(r"(?i)\b(PWD|Password)\s*=\s*(\{(?:[^}]|\}\})*\}|[^;]*)")


def mascarar(connection_string: str) -> str:
    """Troca o valor de ``PWD=``/``Password=`` por ``***`` (para logs)."""
    return _RE_PWD.sub(lambda m: f"{m.group(1)}=***", connection_string or "")


# ------------------------------------------------------------------------------------------------ conexão


def _importar_pyodbc():
    try:
        import pyodbc  # noqa: PLC0415 - import preguiçoso de propósito
    except ImportError as exc:  # pyodbc ausente ou libodbc/unixODBC não encontrada
        raise DriverOdbcAusente(f"Não foi possível carregar o pyodbc: {exc}") from exc
    return pyodbc


def _escolher_driver(pyodbc, desejado: str) -> str:
    try:
        instalados = list(pyodbc.drivers())
    except Exception:  # noqa: BLE001 - se não der para listar, tenta o configurado mesmo
        return desejado
    if desejado in instalados:
        return desejado
    for alternativo in (DRIVER_18, DRIVER_17):
        if alternativo in instalados:
            log.warning("Driver ODBC %r não está instalado; usando %r.", desejado, alternativo)
            return alternativo
    raise DriverOdbcAusente(
        f"Driver ODBC {desejado!r} não está instalado. Drivers encontrados: {', '.join(instalados) or 'nenhum'}."
    )


def conectar(sql: SqlServerConfig) -> "pyodbc.Connection":
    """Abre uma conexão (autocommit desligado). Levanta ``pyodbc.Error`` ou ``DriverOdbcAusente``."""
    pyodbc = _importar_pyodbc()
    driver = _escolher_driver(pyodbc, sql.driver or DRIVER_18)
    cs = montar_connection_string(sql, driver=driver)
    log.debug("Conectando ao SQL Server: %s", mascarar(cs))
    return pyodbc.connect(cs, timeout=max(1, int(sql.timeout_conexao or 5)), autocommit=False)


# ---------------------------------------------------------------------------------------- teste de conexão


def testar_conexao(sql: SqlServerConfig) -> tuple[bool, str]:
    """Testa a conexão e a tabela. Bloqueante (o bridge chama numa thread).

    Devolve ``(sucesso, mensagem multilinha em português)``. Sucesso exige conectar, a tabela existir e o login
    ter permissão de INSERT nela.
    """
    try:
        schema, tabela = validar_tabela(sql.tabela)
    except ValueError as exc:
        return False, f"✗ {exc}"
    nome_tabela = f"{schema}.{tabela}"

    try:
        conexao = conectar(sql)
    except Exception as exc:  # noqa: BLE001
        return False, "✗ Não conectou.\n" + traduzir_erro(exc)

    linhas: list[str] = []
    ok = True
    try:
        cur = conexao.cursor()
        cur.execute("SELECT @@SERVERNAME, DB_NAME(), SUSER_SNAME(), "
                    "CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)), @@VERSION")
        servidor, banco, login, versao_num, versao = cur.fetchone()
        produto = str(versao or "").splitlines()[0].strip() if versao else ""
        linhas.append("✓ Conectado.")
        linhas.append(f"  Servidor/instância: {servidor}")
        linhas.append(f"  Banco: {banco}")
        linhas.append(f"  Login: {login}")
        linhas.append(f"  Versão: {produto or '?'} ({versao_num})")

        cur.execute("SELECT OBJECT_ID(?, 'U')", f"[{schema}].[{tabela}]")
        existe = cur.fetchone()[0] is not None
        if existe:
            linhas.append(f"✓ Tabela {nome_tabela} existe.")
            cur.execute("SELECT HAS_PERMS_BY_NAME(?, 'OBJECT', 'INSERT')", f"[{schema}].[{tabela}]")
            pode = cur.fetchone()[0]
            if pode == 1:
                linhas.append(f"✓ Permissão de INSERT em {nome_tabela}.")
            else:
                ok = False
                linhas.append(f"✗ Sem permissão de INSERT em {nome_tabela}. Peça ao DBA: "
                              f"GRANT INSERT, SELECT ON {nome_tabela} TO [{login}];")
        else:
            ok = False
            linhas.append(f"✗ Tabela {nome_tabela} não existe no banco {banco}. "
                          "Rode sql/criar_tabela.sql (como dbo/sysadmin) no banco.")
        cur.close()
    except Exception as exc:  # noqa: BLE001
        ok = False
        linhas.append("✗ Erro ao consultar o servidor.")
        linhas.append(traduzir_erro(exc))
    finally:
        try:
            conexao.close()
        except Exception:  # noqa: BLE001
            pass
    return ok, "\n".join(linhas)


# ----------------------------------------------------------------------------------------- tradução de erros


def _sqlstate_e_texto(exc: BaseException) -> tuple[str, str]:
    """pyodbc.Error tem ``args == (sqlstate, mensagem)``; outras exceções só a mensagem."""
    args = getattr(exc, "args", ()) or ()
    if len(args) >= 2 and isinstance(args[0], str) and re.fullmatch(r"[0-9A-Z]{5}", args[0]):
        return args[0], str(args[1])
    return "", str(exc)


def _contem(texto: str, *trechos: str) -> bool:
    return any(t in texto for t in trechos)


def traduzir_erro(exc: Exception) -> str:
    """Mensagem acionável em português para o erro, com o detalhe técnico (sem senha) no fim."""
    sqlstate, texto = _sqlstate_e_texto(exc)
    t = texto.lower()
    detalhe = mascarar(f"[{sqlstate}] {texto}" if sqlstate else f"{type(exc).__name__}: {texto}")

    if isinstance(exc, DriverOdbcAusente) or isinstance(exc, ImportError) or sqlstate == "IM002" or _contem(
        t, "data source name not found", "can't open lib", "libodbc", "nome da fonte de dados não encontrado"
    ):
        acao = ("Driver ODBC do SQL Server não encontrado. Instale o \"ODBC Driver 18 for SQL Server\" "
                "(ou o 17) da Microsoft e reabra o app.")
    elif _contem(t, "ssl", "tls", "certificate", "certificado", "encryption not supported",
                 "criptografia não suportada"):
        acao = ("Falha na criptografia/certificado (SSL/TLS). Com Criptografar = Opcional isso não deveria "
                "ocorrer; se usar Obrigatório/Estrito, o certificado do servidor precisa ser confiável e o nome "
                "do servidor bater com o certificado, ou marque \"Confiar no certificado\" (menos seguro).")
    elif sqlstate == "28000" or _contem(t, "18456", "login failed", "falha de logon"):
        acao = ("Login recusado (erro 18456). Confira usuário e senha; verifique se o servidor aceita "
                "\"Autenticação do SQL Server e do Windows\" (modo misto) e se o login está habilitado e sem "
                "bloqueio.")
    elif _contem(t, "4060", "cannot open database", "não é possível abrir o banco"):
        acao = ("Banco de dados inexistente ou sem acesso (erro 4060). Confira o nome do banco e se o login tem "
                "um usuário mapeado nele (CREATE USER ... FOR LOGIN ...).")
    elif _contem(t, "error locating server/instance", "server is not found or not accessible",
                 "servidor não foi encontrado", "network-related", "relacionado à rede", "named pipes",
                 "server does not exist", "não existe ou acesso negado", "tcp provider", "provedor tcp",
                 "xffffffff", "error code 0x2749", "connection refused"):
        acao = ("Servidor/instância não encontrado ou inacessível. Confira o nome (ex.: TERRA\\SOLO); para "
                "instância nomeada o serviço \"SQL Server Browser\" precisa estar rodando e a porta UDP 1434 "
                "liberada no firewall, além da porta TCP da instância; confira também se o TCP/IP está "
                "habilitado no SQL Server Configuration Manager e se a rede alcança o servidor.")
    elif sqlstate in ("HYT00", "HYT01") or _contem(t, "timeout", "tempo limite", "timed out"):
        acao = ("Tempo esgotado ao conectar. O servidor não respondeu a tempo: verifique rede/VPN, firewall "
                "(UDP 1434 e porta TCP da instância), o SQL Server Browser, ou aumente o timeout de conexão.")
    elif sqlstate.startswith("08"):
        acao = "Falha de comunicação com o servidor. Verifique nome do servidor, rede e firewall."
    elif sqlstate == "42S02" or _contem(t, "invalid object name", "nome de objeto inválido"):
        acao = "Tabela não encontrada. Rode sql/criar_tabela.sql no banco configurado."
    elif sqlstate == "42000" and _contem(t, "permission", "permissão"):
        acao = "Permissão negada. Peça ao DBA: GRANT INSERT, SELECT na tabela para o login do app."
    else:
        acao = "Erro no SQL Server."
    return f"{acao}\nDetalhe: {detalhe}"
