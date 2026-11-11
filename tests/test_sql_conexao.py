"""Testes de app/core/sql_conexao.py sem banco real e sem depender do pyodbc carregar."""
from __future__ import annotations

import pytest

from app.config import SqlServerConfig
from app.core import sql_conexao as sc

SENHA_FALSA = "s3nh@;fake}"


def _cfg(**kw) -> SqlServerConfig:
    base = dict(senha=SENHA_FALSA)
    base.update(kw)
    return SqlServerConfig(**base)


def _atributos(cs: str) -> dict[str, str]:
    """Parser mínimo de string ODBC (entende {..} com }} escapado) para conferir o resultado."""
    out, i = {}, 0
    while i < len(cs):
        j = cs.index("=", i)
        chave = cs[i:j]
        i = j + 1
        if cs[i] == "{":
            i += 1
            valor = []
            while True:
                if cs[i] == "}":
                    if i + 1 < len(cs) and cs[i + 1] == "}":
                        valor.append("}")
                        i += 2
                        continue
                    i += 1
                    break
                valor.append(cs[i])
                i += 1
            out[chave.upper()] = "".join(valor)
        else:
            j = cs.find(";", i)
            j = len(cs) if j < 0 else j
            out[chave.upper()] = cs[i:j]
            i = j
        if i < len(cs) and cs[i] == ";":
            i += 1
    return out


class ErroOdbcFalso(Exception):
    """Imita pyodbc.Error: args == (sqlstate, mensagem)."""


# ------------------------------------------------------------------------------------ string de conexão


def test_padroes_sql_auth():
    cs = sc.montar_connection_string(_cfg())
    a = _atributos(cs)
    assert a["DRIVER"] == "ODBC Driver 18 for SQL Server"
    assert a["SERVER"] == "TERRA\\SOLO"
    assert a["DATABASE"] == "MQTT"
    assert a["UID"] == "mqtt"
    assert a["PWD"] == SENHA_FALSA
    assert a["ENCRYPT"] == "Optional"
    assert a["TRUSTSERVERCERTIFICATE"] == "no"
    assert a["APP"] == "MQTT SQL Server"
    assert "TRUSTED_CONNECTION" not in a


def test_escape_de_chave_e_ponto_e_virgula_na_senha():
    cs = sc.montar_connection_string(_cfg())
    assert "PWD={s3nh@;fake}}};" in cs


def test_windows_auth_sem_uid_pwd():
    cs = sc.montar_connection_string(_cfg(autenticacao_windows=True))
    a = _atributos(cs)
    assert a["TRUSTED_CONNECTION"] == "yes"
    assert "UID" not in a and "PWD" not in a
    assert SENHA_FALSA not in cs


@pytest.mark.parametrize(
    "valor, esperado",
    [("Optional", "Optional"), ("Mandatory", "Mandatory"), ("Strict", "Strict"),
     ("Opcional", "Optional"), ("Obrigatório", "Mandatory"), ("estrito", "Strict"), ("", "Optional")],
)
def test_encrypt(valor, esperado):
    a = _atributos(sc.montar_connection_string(_cfg(criptografar=valor)))
    assert a["ENCRYPT"] == esperado


def test_trust_server_certificate_sim():
    a = _atributos(sc.montar_connection_string(_cfg(trust_server_certificate=True)))
    assert a["TRUSTSERVERCERTIFICATE"] == "yes"


def test_driver_17_usa_yes_no():
    a = _atributos(sc.montar_connection_string(_cfg(), driver=sc.DRIVER_17))
    assert a["DRIVER"] == sc.DRIVER_17
    assert a["ENCRYPT"] == "no"
    a = _atributos(sc.montar_connection_string(_cfg(criptografar="Strict"), driver=sc.DRIVER_17))
    assert a["ENCRYPT"] == "yes"


def test_valores_com_caracteres_especiais_sao_escapados():
    a = _atributos(sc.montar_connection_string(_cfg(banco="MQ;TT", usuario="u}x")))
    assert a["DATABASE"] == "MQ;TT"
    assert a["UID"] == "u}x"


# --------------------------------------------------------------------------------------------- mascarar


@pytest.mark.parametrize("senha", [SENHA_FALSA, "simples", "a}}b", "", "x=y;z", " espaço "])
def test_mascarar_nunca_expoe_senha(senha):
    cs = sc.montar_connection_string(_cfg(senha=senha))
    m = sc.mascarar(cs)
    assert "PWD=***" in m
    if senha:
        assert senha not in m
    # o resto continua legível
    a = _atributos(m)
    assert a["UID"] == "mqtt" and a["APP"] == "MQTT SQL Server" and a["DATABASE"] == "MQTT"


def test_mascarar_password_sem_chaves_e_maiusculas():
    assert sc.mascarar("Server=x;Password=abc123;UID=u") == "Server=x;Password=***;UID=u"
    assert sc.mascarar("pwd=abc;") == "pwd=***;"


# ----------------------------------------------------------------------------------------- validar_tabela


def test_validar_tabela():
    assert sc.validar_tabela("dbo.IOT001") == ("dbo", "IOT001")
    assert sc.validar_tabela("[dbo].[IOT001]") == ("dbo", "IOT001")
    assert sc.validar_tabela("Leituras") == ("dbo", "Leituras")
    assert sc.tabela_citada("dbo.IOT001") == "[dbo].[IOT001]"
    for ruim in ("dbo.Leit;uras", "a.b.c", "dbo.]x", "", "dbo.x--"):
        with pytest.raises(ValueError):
            sc.validar_tabela(ruim)


# ----------------------------------------------------------------------------------------- traduzir_erro


@pytest.mark.parametrize(
    "exc, trecho",
    [
        (ErroOdbcFalso("28000", "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Login failed for user "
                                "'mqtt'. (18456) (SQLDriverConnect)"), "18456"),
        (ErroOdbcFalso("42000", "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Cannot open database "
                                "\"MQTT\" requested by the login. The login failed. (4060) (SQLDriverConnect)"),
         "4060"),
        (ErroOdbcFalso("08001", "[Microsoft][ODBC Driver 18 for SQL Server]SQL Server Network Interfaces: "
                                "Error Locating Server/Instance Specified [xFFFFFFFF]. (-1) (SQLDriverConnect)"),
         "SQL Server Browser"),
        (ErroOdbcFalso("08001", "[Microsoft][ODBC Driver 18 for SQL Server]Named Pipes Provider: Could not open "
                                "a connection to SQL Server [53]. A network-related or instance-specific error "
                                "has occurred while establishing a connection to SQL Server."),
         "TCP/IP"),
        (ErroOdbcFalso("IM002", "[Microsoft][ODBC Driver Manager] Data source name not found and no default "
                                "driver specified (0) (SQLDriverConnect)"), "Driver ODBC"),
        (ErroOdbcFalso("01000", "[unixODBC][Driver Manager]Can't open lib 'ODBC Driver 18 for SQL Server' : "
                                "file not found (0) (SQLDriverConnect)"), "Driver ODBC"),
        (ImportError("libodbc.so.2: cannot open shared object file"), "Driver ODBC"),
        (sc.DriverOdbcAusente("nenhum driver"), "Driver ODBC"),
        (ErroOdbcFalso("08001", "[Microsoft][ODBC Driver 18 for SQL Server]SSL Provider: The certificate chain "
                                "was issued by an authority that is not trusted. (-2146893019)"), "certificado"),
        (ErroOdbcFalso("HYT00", "[Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired (0) "
                                "(SQLDriverConnect)"), "Tempo esgotado"),
        (ErroOdbcFalso("42S02", "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Invalid object name "
                                "'dbo.IOT001'. (208) (SQLExecDirectW)"), "não cria a tabela"),
    ],
)
def test_traduzir_erro(exc, trecho):
    msg = sc.traduzir_erro(exc)
    assert trecho in msg
    assert "Detalhe:" in msg


def test_traduzir_erro_inclui_sqlstate_e_mascara_senha():
    exc = ErroOdbcFalso("08001", f"falha qualquer PWD={{{SENHA_FALSA}}}; resto")
    msg = sc.traduzir_erro(exc)
    assert "[08001]" in msg
    assert "fake" not in msg


def test_traduzir_erro_generico():
    msg = sc.traduzir_erro(RuntimeError("algo estranho"))
    assert "RuntimeError: algo estranho" in msg


# ------------------------------------------------------------------------------------- conectar (fallback)


class PyodbcFalso:
    def __init__(self, drivers):
        self._drivers = drivers
        self.chamadas = []

    def drivers(self):
        return self._drivers

    def connect(self, cs, **kw):
        self.chamadas.append((cs, kw))
        return "conexao"


def test_conectar_fallback_para_driver_17(monkeypatch, caplog):
    falso = PyodbcFalso([sc.DRIVER_17, "SQL Server"])
    monkeypatch.setattr(sc, "_importar_pyodbc", lambda: falso)
    with caplog.at_level("WARNING"):
        assert sc.conectar(_cfg()) == "conexao"
    cs, kw = falso.chamadas[0]
    assert "DRIVER={ODBC Driver 17 for SQL Server}" in cs
    assert kw == {"timeout": 5, "autocommit": False}
    assert "17" in caplog.text
    assert SENHA_FALSA not in caplog.text


def test_conectar_sem_driver(monkeypatch):
    monkeypatch.setattr(sc, "_importar_pyodbc", lambda: PyodbcFalso(["SQL Server"]))
    with pytest.raises(sc.DriverOdbcAusente):
        sc.conectar(_cfg())


# ------------------------------------------------------------------------------------------ testar_conexao


class CursorFalso:
    def __init__(self, existe=True, insert=1, canais_novos=True):
        self.existe, self.insert = existe, insert
        self.canais_novos = canais_novos
        self.consultas = []
        self._ultimo = None
        self._todas = []

    def execute(self, sql, *params):
        self.consultas.append((sql, params))
        if "@@SERVERNAME" in sql:
            self._ultimo = ("SOLO-HOST\\SOLO", "MQTT", "mqtt", "16.0.4135.4",
                            "Microsoft SQL Server 2022 (RTM-CU15) - 16.0.4135.4 (X64)\n\tCopyright ...")
        elif "OBJECT_ID" in sql:
            self._ultimo = (123 if self.existe else None,)
        elif "HAS_PERMS_BY_NAME" in sql:
            self._ultimo = (self.insert,)
        elif "INFORMATION_SCHEMA.COLUMNS" in sql:
            assert params == ("dbo", "IOT001")
            nomes = ["pid", "device_id", "ts_unix", "chd1_value", "chd2_value", "chd3_value"]
            if self.canais_novos:
                nomes += ["chd4_value", "chd5_value", "chd6_value"]
            self._todas = [(n,) for n in nomes]
        return self

    def fetchall(self):
        return self._todas

    def fetchone(self):
        return self._ultimo

    def close(self):
        pass


class ConexaoFalsa:
    def __init__(self, cursor):
        self._cursor = cursor
        self.fechada = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.fechada = True


def test_testar_conexao_ok(monkeypatch):
    con = ConexaoFalsa(CursorFalso())
    monkeypatch.setattr(sc, "conectar", lambda sql: con)
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is True
    assert "SOLO-HOST\\SOLO" in msg and "Banco: MQTT" in msg and "Login: mqtt" in msg
    assert "SQL Server 2022" in msg and "Copyright" not in msg
    assert "existe" in msg and "INSERT" in msg
    assert con.fechada
    # nome da tabela vai como parâmetro, não concatenado
    assert any(p == ("[dbo].[IOT001]",) for _, p in con._cursor.consultas)


def test_testar_conexao_tabela_inexistente(monkeypatch):
    monkeypatch.setattr(sc, "conectar", lambda sql: ConexaoFalsa(CursorFalso(existe=False)))
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is False
    assert "dbo.IOT001 não existe" in msg and "não cria a tabela" in msg


def test_testar_conexao_sem_insert(monkeypatch):
    monkeypatch.setattr(sc, "conectar", lambda sql: ConexaoFalsa(CursorFalso(insert=0)))
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is False
    assert "Sem permissão de INSERT" in msg and "GRANT INSERT, SELECT ON dbo.IOT001" in msg


def test_testar_conexao_falha_login(monkeypatch):
    def falha(sql):
        raise ErroOdbcFalso("28000", "Login failed for user 'mqtt'. (18456)")

    monkeypatch.setattr(sc, "conectar", falha)
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is False
    assert "Não conectou" in msg and "18456" in msg
    assert SENHA_FALSA not in msg


def test_testar_conexao_tabela_invalida_nem_conecta(monkeypatch):
    monkeypatch.setattr(sc, "conectar", lambda sql: pytest.fail("não deveria conectar"))
    ok, msg = sc.testar_conexao(_cfg(tabela="dbo.x;DROP"))
    assert ok is False and "inválido" in msg


def test_testar_conexao_informa_colunas_chd4_a_chd6(monkeypatch):
    monkeypatch.setattr(sc, "conectar", lambda sql: ConexaoFalsa(CursorFalso()))
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is True and "✓ Colunas chd4_value..chd6_value existem" in msg


def test_testar_conexao_sem_chd4_a_chd6_e_aviso_nao_falha(monkeypatch):
    monkeypatch.setattr(sc, "conectar", lambda sql: ConexaoFalsa(CursorFalso(canais_novos=False)))
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is True
    linha = next(l for l in msg.splitlines() if "chd4_value" in l)
    assert linha.startswith("⚠") and "sql/canais_4_a_6_iot001.sql" in linha and "payload_json" in linha


def test_testar_conexao_falha_ao_ler_colunas_nao_reprova(monkeypatch):
    cur = CursorFalso()
    cur.fetchall = lambda: (_ for _ in ()).throw(RuntimeError("sem metadados"))
    monkeypatch.setattr(sc, "conectar", lambda sql: ConexaoFalsa(cur))
    ok, msg = sc.testar_conexao(_cfg())
    assert ok is True and "Não foi possível conferir as colunas" in msg


def test_validar_banco_e_nome_em_3_partes():
    assert sc.validar_banco("OEE_Alumbra") == "OEE_Alumbra"
    assert sc.tabela_em_banco("[OEE]", "dbo.dispositivos_iot") == "[OEE].[dbo].[dispositivos_iot]"
    for ruim in ["", "OEE;DROP", "a b", "OEE]", "x.y"]:
        with pytest.raises(ValueError):
            sc.validar_banco(ruim)

