@echo off
rem ---------------------------------------------------------------------------
rem Gera dist\MQTT-SQLServer\MQTT-SQLServer.exe (sem console) com PyInstaller.
rem Uso: abra o "Prompt de Comando" na pasta do projeto e rode  build_exe.bat
rem Requer: Python 3.12+ e  pip install -r requirements-dev.txt  (inclui pyinstaller)
rem ---------------------------------------------------------------------------
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (set PY=.venv\Scripts\python.exe) else (set PY=python)

echo [1/3] Gerando o icone a partir do SVG...
"%PY%" -m app.gui.recursos build\icone.ico || goto :erro

echo [2/3] Empacotando com PyInstaller...
"%PY%" -m PyInstaller --noconfirm --clean ^
  --windowed ^
  --name "MQTT-SQLServer" ^
  --icon "build\icone.ico" ^
  --paths "." ^
  --collect-submodules keyring ^
  --collect-data tzdata ^
  --add-data "config.example.toml;." ^
  app\main.py || goto :erro

echo [3/3] Copiando o modelo de configuracao...
copy /y "config.example.toml" "dist\MQTT-SQLServer\config.example.toml" >nul

echo.
echo Pronto: dist\MQTT-SQLServer\MQTT-SQLServer.exe
echo Para configurar o SQL Server, copie config.example.toml para config.toml ao lado do .exe.
exit /b 0

:erro
echo.
echo Falha ao gerar o executavel. Veja as mensagens acima.
exit /b 1
