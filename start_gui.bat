@echo off
setlocal
cd /d "%~dp0"
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

if exist "%~dp0env\Scripts\pythonw.exe" (
    start "" "%~dp0env\Scripts\pythonw.exe" "%~dp0main.py" %*
    exit /b 0
)

if exist "%~dp0..\.venv\Scripts\pythonw.exe" (
    start "" "%~dp0..\.venv\Scripts\pythonw.exe" "%~dp0main.py" %*
    exit /b 0
)

if exist "%~dp0..\TwitchDropsMiner\env\Scripts\pythonw.exe" (
    start "" "%~dp0..\TwitchDropsMiner\env\Scripts\pythonw.exe" "%~dp0main.py" %*
    exit /b 0
)

start "" /min pythonw "%~dp0main.py" %*
exit /b 0
