@echo off
REM ─── Centaur Prism — Windows one-click launcher ───
cd /d "%~dp0"

echo.
echo  =====================================================
echo   CENTAUR PRISM
echo   NSE stocks refracted into spectrums
echo   Educational only. Not investment advice. DYOR.
echo  =====================================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python is not installed or not in PATH.
    echo  Install Python 3.10+ from https://python.org/downloads
    echo  IMPORTANT: tick "Add Python to PATH" during install.
    pause
    exit /b 1
)

REM Install/update dependencies (only first run is slow)
echo  Checking dependencies...
python -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo  [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)

REM Open browser after 3 seconds (server should be up by then)
start "" /b cmd /c "timeout /t 3 /nobreak >nul & start http://localhost:5001"

echo.
echo  Starting server on http://localhost:5001
echo  Press Ctrl+C to stop.
echo.

python run.py
pause
