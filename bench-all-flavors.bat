@echo off
setlocal
set WDIR=c:\Users\visse\OneDrive\Documents\Velocity-workflow
set BIN=%WDIR%\target\release

echo === Velocity 3-Flavor Benchmark ===
echo.

REM ─── Kill any leftover processes ──────────────────────────
taskkill /F /IM velocity-bench-server.exe >nul 2>&1
taskkill /F /IM velocity-server.exe >nul 2>&1
taskkill /F /IM velocity-embedded-server.exe >nul 2>&1
ping -n 2 127.0.0.1 >nul

REM ─── Flavor 1: Velocity Classic (bench-server) ───────────
echo [1/3] Starting Velocity Classic (bench-server on :18083)...
start "" /B "%BIN%\velocity-bench-server.exe" --bind 0.0.0.0:18083 --wal-path "%WDIR%\bench-f1.wal"
ping -n 5 127.0.0.1 >nul

echo Running all workloads against Classic...
"%BIN%\velocity-bench-universal.exe" --engines velocity-classic --velocity-classic-address http://localhost:18083 --runs 5 --profile standard --output "%WDIR%\bench-results-classic" --format json

echo Stopping bench-server...
taskkill /F /IM velocity-bench-server.exe >nul 2>&1
ping -n 2 127.0.0.1 >nul
del "%WDIR%\bench-f1.wal" 2>nul
echo.

REM ─── Flavor 2: Velocity Server (VCTP + HTTP bench) ───────
echo [2/3] Starting Velocity Server (VCTP on :18080)...
start "" /B "%BIN%\velocity-server.exe" --vctp-port 17234 --http-bench-port 18080 --health-bind 0.0.0.0:18095 --wal-path "%WDIR%\bench-f2.wal"
ping -n 5 127.0.0.1 >nul

echo Running workloads against VCTP server...
"%BIN%\velocity-bench-universal.exe" --engines velocity-runtime --velocity-runtime-address http://localhost:18080 --runs 5 --profile standard --output "%WDIR%\bench-results-vctp" --format json

echo Stopping velocity-server...
taskkill /F /IM velocity-server.exe >nul 2>&1
ping -n 2 127.0.0.1 >nul
del "%WDIR%\bench-f2.wal" 2>nul
echo.

REM ─── Flavor 3: Velocity Embedded Server ──────────────────
echo [3/3] Starting Velocity Embedded Server on :18086...
echo NOTE: If UAC pops up, click Yes to allow.
start "" /B "%BIN%\velocity-embedded-server.exe" --ws-bind 0.0.0.0:18086 --wal-path "%WDIR%\bench-f3.wal"
ping -n 5 127.0.0.1 >nul

echo Checking embedded server health...
curl -s http://localhost:18086/health
echo.
echo Note: Embedded uses NMCP (shmem + WebSocket), engine core matches Classic.

echo Stopping embedded-server...
taskkill /F /IM velocity-embedded-server.exe >nul 2>&1
del "%WDIR%\bench-f3.wal" 2>nul

echo.
echo === All Benchmarks Complete ===
echo Results at: %WDIR%\bench-results-classic.json
echo             %WDIR%\bench-results-vctp.json
endlocal
