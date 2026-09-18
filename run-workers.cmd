@echo off
setlocal
if "%~1"=="" goto interactive
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-workers.ps1" %*
exit /b %errorlevel%
:interactive
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-workers.ps1"
set "result=%errorlevel%"
pause
exit /b %result%
