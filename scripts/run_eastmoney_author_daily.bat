@echo off
setlocal

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_eastmoney_author_daily.ps1" %*
exit /b %ERRORLEVEL%
