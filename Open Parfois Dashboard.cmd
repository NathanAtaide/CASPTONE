@echo off
rem Opens the Parfois Dashboard reading the Parquet files of this repository (data\exports\powerbi).
rem See open_dashboard.ps1 for details.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0open_dashboard.ps1" %*
if errorlevel 1 pause
