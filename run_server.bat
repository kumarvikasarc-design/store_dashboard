@echo off
echo ---------------------------------------
echo Starting Store Dashboard (Production Mode)
echo Using Waitress WSGI Server...
echo ---------------------------------------

REM Switch to project directory
cd /d C:\Users\ACER\store_dashboard

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Start server on port 8053
waitress-serve --listen=0.0.0.0:8053 app:server

pause
