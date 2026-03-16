@echo off
echo ---------------------------------------
echo   Store Dashboard - Auto Installer
echo ---------------------------------------

REM Go to project folder
cd /d C:\Users\ACER\store_dashboard

echo [1/5] Removing old virtual environment (if exists)...
rmdir /s /q venv 2>nul

echo [2/5] Creating new virtual environment...
python -m venv venv

echo [3/5] Activating virtual environment...
call venv\Scripts\activate.bat

echo [4/5] Installing required packages...
pip install --upgrade pip
pip install dash pandas plotly openpyxl waitress pymysql

echo [5/5] Creating requirements.txt...
pip freeze > requirements.txt

echo ---------------------------------------
echo Installation Complete!
echo Virtual Environment: venv
echo Run server with: run_server.bat
echo ---------------------------------------

pause
