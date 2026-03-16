@echo off
echo Starting Store Dashboard...

REM Change directory to your project
cd /d C:\Users\ACER\store_dashboard

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the app
python app.py

pause
