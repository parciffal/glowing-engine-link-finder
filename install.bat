@echo off

install:
    echo Installing dependencies...
    python3.11.exe -m pip install -r requirements.txt

    if %errorlevel% neq 0 (
        echo Error: Failed to install dependencies.
        exit /b %errorlevel%
    )

pause