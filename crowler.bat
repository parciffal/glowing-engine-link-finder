@echo off

install:
    echo Installing dependencies...
    python3.11.exe -m pip install -r requirements.txt

    if %errorlevel% neq 0 (
        echo Error: Failed to install dependencies.
        exit /b %errorlevel%
    )

run:
    echo Running the script...
    python3.11.exe run.py

    # Check if the script execution was successful
    if %errorlevel% neq 0 (
        echo Error: Failed to run the script.
    ) else (
        echo Script executed successfully.
    )

pause
