@echo off

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
