@echo off
echo 🚀 Building Bible Database...
echo.

REM Set paths
set JSON_DIR=.\BibleData\Json
set OUTPUT_DIR=.\BibleData\Output

REM Run the converter
dotnet run --project . -- "%JSON_DIR%" "%OUTPUT_DIR%"

if %ERRORLEVEL% EQU 0 (
    echo ✅ Database created successfully in: %OUTPUT_DIR%
) else (
    echo ❌ Build failed!
)