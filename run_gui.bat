@echo off
echo Oracle zu PostgreSQL Migrationstool - GUI starten

REM Verzeichnis für kompilierte Klassen erstellen
mkdir bin 2>NUL

echo.
echo Kompiliere Java-Dateien...
javac -d bin src\*.java

if %errorlevel% neq 0 (
    echo Fehler beim Kompilieren der Java-Dateien.
    pause
    exit /b 1
)

echo Kompilieren erfolgreich.
echo.

echo Starte GUI...
echo.

java -cp bin MigrationsToolGUI

echo.
echo Anwendung beendet.
