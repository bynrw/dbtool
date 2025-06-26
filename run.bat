@echo off
echo Oracle zu PostgreSQL Migrationstool - Kompilieren und Ausführen

REM Verzeichnis für kompilierte Klassen erstellen
mkdir bin 2>NUL

echo.
echo Kompiliere Java-Dateien...
javac -d bin src\*.java

if %errorlevel% neq 0 (
    echo Fehler beim Kompilieren der Java-Dateien.
    exit /b 1
)

echo Kompilieren erfolgreich.
echo.

if "%~1"=="" (
    echo Verwendung: %0 [Pfad zur Konfigurationsdatei]
    echo Bitte geben Sie den Pfad zur Konfigurationsdatei an.
    exit /b 1
)

echo Starte Migration mit Konfiguration: %1
echo.

java -cp bin Main %1

echo.
echo Migration abgeschlossen.
