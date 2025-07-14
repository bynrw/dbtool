@echo off
echo Oracle zu PostgreSQL Migrationstool - Kompilieren und Ausführen

REM Verzeichnis für kompilierte Klassen erstellen
mkdir bin 2>NUL

echo.
echo Kompiliere Java-Dateien...
javac -d bin src\de\dbmigration\*.java src\de\dbmigration\core\*.java src\de\dbmigration\db\*.java src\de\dbmigration\ui\*.java src\de\dbmigration\util\*.java

if %errorlevel% neq 0 (
    echo Fehler beim Kompilieren der Java-Dateien.
    exit /b 1
)

echo Kompilieren erfolgreich.
echo.

echo Starte Migration...
echo.

REM Prüfe Parameter für Netzwerk und Umgebung
set NETZWERK=
set UMGEBUNG=
set CONFIG=

REM Verarbeite alle Parameter
:PARSE_PARAMS
if "%~1"=="" goto EXECUTE

if /i "%~1"=="netz" (
    if not "%~2"=="" (
        set NETZWERK=%~2
        shift
        shift
        goto PARSE_PARAMS
    )
) else if /i "%~1"=="umgebung" (
    if not "%~2"=="" (
        set UMGEBUNG=%~2
        shift
        shift
        goto PARSE_PARAMS
    )
) else (
    REM Falls Parameter keine Netzwerk/Umgebung ist, wird er als Konfigurationsdatei interpretiert
    set CONFIG=%~1
    shift
    goto PARSE_PARAMS
)

:EXECUTE
if defined NETZWERK (
    if defined UMGEBUNG (
        echo Mit Netzwerk und Umgebung: %NETZWERK% %UMGEBUNG%
        java -cp bin;lib\* de.dbmigration.Main netz=%NETZWERK% umgebung=%UMGEBUNG%
        goto END
    )
)

if defined CONFIG (
    echo Mit angegebener Konfiguration: %CONFIG%
    java -cp bin;lib\* de.dbmigration.Main %CONFIG%
) else (
    java -cp bin;lib\* de.dbmigration.Main
)

:END

echo.
echo Migration abgeschlossen.
