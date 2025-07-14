#!/bin/bash
echo "Oracle zu PostgreSQL Migrationstool - Kompilieren und Ausführen"

# Verzeichnis für kompilierte Klassen erstellen
mkdir -p bin

echo
echo "Kompiliere Java-Dateien..."
javac -d bin src/de/dbmigration/*.java src/de/dbmigration/core/*.java src/de/dbmigration/db/*.java src/de/dbmigration/ui/*.java src/de/dbmigration/util/*.java

if [ $? -ne 0 ]; then
    echo "Fehler beim Kompilieren der Java-Dateien."
    exit 1
fi

echo "Kompilieren erfolgreich."
echo

echo "Starte Migration..."
echo

# Parameter-Verarbeitung
NETZWERK=""
UMGEBUNG=""
CONFIG=""

while [ $# -gt 0 ]; do
    case "$1" in
        netz)
            NETZWERK="$2"
            shift 2
            ;;
        umgebung)
            UMGEBUNG="$2"
            shift 2
            ;;
        *)
            CONFIG="$1"
            shift
            ;;
    esac
done

# Ausführung mit entsprechenden Parametern
if [ ! -z "$NETZWERK" ] && [ ! -z "$UMGEBUNG" ]; then
    echo "Mit Netzwerk und Umgebung: $NETZWERK $UMGEBUNG"
    java -cp bin:lib/* de.dbmigration.Main netz=$NETZWERK umgebung=$UMGEBUNG
elif [ ! -z "$CONFIG" ]; then
    echo "Mit angegebener Konfiguration: $CONFIG"
    java -cp bin:lib/* de.dbmigration.Main "$CONFIG"
else
    java -cp bin:lib/* de.dbmigration.Main
fi

echo
echo "Migration abgeschlossen."
