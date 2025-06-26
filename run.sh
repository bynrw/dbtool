#!/bin/bash
echo "Oracle zu PostgreSQL Migrationstool - Kompilieren und Ausführen"

# Verzeichnis für kompilierte Klassen erstellen
mkdir -p bin

echo
echo "Kompiliere Java-Dateien..."
javac -d bin src/*.java

if [ $? -ne 0 ]; then
    echo "Fehler beim Kompilieren der Java-Dateien."
    exit 1
fi

echo "Kompilieren erfolgreich."
echo

if [ -z "$1" ]; then
    echo "Verwendung: $0 [Pfad zur Konfigurationsdatei]"
    echo "Bitte geben Sie den Pfad zur Konfigurationsdatei an."
    exit 1
fi

echo "Starte Migration mit Konfiguration: $1"
echo

java -cp bin Main "$1"

echo
echo "Migration abgeschlossen."
