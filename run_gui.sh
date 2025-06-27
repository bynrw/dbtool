#!/bin/bash
echo "Oracle zu PostgreSQL Migrationstool - GUI starten"

# Verzeichnis für kompilierte Klassen erstellen
mkdir -p bin

echo
echo "Kompiliere Java-Dateien..."
javac -d bin src/*.java

if [ $? -ne 0 ]; then
    echo "Fehler beim Kompilieren der Java-Dateien."
    read -p "Drücken Sie eine Taste, um fortzufahren..."
    exit 1
fi

echo "Kompilieren erfolgreich."
echo

echo "Starte GUI..."
echo

java -cp bin MigrationsToolGUI

echo
echo "Anwendung beendet."
