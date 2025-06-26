# Oracle zu PostgreSQL Migrationstool

Dieses Java-Tool migriert Tabellen von einer Oracle-Datenbank zu PostgreSQL, indem es SQL-Skripte für CREATE TABLE und INSERT-Statements erzeugt.

## Anforderungen

- Java 8 oder höher
- Oracle JDBC-Treiber (ojdbc8.jar)
- PostgreSQL JDBC-Treiber (postgresql.jar)

## Installation und Vorbereitung

1. Stellen Sie sicher, dass Java installiert ist
2. Laden Sie die JDBC-Treiber herunter und legen Sie sie im `lib`-Verzeichnis ab:
   - Oracle JDBC-Treiber: [Oracle JDBC Downloads](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
   - PostgreSQL JDBC-Treiber: [PostgreSQL JDBC Downloads](https://jdbc.postgresql.org/download/)
3. Kompilieren Sie das Projekt:

```
javac -cp "lib/*" -d bin src/*.java
```

## Verwendung

1. Erstellen Sie eine Konfigurationsdatei nach dem Muster von `config_beispiel.properties`
2. Führen Sie das Tool aus:

```
java -cp "bin;lib/*" Main pfad/zur/konfigurationsdatei.properties
```

## Konfiguration

Die Konfiguration erfolgt über eine Properties-Datei mit folgenden Einstellungen:

### Datenbankverbindungen

```
oracle.url=jdbc:oracle:thin:@//localhost:1521/ORCLPDB
oracle.benutzer=user
oracle.passwort=password

postgres.url=jdbc:postgresql://localhost:5432/postgres
postgres.benutzer=postgres
postgres.passwort=postgres
```

### Tabellen-Konfiguration

```
# Tabellen für Migration (durch Kommas getrennt)
tabellen.whitelist=KUNDEN,BESTELLUNGEN,PRODUKTE

# Tabellen, die von der Migration ausgeschlossen werden sollen
tabellen.blacklist=TEMP_TABELLE,BACKUP_TABELLE

# Spalten, die pro Tabelle ignoriert werden sollen
tabelle.KUNDEN.ignorierte_spalten=LETZTE_AKTUALISIERUNG,INTERNE_ID
```

### Datentyp-Mappings und Transformationen

```
# Datentyp-Mappings (Oracle -> PostgreSQL)
datentyp.mapping=NUMBER(1)->BOOLEAN;VARCHAR2->VARCHAR;CLOB->TEXT

# Wertetransformationen für spezielle Typen
transform.NUMBER_1_=0->false;1->true
```

## Ausgabe

Das Tool erzeugt für jede Tabelle zwei Dateien im konfigurierten Ausgabeverzeichnis:

1. `<tabellenname>_create.sql`: SQL für die Tabellendefinition in PostgreSQL
2. `<tabellenname>_inserts.sql`: INSERT-Statements für alle Datenzeilen

## Logdateien

Logdateien werden im `logs`-Verzeichnis abgelegt und enthalten detaillierte Informationen über den Migrationsprozess.
