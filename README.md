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

3. Beim Start werden Sie nach folgenden Informationen gefragt:
   - Datenbank-Name (z.B. "lvn_database")
   - Umgebung (z.B. "development", "testing", "production")

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

### Ausgabe-Konfiguration

```
# Ausgabepfad für SQL-Dateien
ausgabe.pfad=./output

# Datenbank-Name und Umgebung (können beim Start überschrieben werden)
datenbank.name=lvn_database
umgebung=development

# Ordner für zusätzliche Datenbankobjekte erstellen
ordner.erstellen=true
ordner.tables_create=tables_create
ordner.tables_inserts=tables_inserts
ordner.sequenzen=sequences
ordner.indizes=indices
ordner.constraints=constraints
ordner.views=views
```

### Zusätzliche Datenbankobjekte

```
# Zusätzliche Datenbankobjekte migrieren
sequenzen.migrieren=true
indizes.migrieren=true
constraints.migrieren=true
views.migrieren=true
```

### Tabellen-Konfiguration

```
# Tabellen für Migration (durch Kommas getrennt)
tabellen.whitelist=KUNDEN,BESTELLUNGEN,PRODUKTE

# Alle Tabellen migrieren (wenn true, wird whitelist ignoriert)
alle.tabellen.migrieren=false

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

Das Tool erzeugt für jede Tabelle SQL-Dateien in einer strukturierten Verzeichnishierarchie:

```
ausgabe.pfad/
  ├── 20250714_123456_lvn_database_production/  # Zeitstempel_Datenbankname_Umgebung
  │   ├── tables_create/
  │   │   ├── 20250714_123456_lvn_database_production_KUNDEN_create.sql
  │   │   ├── 20250714_123456_lvn_database_production_BESTELLUNGEN_create.sql
  │   │   └── ...
  │   ├── tables_inserts/
  │   │   ├── 20250714_123456_lvn_database_production_KUNDEN_inserts.sql
  │   │   ├── 20250714_123456_lvn_database_production_BESTELLUNGEN_inserts.sql
  │   │   └── ...
  │   ├── sequences/
  │   │   └── 20250714_123456_lvn_database_production_sequences_sql.sql
  │   ├── indices/
  │   │   └── 20250714_123456_lvn_database_production_indexes_sql.sql
  │   ├── constraints/
  │   │   └── 20250714_123456_lvn_database_production_constraints_sql.sql
  │   └── views/
  │       └── 20250714_123456_lvn_database_production_views_sql.sql
```

Jede SQL-Datei enthält einen Zeitstempel-Kommentar mit Informationen zur Generierungszeit, Datenbank und Umgebung.

## Logdateien

Logdateien werden im `logs`-Verzeichnis abgelegt und enthalten detaillierte Informationen über den Migrationsprozess.
