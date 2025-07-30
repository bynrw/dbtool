package de.dbmigration.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.dbmigration.util.Logger;

/**
 * Hauptklasse für die Migration von Oracle nach PostgreSQL.
 * Implementiert die Migration von Tabellen, Sequenzen, Indizes, Constraints und Views.
 */
public class OracleToPgMigrator implements Migrator {
    private final Konfiguration konfiguration;
    private final Connection oracleConnection;
    private final String ausgabePfad;
    private final String basisVerzeichnis;
    
    // Neue Map für die Verfolgung der PostgreSQL-Spaltentypen während der Tabellenmigration
    private Map<String, String> aktuelleTabelleSpaltenTypen; // Spaltenname -> PostgreSQL-Typ

    /**
     * Konstruktor für den OracleToPgMigrator.
     * 
     * @param konfiguration Die Konfiguration für die Migration
     * @param oracleConnection Die Verbindung zur Oracle-Datenbank
     */
    public OracleToPgMigrator(Konfiguration konfiguration, Connection oracleConnection) {
        this.konfiguration = konfiguration;
        this.oracleConnection = oracleConnection;
        
        // Erstelle strukturiertes Basisverzeichnis mit Zeitstempel, DB-Namen und Umgebung
        this.basisVerzeichnis = this.erstelleStrukturiertesBasisVerzeichnis();
        this.ausgabePfad = this.basisVerzeichnis;
        
        if (konfiguration.isOrdnerErstellen()) {
            this.erstelleUnterordner();
        }
    }
    
    /**
     * Erstellt das strukturierte Basisverzeichnis basierend auf Zeitstempel, Datenbankname und Umgebung
     * 
     * @return Pfad zum erstellten Basisverzeichnis
     */
    private String erstelleStrukturiertesBasisVerzeichnis() {
        // Format: ausgabe.pfad/zeitstempel_datenbankname_umgebung/
        String basisPfad = this.konfiguration.getAusgabePfad();
        String zeitstempel = this.konfiguration.getZeitstempel();
        String datenbankName = this.konfiguration.getDatenbankName();
        String umgebung = this.konfiguration.getUmgebung();
        
        // Sicherstellen, dass keine null-Werte verwendet werden
        if (zeitstempel == null) {
            zeitstempel = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            Logger.warnung("Zeitstempel war null. Verwende generierten Wert: " + zeitstempel);
        }
        if (datenbankName == null) {
            datenbankName = "unbekannt";
            Logger.warnung("Datenbankname war null. Verwende Standardwert: " + datenbankName);
        }
        if (umgebung == null) {
            umgebung = "unbekannt";
            Logger.warnung("Umgebung war null. Verwende Standardwert: " + umgebung);
        }
        
        Logger.info("Erstelle Verzeichnis mit Werten - Zeitstempel: " + zeitstempel + 
                   ", Datenbank: " + datenbankName + ", Umgebung: " + umgebung);
        
        // Erstelle den strukturierten Verzeichnisnamen
        String verzeichnisName = zeitstempel + "_" + datenbankName + "_" + umgebung;
        String vollstaendigerPfad = basisPfad + File.separator + verzeichnisName;
        
        // Erstelle das Verzeichnis
        File dir = new File(vollstaendigerPfad);
        if (!dir.exists()) {
            dir.mkdirs();
            Logger.info("Strukturiertes Basisverzeichnis erstellt: " + vollstaendigerPfad);
        }
        
        return vollstaendigerPfad;
    }

    /**
     * Erstellt die benötigten Unterordner für die Migration.
     */
    private void erstelleUnterordner() {
        // Unterordner für die Tabellen-CREATE-Statements
        erstelleVerzeichnisFallsNichtVorhanden(this.konfiguration.getOrdnerTablesCreate(), "Tables-Create-Ordner");
        
        // Unterordner für die Tabellen-INSERT-Statements
        erstelleVerzeichnisFallsNichtVorhanden(this.konfiguration.getOrdnerTablesInserts(), "Tables-Inserts-Ordner");
        
        // Sequenzen-Ordner
        if (this.konfiguration.isSequenzenMigrieren()) {
            erstelleVerzeichnisFallsNichtVorhanden(this.konfiguration.getOrdnerSequenzen(), "Sequenzen-Ordner");
        }

        // Indizes-Ordner
        if (this.konfiguration.isIndizesMigrieren()) {
            erstelleVerzeichnisFallsNichtVorhanden(this.konfiguration.getOrdnerIndizes(), "Indizes-Ordner");
        }

        // Constraints-Ordner
        if (this.konfiguration.isConstraintsMigrieren()) {
            erstelleVerzeichnisFallsNichtVorhanden(this.konfiguration.getOrdnerConstraints(), "Constraints-Ordner");
        }

        // Views-Ordner
        if (this.konfiguration.isViewsMigrieren()) {
            erstelleVerzeichnisFallsNichtVorhanden(this.konfiguration.getOrdnerViews(), "Views-Ordner");
        }
    }
    
    /**
     * Hilfs-Methode zum Erstellen eines Verzeichnisses, falls es noch nicht existiert.
     * 
     * @param ordnerName Name des Ordners (relativ zum ausgabePfad)
     * @param beschreibung Beschreibung für Log-Ausgabe
     */
    private void erstelleVerzeichnisFallsNichtVorhanden(String ordnerName, String beschreibung) {
        File dir = new File(this.ausgabePfad, ordnerName);
        if (!dir.exists()) {
            dir.mkdirs();
            Logger.info(beschreibung + " erstellt: " + dir.getPath());
        }
    }

    /**
     * Führt die Migration durch.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    @Override
    public void migriere() throws SQLException, IOException {
        List<String> tabellen;
        
        // Entweder alle Tabellen oder nur die in der Whitelist migrieren
        if (this.konfiguration.isAlleTabellenMigrieren()) {
            tabellen = this.ermittleAlleTabellenAusDatenbank();
            Logger.info("Alle Tabellen migrieren: " + tabellen.size() + " Tabellen gefunden");
        } else {
            tabellen = this.konfiguration.getWhitelist();
            Logger.info("Whitelist-Tabellen migrieren: " + tabellen.size() + " Tabellen konfiguriert");
        }

        // Tabellen migrieren
        for (String tabelle : tabellen) {
            if (this.konfiguration.sollTabelleMigriert(tabelle)) {
                Logger.info("Beginne Migration der Tabelle: " + tabelle);
                this.migrierenTabelle(tabelle);
            } else {
                Logger.info("Überspringe Tabelle (in Blacklist oder Präfix-Blacklist): " + tabelle);
            }
        }

        // Zusätzliche Objekte migrieren, falls konfiguriert
        if (this.konfiguration.isSequenzenMigrieren()) {
            this.migrierenSequenzen();
        }

        if (this.konfiguration.isIndizesMigrieren()) {
            this.migrierenIndizes();
        }

        if (this.konfiguration.isConstraintsMigrieren()) {
            this.migrierenConstraints();
        }

        if (this.konfiguration.isViewsMigrieren()) {
            this.migrierenViews();
        }
    }

    /**
     * Ermittelt alle Tabellen aus der Oracle-Datenbank.
     * 
     * @return Liste aller Tabellennamen
     * @throws SQLException Bei Datenbankfehlern
     */
    private List<String> ermittleAlleTabellenAusDatenbank() throws SQLException {
        List<String> tabellen = new ArrayList<>();
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")) {
            
            while (rs.next()) {
                tabellen.add(rs.getString("TABLE_NAME"));
            }
        }
        
        return tabellen;
    }

    /**
     * Migriert eine Tabelle (CREATE TABLE und INSERT-Statements).
     * 
     * @param tabellenName Der Name der Tabelle
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    private void migrierenTabelle(String tabellenName) throws SQLException, IOException {
        // Map für die Spaltentypen initialisieren
        this.aktuelleTabelleSpaltenTypen = new HashMap<>();
        
        // CREATE TABLE Statement generieren (füllt die Map)
        String createTableSql = this.erzeugeCreateTable(tabellenName);
        
        // Strukturierter Dateiname für CREATE TABLE
        String createTableDateiname = this.erzeugeStrukturiertenDateinamen(tabellenName, "create");
        
        // Speichern der CREATE TABLE SQL in den entsprechenden Unterordner
        if (this.konfiguration.isOrdnerErstellen()) {
            this.speichereSQL(this.konfiguration.getOrdnerTablesCreate() + File.separator + createTableDateiname, createTableSql, true);
        } else {
            this.speichereSQL(createTableDateiname, createTableSql, true);
        }
        Logger.info("CREATE TABLE-SQL für " + tabellenName + " erzeugt: " + createTableDateiname);
        
        // INSERT Statements generieren (verwendet die Map)
        String insertSql = this.erzeugeInserts(tabellenName);
        
        // Strukturierter Dateiname für INSERTs
        String insertDateiname = this.erzeugeStrukturiertenDateinamen(tabellenName, "inserts");
        
        // Speichern der INSERT SQL in den entsprechenden Unterordner
        if (this.konfiguration.isOrdnerErstellen()) {
            this.speichereSQL(this.konfiguration.getOrdnerTablesInserts() + File.separator + insertDateiname, insertSql, true);
        } else {
            this.speichereSQL(insertDateiname, insertSql, true);
        }
        Logger.info("INSERT-Statements für " + tabellenName + " erzeugt: " + insertDateiname);
        
        // Map zurücksetzen
        this.aktuelleTabelleSpaltenTypen = null;
    }
    
    /**
     * Erzeugt einen strukturierten Dateinamen basierend auf Objekt-Namen und Typ
     * 
     * @param objektName Der Name des Objekts (z.B. Tabellen-Name, oder "sequences", "views", etc.)
     * @param typ Der Typ der Datei (z.B. "create", "inserts", "sql")
     * @return Dateiname im Format: zeitstempel_datenbankname_umgebung_objekt_typ.sql
     */
    private String erzeugeStrukturiertenDateinamen(String objektName, String typ) {
        String zeitstempel = this.konfiguration.getZeitstempel();
        String datenbankName = this.konfiguration.getDatenbankName();
        String umgebung = this.konfiguration.getUmgebung();
        
        // Erstellt strukturierten Dateinamen basierend auf Zeitstempel, Datenbank, Umgebung und Objekt
        String dateiname = zeitstempel + "_" + datenbankName + "_" + umgebung + "_" + objektName;
        
        // Falls Typ nicht bereits .sql enthält, Dateierweiterung hinzufügen
        if (typ.endsWith(".sql")) {
            return dateiname + "_" + typ;
        } else {
            return dateiname + "_" + typ + ".sql";
        }
    }

    /**
     * Erzeugt das CREATE TABLE Statement für eine Tabelle.
     * 
     * @param tabellenName Der Name der Tabelle
     * @return Das CREATE TABLE SQL Statement
     * @throws SQLException Bei Datenbankfehlern
     */
    private String erzeugeCreateTable(String tabellenName) throws SQLException {
        Logger.info("Erzeuge CREATE TABLE-Statement für " + tabellenName);
        
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tabellenName).append(" (\n");
        
        List<String> spaltenDefinitionen = new ArrayList<>();
        List<String> ignorierteSpalten = this.konfiguration.getIgnorierteSpalten(tabellenName);
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tabellenName + " WHERE 1=0")) {
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            DatabaseMetaData dbmd = this.oracleConnection.getMetaData();
            Map<String, String> spaltenInfos = new HashMap<>();
            Map<String, String> defaultWerte = new HashMap<>();
            
            try (ResultSet columns = dbmd.getColumns(null, null, tabellenName.toUpperCase(), null)) {
                while (columns.next()) {
                    String spaltenName = columns.getString("COLUMN_NAME");
                    String typeName = columns.getString("TYPE_NAME");
                    int size = columns.getInt("COLUMN_SIZE");
                    int digits = columns.getInt("DECIMAL_DIGITS");
                    String nullable = columns.getString("IS_NULLABLE");
                    String defaultVal = columns.getString("COLUMN_DEF");
                    
                    String detailTyp = typeName;
                    if (typeName.equals("NUMBER") && size > 0) {
                        if (size == 1 && digits == 0) {
                            detailTyp = "NUMBER(1,0)";
                        } else if (digits >= 0) {
                            detailTyp = "NUMBER(" + size + "," + digits + ")";
                        } else {
                            detailTyp = "NUMBER(" + size + ")";
                        }
                    } else if (typeName.equals("VARCHAR2") || typeName.equals("CHAR")) {
                        detailTyp = typeName + "(" + size + ")";
                    }
                    
                    spaltenInfos.put(spaltenName, detailTyp + ":" + nullable);
                    
                    if (this.konfiguration.isSpaltenDefaultWerteUebertragen() && defaultVal != null && !defaultVal.trim().isEmpty()) {
                        defaultWerte.put(spaltenName, defaultVal.trim());
                    }
                }
            }
            
            // Spalten-Definitionen erstellen
            for (int i = 1; i <= columnCount; i++) {
                String spaltenName = rsmd.getColumnName(i);
                
                if (ignorierteSpalten.contains(spaltenName)) {
                    Logger.info("Spalte wird ignoriert: " + spaltenName);
                    continue;
                }
                
                String typDef = "";
                String nullableFlag = "YES";
                
                if (spaltenInfos.containsKey(spaltenName)) {
                    String[] teile = spaltenInfos.get(spaltenName).split(":");
                    typDef = teile[0];
                    if (teile.length > 1) {
                        nullableFlag = teile[1];
                    }
                } else {
                    typDef = rsmd.getColumnTypeName(i);
                    int precision = rsmd.getPrecision(i);
                    int scale = rsmd.getScale(i);
                    
                    if (typDef.equals("NUMBER") && precision > 0) {
                        if (precision == 1 && scale == 0) {
                            typDef = "NUMBER(1,0)";
                        } else if (scale >= 0) {
                            typDef = "NUMBER(" + precision + "," + scale + ")";
                        } else {
                            typDef = "NUMBER(" + precision + ")";
                        }
                    } else if (typDef.equals("VARCHAR2") || typDef.equals("CHAR")) {
                        typDef = typDef + "(" + precision + ")";
                    }
                }
                
                // Typ zu PostgreSQL konvertieren
                String pgTyp = this.mappeOracleZuPostgresDatentyp(typDef, spaltenName);
                
                // Spaltentyp in Map speichern für spätere Verwendung bei INSERTs
                this.aktuelleTabelleSpaltenTypen.put(spaltenName, pgTyp);
                
                // Spaltendefinition erstellen
                StringBuilder spaltenDef = new StringBuilder();
                spaltenDef.append("    ").append(spaltenName).append(" ").append(pgTyp);
                
                // NOT NULL-Constraint hinzufügen
                if (this.konfiguration.isSpaltenNullConstraintsUebertragen() && nullableFlag.equals("NO")) {
                    spaltenDef.append(" NOT NULL");
                }
                
                // Default-Wert hinzufügen
                if (this.konfiguration.isSpaltenDefaultWerteUebertragen() && defaultWerte.containsKey(spaltenName)) {
                    String defaultVal = defaultWerte.get(spaltenName);
                    defaultVal = this.konvertiereOracleDefaultZuPostgres(defaultVal, pgTyp);
                    spaltenDef.append(" DEFAULT ").append(defaultVal);
                }
                
                spaltenDefinitionen.add(spaltenDef.toString());
            }
        }
        
        // Primary Keys hinzufügen
        try (ResultSet primaryKeys = this.oracleConnection.getMetaData().getPrimaryKeys(null, null, tabellenName.toUpperCase())) {
            Map<String, List<String>> pkConstraints = new HashMap<>();
            
            while (primaryKeys.next()) {
                String pkName = primaryKeys.getString("PK_NAME");
                String columnName = primaryKeys.getString("COLUMN_NAME");
                
                if (!ignorierteSpalten.contains(columnName)) {
                    if (!pkConstraints.containsKey(pkName)) {
                        pkConstraints.put(pkName, new ArrayList<>());
                    }
                    pkConstraints.get(pkName).add(columnName);
                }
            }
            
            for (String pkName : pkConstraints.keySet()) {
                List<String> pkColumns = pkConstraints.get(pkName);
                if (!pkColumns.isEmpty()) {
                    String pkConstraint = "    CONSTRAINT " + pkName + " PRIMARY KEY (" + String.join(", ", pkColumns) + ")";
                    spaltenDefinitionen.add(pkConstraint);
                }
            }
        }
        
        sql.append(String.join(",\n", spaltenDefinitionen));
        sql.append("\n);\n");
        sql.append(this.migriereSpaltenKommentare(tabellenName));
        
        return sql.toString();
    }

    /**
     * Konvertiert einen Oracle-Datentyp in einen PostgreSQL-Datentyp.
     * 
     * @param oracleTyp Der Oracle-Datentyp
     * @param spaltenName Der Name der Spalte (für Logging)
     * @return Der entsprechende PostgreSQL-Datentyp
     */
   /**
 * Konvertiert einen Oracle-Datentyp in einen PostgreSQL-Datentyp.
 * 
 * @param oracleTyp Der Oracle-Datentyp
 * @param spaltenName Der Name der Spalte (für Logging)
 * @return Der entsprechende PostgreSQL-Datentyp
 */
private String mappeOracleZuPostgresDatentyp(String oracleTyp, String spaltenName) {
    Map<String, String> mapping = this.konfiguration.getDatentypMapping();
    
    // Spezialbehandlung für Oracle DATE -> PostgreSQL TIMESTAMP WITHOUT TIME ZONE
    if (oracleTyp.equals("DATE")) {
        Logger.info("Oracle DATE-Spalte '" + spaltenName + "' wird zu TIMESTAMP WITHOUT TIME ZONE konvertiert");
        return "TIMESTAMP WITHOUT TIME ZONE";
    }
    
    // Spezialbehandlung für boolsche Werte
    if (oracleTyp.equals("NUMBER(1,0)")) {
        return "BOOLEAN";
    }
    
    // Spezialbehandlung für NUMBER-Typen
    if (oracleTyp.startsWith("NUMBER(")) {
        String precision = oracleTyp.substring(7, oracleTyp.length() - 1);
        String[] teile = precision.split(",");
        
        if (teile.length >= 2) {
            try {
                int size = Integer.parseInt(teile[0].trim());
                int scale = Integer.parseInt(teile[1].trim());
                
                if (size == 1 && scale == 0) {
                    return "BOOLEAN";
                }
                
                if (scale == 0) {
                    return "BIGINT";
                }
                
                return "NUMERIC";
            } catch (NumberFormatException e) {
                Logger.info("Fehler beim Parsen von NUMBER-Typ: " + oracleTyp);
            }
        }
        
        if (teile.length == 1) {
            return "BIGINT";
        }
    }
    
    // Standard NUMBER ohne Größenangabe
    if (oracleTyp.equals("NUMBER")) {
        return "BIGINT";
    }
    
    // Explizites Mapping aus der Konfiguration verwenden
    if (mapping.containsKey(oracleTyp)) {
        return mapping.get(oracleTyp);
    }
    
    // VARCHAR2 zu VARCHAR konvertieren
    if (oracleTyp.startsWith("VARCHAR2")) {
        return "VARCHAR";
    }
    
    // CHAR-Typen unverändert übernehmen
    if (oracleTyp.startsWith("CHAR")) {
        return oracleTyp;
    }
    
    // Zusätzliche Oracle-Datentypen für bessere Kompatibilität
    if (oracleTyp.equals("TIMESTAMP")) {
        return "TIMESTAMP WITHOUT TIME ZONE";
    }
    
    if (oracleTyp.startsWith("TIMESTAMP(")) {
        return "TIMESTAMP WITHOUT TIME ZONE";
    }
    
    if (oracleTyp.equals("CLOB")) {
        return "TEXT";
    }
    
    if (oracleTyp.equals("BLOB")) {
        return "BYTEA";
    }
    
    if (oracleTyp.equals("RAW")) {
        return "BYTEA";
    }
    
    // Fallback für unbekannte Typen
    Logger.info("Unbekannter Datentyp: " + oracleTyp + " für Spalte '" + spaltenName + "', verwende TEXT als Standard");
    return "TEXT";
}

    /**
     * Erzeugt INSERT-Statements für eine Tabelle.
     * 
     * @param tabellenName Der Name der Tabelle
     * @return Die INSERT SQL Statements
     * @throws SQLException Bei Datenbankfehlern
     */
    private String erzeugeInserts(String tabellenName) throws SQLException {
        List<String> ignorierteSpalten = this.konfiguration.getIgnorierteSpalten(tabellenName);
        String spaltenListe = "*";
        
        // Falls Spalten ignoriert werden sollen, explizite Spaltenliste erstellen
        if (!ignorierteSpalten.isEmpty()) {
            List<String> alleSpalten = new ArrayList<>();
            List<String> zuMigrierendeSpalten = new ArrayList<>();
            
            try (Statement stmt = this.oracleConnection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + tabellenName + " WHERE 1=0")) {
                
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                
                for (int i = 1; i <= columnCount; i++) {
                    String spaltenName = rsmd.getColumnName(i);
                    alleSpalten.add(spaltenName);
                    
                    if (!ignorierteSpalten.contains(spaltenName)) {
                        zuMigrierendeSpalten.add(spaltenName);
                    }
                }
            }
            
            spaltenListe = String.join(", ", zuMigrierendeSpalten);
        }
        
        StringBuilder inserts = new StringBuilder();
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + spaltenListe + " FROM " + tabellenName)) {
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            // Spaltenliste für INSERTs vorbereiten
            List<String> spaltenNamen = new ArrayList<>();
            Map<Integer, String> oracleTypen = new HashMap<>();
            
            for (int i = 1; i <= columnCount; i++) {
                String spaltenName = rsmd.getColumnName(i);
                spaltenNamen.add(spaltenName);
                oracleTypen.put(i, rsmd.getColumnTypeName(i));
            }
            
            String spaltenString = "(" + String.join(", ", spaltenNamen) + ")";
            int zeilenZaehler = 0;
            
            // Daten durchgehen und INSERT-Statements generieren
            while (rs.next()) {
                List<String> werte = new ArrayList<>();
                
                for (int i = 1; i <= columnCount; i++) {
                    String spaltenName = rsmd.getColumnName(i);
                    String oracleTyp = oracleTypen.get(i);
                    Object wert = rs.getObject(i);
                    String postgresTyp = this.aktuelleTabelleSpaltenTypen.get(spaltenName);
                    
                    if (!rs.wasNull() && wert != null) {
                        String formatierterWert = this.formatierteWert(wert, oracleTyp, postgresTyp);
                        werte.add(formatierterWert);
                    } else {
                        werte.add("NULL");
                    }
                }
                
                String werteString = "(" + String.join(", ", werte) + ")";
                inserts.append("INSERT INTO ").append(tabellenName).append(" ")
                       .append(spaltenString).append(" VALUES ").append(werteString).append(";\n");
                
                zeilenZaehler++;
                if (zeilenZaehler % 1000 == 0) {
                    Logger.info("Verarbeite Zeile " + zeilenZaehler + " für Tabelle " + tabellenName);
                }
            }
            
            Logger.info("Insgesamt " + zeilenZaehler + " Zeilen für Tabelle " + tabellenName + " verarbeitet");
        }
        
        return inserts.toString();
    }

    /**
     * Formatiert einen Wert für die Verwendung in INSERT-Statements.
     * Verbesserte Version, die PostgreSQL-Spaltentypen berücksichtigt.
     * 
     * @param wert Der zu formatierende Wert
     * @param oracleTyp Der Oracle-Datentyp des Werts
     * @param postgresTyp Der PostgreSQL-Datentyp der Spalte
     * @return Der formatierte Wert als String
     */
    private String formatierteWert(Object wert, String oracleTyp, String postgresTyp) {
        // Behandlung für PostgreSQL BOOLEAN
        if (postgresTyp != null && postgresTyp.equalsIgnoreCase("BOOLEAN")) {
            if (wert instanceof Number) {
                int num = ((Number) wert).intValue();
                return (num == 1) ? "true" : "false";
            } else if (wert instanceof Boolean) {
                return (Boolean) wert ? "true" : "false";
            } else if (wert instanceof String) {
                String s = (String) wert;
                if ("1".equals(s) || "true".equalsIgnoreCase(s)) {
                    return "true";
                } else if ("0".equals(s) || "false".equalsIgnoreCase(s)) {
                    return "false";
                }
            }
            return wert.toString();
        }
        
        // Strings escapen
        if (wert instanceof String || wert instanceof Character) {
            return "'" + this.escapeStringWert(wert.toString()) + "'";
        }
        
        // Datum/Timestamp
        if (wert instanceof Date || wert instanceof Timestamp) {
            return "'" + wert.toString() + "'";
        }
        
        // Alles andere direkt
        return wert.toString();
    }

    /**
     * Escaped einen String-Wert für SQL.
     * 
     * @param wert Der zu escapende String
     * @return Der escapete String
     */
    private String escapeStringWert(String wert) {
        return wert.replace("'", "''");
    }
    
    /**
     * Migriert alle Sequenzen aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    private void migrierenSequenzen() throws SQLException, IOException {
        Logger.info("Beginne Migration der Sequenzen");
        
        String query = "SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CACHE_SIZE, CYCLE_FLAG, LAST_NUMBER " +
                      "FROM USER_SEQUENCES ORDER BY SEQUENCE_NAME";
        
        StringBuilder sql = new StringBuilder();
        sql.append("-- Sequenzen Migration\n");
        sql.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n");
        sql.append("-- Überlauf-Schutz für PostgreSQL implementiert\n\n");
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String sequenzName = rs.getString("SEQUENCE_NAME");
                String minWertStr = rs.getString("MIN_VALUE");
                String maxWertStr = rs.getString("MAX_VALUE");
                String lastNumStr = rs.getString("LAST_NUMBER");
                long increment = rs.getLong("INCREMENT_BY");
                int cache = rs.getInt("CACHE_SIZE");
                String cycleFlag = rs.getString("CYCLE_FLAG");
                
                Logger.info("Verarbeite Sequenz " + sequenzName + ": MIN=" + minWertStr + 
                           ", MAX=" + maxWertStr + ", LAST=" + lastNumStr);
                
                // Konvertiere Strings zu long mit Fehlerbehandlung für Extremwerte
                long minWert;
                try {
                    minWert = Long.parseLong(minWertStr);
                } catch (NumberFormatException e) {
                    Logger.info("Sequenz " + sequenzName + ": MIN_VALUE " + minWertStr + 
                               " ist zu groß für Java long, verwende 1");
                    minWert = 1L;
                }
                
                // MAX_VALUE kann sehr groß sein
                long maxWert;
                if (!maxWertStr.matches("9\\.?9+E?\\+?\\d*") && !maxWertStr.contains("999999")) {
                    try {
                        maxWert = Long.parseLong(maxWertStr);
                    } catch (NumberFormatException e) {
                        Logger.info("Sequenz " + sequenzName + ": MAX_VALUE " + maxWertStr + 
                                   " ist zu groß für Java long, verwende PostgreSQL Maximum");
                        maxWert = 9223372036854775806L; // PostgreSQL Maximum
                    }
                } else {
                    Logger.info("Sequenz " + sequenzName + ": Extremer MAX_VALUE " + maxWertStr + 
                               " erkannt, verwende PostgreSQL Maximum");
                    maxWert = 9223372036854775806L; // PostgreSQL Maximum
                }
                
                long lastNum;
                try {
                    lastNum = Long.parseLong(lastNumStr);
                } catch (NumberFormatException e) {
                    Logger.info("Sequenz " + sequenzName + ": LAST_NUMBER " + lastNumStr + 
                               " ist zu groß für Java long, verwende sicheren Startwert");
                    lastNum = 1L;
                }
                
                // Sicherstellung, dass Werte innerhalb der PostgreSQL-Grenzen liegen
                long safeMin = Math.max(minWert, -9223372036854775807L);
                long safeMax = Math.min(maxWert, 9223372036854775806L);
                long startWith = Math.max(lastNum, safeMin);
                
                // Warnungen für angepasste Werte
                if (minWert != safeMin) {
                    sql.append("-- WARNUNG: MIN_VALUE angepasst von ").append(minWert)
                       .append(" auf ").append(safeMin).append(" (PostgreSQL Limit)\n");
                    Logger.info("Sequenz " + sequenzName + ": MIN_VALUE angepasst von " + minWert + 
                               " auf " + safeMin);
                }
                
                if (maxWert != safeMax) {
                    sql.append("-- WARNUNG: MAX_VALUE angepasst von ").append(maxWert)
                       .append(" auf ").append(safeMax).append(" (PostgreSQL Limit)\n");
                    Logger.info("Sequenz " + sequenzName + ": MAX_VALUE angepasst von " + maxWert + 
                               " auf " + safeMax);
                }
                
                // Sicherstellen, dass startWith positiv ist
                if (startWith <= 0L) {
                    Logger.info("Sequenz " + sequenzName + ": START WITH Wert " + startWith + 
                               " ist ungültig, setze auf 1");
                    startWith = 1L;
                }
                
                // Überlaufschutz - wenn die Sequenz bald überlaufen könnte
                if (increment > 0L && startWith > safeMax - increment * 1000L) {
                    sql.append("-- KRITISCH: Sequenz ").append(sequenzName)
                       .append(" könnte bald überlaufen! Starte mit niedrigerem Wert.\n");
                    Logger.info("WARNUNG: Sequenz " + sequenzName + " könnte schnell überlaufen! " +
                               "Aktuell: " + startWith + ", Max: " + safeMax);
                    
                    // Setze auf einen sicheren Wert (z.B. 1/4 des maximalen Werts)
                    long safeStart = safeMax / 4L;
                    Logger.info("Sequenz " + sequenzName + ": Setze START WITH auf sicheren Wert " + safeStart);
                    startWith = safeStart > 0L ? safeStart : 1L;
                }
                
                // CREATE SEQUENCE Statement generieren
                sql.append("CREATE SEQUENCE ").append(sequenzName);
                sql.append("\n    START WITH ").append(startWith);
                sql.append("\n    INCREMENT BY ").append(increment);
                sql.append("\n    MINVALUE ").append(safeMin);
                sql.append("\n    MAXVALUE ").append(safeMax);
                sql.append("\n    CACHE ").append(cache);
                
                if ("Y".equals(cycleFlag)) {
                    sql.append("\n    CYCLE");
                } else {
                    sql.append("\n    NO CYCLE");
                }
                
                sql.append(";\n");
                sql.append("-- Original Oracle Werte: MIN=").append(minWertStr)
                   .append(", MAX=").append(maxWertStr)
                   .append(", LAST=").append(lastNumStr).append("\n\n");
            }
        }
        
        // Speichern der Sequenz-SQL
        if (this.konfiguration.isOrdnerErstellen()) {
            String sequenzDateiname = this.erzeugeStrukturiertenDateinamen("sequences", "sql");
            this.speichereSQL(this.konfiguration.getOrdnerSequenzen() + File.separator + sequenzDateiname, 
                             sql.toString(), true);
        } else {
            String sequenzDateiname = this.erzeugeStrukturiertenDateinamen("sequences", "sql");
            this.speichereSQL(sequenzDateiname, sql.toString(), true);
        }
        
        Logger.info("Sequenzen-Migration abgeschlossen");
    }

    /**
     * Migriert alle Indizes aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    private void migrierenIndizes() throws SQLException, IOException {
        Logger.info("Beginne Migration der Indizes");
        
        String query = "SELECT i.INDEX_NAME, i.TABLE_NAME, i.UNIQUENESS, " +
                      "LISTAGG(ic.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) AS COLUMNS " +
                      "FROM USER_INDEXES i " +
                      "JOIN USER_IND_COLUMNS ic ON i.INDEX_NAME = ic.INDEX_NAME " +
                      "WHERE i.INDEX_TYPE = 'NORMAL' AND i.GENERATED = 'N' " +
                      "GROUP BY i.INDEX_NAME, i.TABLE_NAME, i.UNIQUENESS " +
                      "ORDER BY i.TABLE_NAME, i.INDEX_NAME";
        
        StringBuilder sql = new StringBuilder();
        sql.append("-- Indizes Migration\n");
        sql.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n\n");
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String uniqueness = rs.getString("UNIQUENESS");
                String columns = rs.getString("COLUMNS");
                
                sql.append("CREATE ");
                if ("UNIQUE".equals(uniqueness)) {
                    sql.append("UNIQUE ");
                }
                
                sql.append("INDEX ").append(indexName);
                sql.append(" ON ").append(tableName);
                sql.append(" (").append(columns).append(");\n\n");
            }
        }
        
        // Speichern der Indizes-SQL
        if (this.konfiguration.isOrdnerErstellen()) {
            String indexDateiname = this.erzeugeStrukturiertenDateinamen("indexes", "sql");
            this.speichereSQL(this.konfiguration.getOrdnerIndizes() + File.separator + indexDateiname, 
                             sql.toString(), true);
        } else {
            String indexDateiname = this.erzeugeStrukturiertenDateinamen("indexes", "sql");
            this.speichereSQL(indexDateiname, sql.toString(), true);
        }
        
        Logger.info("Indizes-Migration abgeschlossen");
    }

    /**
     * Migriert alle Constraints aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    private void migrierenConstraints() throws SQLException, IOException {
        Logger.info("Beginne Migration der Constraints");
        
        StringBuilder sql = new StringBuilder();
        sql.append("-- Constraints Migration\n");
        sql.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n\n");
        
        sql.append("-- Primary Key Constraints\n");
        this.migrierePrimaryKeyConstraints(sql);
        
        sql.append("\n-- Foreign Key Constraints\n");
        this.migriereForeignKeyConstraints(sql);
        
        sql.append("\n-- Check Constraints\n");
        this.migriereCheckConstraints(sql);
        
        // Speichern der Constraints-SQL
        if (this.konfiguration.isOrdnerErstellen()) {
            String constraintDateiname = this.erzeugeStrukturiertenDateinamen("constraints", "sql");
            this.speichereSQL(this.konfiguration.getOrdnerConstraints() + File.separator + constraintDateiname, 
                             sql.toString(), true);
        } else {
            String constraintDateiname = this.erzeugeStrukturiertenDateinamen("constraints", "sql");
            this.speichereSQL(constraintDateiname, sql.toString(), true);
        }
        
        Logger.info("Constraints-Migration abgeschlossen");
    }

    /**
     * Migriert Primary Key Constraints aus der Oracle-Datenbank.
     * 
     * @param sql StringBuilder zum Anhängen der SQL-Statements
     * @throws SQLException Bei Datenbankfehlern
     */
    private void migrierePrimaryKeyConstraints(StringBuilder sql) throws SQLException {
        String query = "SELECT c.CONSTRAINT_NAME, c.TABLE_NAME, " +
                      "LISTAGG(cc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY cc.POSITION) AS COLUMNS " +
                      "FROM USER_CONSTRAINTS c " +
                      "JOIN USER_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME " +
                      "WHERE c.CONSTRAINT_TYPE = 'P' " +
                      "GROUP BY c.CONSTRAINT_NAME, c.TABLE_NAME " +
                      "ORDER BY c.TABLE_NAME";
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String columns = rs.getString("COLUMNS");
                
                sql.append("ALTER TABLE ").append(tableName);
                sql.append(" ADD CONSTRAINT ").append(constraintName);
                sql.append(" PRIMARY KEY (").append(columns).append(");\n");
            }
        }
    }

    /**
     * Migriert Foreign Key Constraints aus der Oracle-Datenbank.
     * 
     * @param sql StringBuilder zum Anhängen der SQL-Statements
     * @throws SQLException Bei Datenbankfehlern
     */
    private void migriereForeignKeyConstraints(StringBuilder sql) throws SQLException {
        String query = "SELECT c.CONSTRAINT_NAME, c.TABLE_NAME, c.R_CONSTRAINT_NAME, " +
                      "LISTAGG(cc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY cc.POSITION) AS COLUMNS, " +
                      "rc.TABLE_NAME as R_TABLE_NAME, " +
                      "LISTAGG(rcc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY rcc.POSITION) AS R_COLUMNS " +
                      "FROM USER_CONSTRAINTS c " +
                      "JOIN USER_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME " +
                      "JOIN USER_CONSTRAINTS rc ON c.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME " +
                      "JOIN USER_CONS_COLUMNS rcc ON rc.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME " +
                      "WHERE c.CONSTRAINT_TYPE = 'R' " +
                      "GROUP BY c.CONSTRAINT_NAME, c.TABLE_NAME, c.R_CONSTRAINT_NAME, rc.TABLE_NAME " +
                      "ORDER BY c.TABLE_NAME";
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String columns = rs.getString("COLUMNS");
                String refTableName = rs.getString("R_TABLE_NAME");
                String refColumns = rs.getString("R_COLUMNS");
                
                sql.append("ALTER TABLE ").append(tableName);
                sql.append(" ADD CONSTRAINT ").append(constraintName);
                sql.append(" FOREIGN KEY (").append(columns).append(")");
                sql.append(" REFERENCES ").append(refTableName);
                sql.append(" (").append(refColumns).append(");\n");
            }
        }
    }

    /**
     * Migriert Check Constraints aus der Oracle-Datenbank.
     * 
     * @param sql StringBuilder zum Anhängen der SQL-Statements
     * @throws SQLException Bei Datenbankfehlern
     */
    private void migriereCheckConstraints(StringBuilder sql) throws SQLException {
        String query = "SELECT CONSTRAINT_NAME, TABLE_NAME, SEARCH_CONDITION " +
                      "FROM USER_CONSTRAINTS " +
                      "WHERE CONSTRAINT_TYPE = 'C' AND GENERATED = 'USER NAME' " +
                      "ORDER BY TABLE_NAME";
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String condition = rs.getString("SEARCH_CONDITION");
                
                sql.append("ALTER TABLE ").append(tableName);
                sql.append(" ADD CONSTRAINT ").append(constraintName);
                sql.append(" CHECK (").append(condition).append(");\n");
            }
        }
    }

    /**
     * Migriert Views aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    private void migrierenViews() throws SQLException, IOException {
        Logger.info("Beginne Migration der Views");
        
        String query = "SELECT VIEW_NAME, TEXT FROM USER_VIEWS ORDER BY VIEW_NAME";
        
        StringBuilder sql = new StringBuilder();
        sql.append("-- Views Migration\n");
        sql.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n\n");
        
        try (Statement stmt = this.oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                String viewName = rs.getString("VIEW_NAME");
                String viewText = rs.getString("TEXT");
                
                sql.append("CREATE OR REPLACE VIEW ").append(viewName).append(" AS\n");
                sql.append(viewText);
                sql.append(";\n\n");
            }
        }
        
        // Speichern der Views-SQL
        if (this.konfiguration.isOrdnerErstellen()) {
            String viewsDateiname = this.erzeugeStrukturiertenDateinamen("views", "sql");
            this.speichereSQL(this.konfiguration.getOrdnerViews() + File.separator + viewsDateiname, 
                             sql.toString(), true);
        } else {
            String viewsDateiname = this.erzeugeStrukturiertenDateinamen("views", "sql");
            this.speichereSQL(viewsDateiname, sql.toString(), true);
        }
        
        Logger.info("Views-Migration abgeschlossen");
    }

    /**
     * Konvertiert Oracle-spezifische Default-Werte zu PostgreSQL-Syntax.
     * 
     * @param defaultWert Der Oracle Default-Wert
     * @return Der konvertierte PostgreSQL Default-Wert
     */
    private String konvertiereOracleDefaultZuPostgres(String defaultWert, String spaltenTyp) {
        if (defaultWert == null || defaultWert.trim().isEmpty()) {
            return "";
        }
        
        String trimmedWert = defaultWert.trim();
        
            // Boolean-Konvertierung für BOOLEAN-Spalten
         if ("BOOLEAN".equalsIgnoreCase(spaltenTyp)) {
        if ("0".equals(trimmedWert)) {
            return "false";
        } else if ("1".equals(trimmedWert)) {
            return "true";
        }
    }
        if (trimmedWert.toUpperCase().equals("SYSDATE")) {
            return "CURRENT_TIMESTAMP";
        } else if (trimmedWert.toUpperCase().equals("USER")) {
            return "CURRENT_USER";
        } else if (trimmedWert.toUpperCase().equals("SYS_GUID()")) {
            return "gen_random_uuid()";
        } else if (trimmedWert.toUpperCase().contains(".NEXTVAL")) {
            String sequenceName = trimmedWert.substring(0, trimmedWert.toUpperCase().indexOf(".NEXTVAL"));
            return "nextval('" + sequenceName + "')";
        } else {
            return trimmedWert;
        }
    }

   /**
 * Migriert Spaltenkommentare für eine Tabelle mit Unterstützung für mehrzeilige Kommentare.
 * 
 * @param tabellenName Der Name der Tabelle
 * @return SQL-Statements für Spaltenkommentare
 * @throws SQLException Bei Datenbankfehlern
 */
private String migriereSpaltenKommentare(String tabellenName) throws SQLException {
    if (!this.konfiguration.isSpaltenKommentareUebertragen()) {
        return "";
    }
    
    StringBuilder sql = new StringBuilder();
    sql.append("\n-- Spalten-Kommentare für Tabelle ").append(tabellenName).append("\n");
    
    String query = "SELECT COLUMN_NAME, COMMENTS FROM USER_COL_COMMENTS " +
                  "WHERE TABLE_NAME = '" + tabellenName.toUpperCase() + "' " +
                  "AND COMMENTS IS NOT NULL ORDER BY COLUMN_NAME";
    
    try (Statement stmt = this.oracleConnection.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
        
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String comment = rs.getString("COMMENTS");
            
            List<String> ignorierteSpalten = this.konfiguration.getIgnorierteSpalten(tabellenName);
            
            if (!ignorierteSpalten.contains(columnName) && comment != null && !comment.trim().isEmpty()) {
                
                // Prüfen ob der Kommentar mehrzeilig ist
                if (comment.contains("\n") || comment.contains("\r")) {
                    // Mehrzeilige Kommentare mit $$ Syntax
                    String bereinigerKommentar = bereinigeMehrzeiligerKommentar(comment);
                    sql.append("COMMENT ON COLUMN ").append(tabellenName).append(".")
                       .append(columnName).append(" IS $$").append(bereinigerKommentar).append("$$;\n");
                } else {
                    // Einzeilige Kommentare mit einfachen Anführungszeichen
                    String bereinigerKommentar = comment.replace("'", "''").trim();
                    sql.append("COMMENT ON COLUMN ").append(tabellenName).append(".")
                       .append(columnName).append(" IS '").append(bereinigerKommentar).append("';\n");
                }
            }
        }
    }
    
    return sql.toString();
}

/**
 * Bereinigt mehrzeilige Kommentare für PostgreSQL $$ Syntax.
 * 
 * @param kommentar Der ursprüngliche mehrzeilige Kommentar
 * @return Der bereinigte Kommentar
 */
private String bereinigeMehrzeiligerKommentar(String kommentar) {
    if (kommentar == null) {
        return "";
    }
    
    // $$ in Kommentaren durch alternative Zeichen ersetzen, da sie die Syntax stören würden
    String bereinigt = kommentar.replace("$$", "<<>>");
    
    // Trim jede Zeile einzeln für bessere Formatierung
    String[] zeilen = bereinigt.split("\r?\n");
    StringBuilder result = new StringBuilder();
    
    for (int i = 0; i < zeilen.length; i++) {
        result.append(zeilen[i].trim());
        if (i < zeilen.length - 1) {
            result.append("\n");
        }
    }
    
    Logger.info("Mehrzeiliger Kommentar bereinigt für $$ Syntax");
    
    return result.toString();
}
    
    /**
     * Speichert SQL-Code in einer Datei.
     * 
     * @param dateiName Relativer Pfad und Dateiname für die SQL-Datei
     * @param sqlCode Der zu speichernde SQL-Code
     * @param mitZeitstempel Ob ein Zeitstempelkommentar am Anfang eingefügt werden soll
     * @throws IOException bei Problemen beim Schreiben in die Datei
     */
    private void speichereSQL(String dateiName, String sqlCode, boolean mitZeitstempel) throws IOException {
        String dateipfad = this.ausgabePfad + File.separator + dateiName;
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(dateipfad))) {
            if (mitZeitstempel) {
                // Zeitstempel als Kommentar einfügen
                LocalDateTime jetzt = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                String zeitstempel = jetzt.format(formatter);
                writer.println("-- Erstellt am: " + zeitstempel);
                writer.println("-- Datenbank: " + this.konfiguration.getDatenbankName());
                writer.println("-- Umgebung: " + this.konfiguration.getUmgebung());
                writer.println("\n");
            }
            
            writer.print(sqlCode);
        }
        
        Logger.info("SQL-Datei gespeichert: " + dateipfad);
    }
    
    /**
     * Überladene Version von speichereSQL ohne Zeitstempel-Parameter für Kompatibilität.
     * 
     * @param dateiName Relativer Pfad und Dateiname für die SQL-Datei
     * @param sqlCode Der zu speichernde SQL-Code
     * @throws IOException bei Problemen beim Schreiben in die Datei
     */
    @SuppressWarnings("unused")
    private void speichereSQL(String dateiName, String sqlCode) throws IOException {
        speichereSQL(dateiName, sqlCode, false);
    }
}
