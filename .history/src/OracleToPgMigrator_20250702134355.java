import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hauptklasse für die Migration von Oracle zu PostgreSQL.
 */
public class OracleToPgMigrator {
    private final Konfiguration konfiguration;
    private final Connection oracleConnection;
    private final String ausgabePfad;
    
    /**
     * Konstruktor für die Migrationsklasse.
     * 
     * @param konfiguration Die Konfigurationseinstellungen
     * @param oracleConnection Die Verbindung zur Oracle-Datenbank
     */
    public OracleToPgMigrator(Konfiguration konfiguration, Connection oracleConnection) {
        this.konfiguration = konfiguration;
        this.oracleConnection = oracleConnection;
        this.ausgabePfad = konfiguration.getAusgabePfad();
        
        // Sicherstellen, dass der Ausgabeordner existiert
        File ausgabeDir = new File(ausgabePfad);
        if (!ausgabeDir.exists()) {
            ausgabeDir.mkdirs();
            Logger.info("Ausgabeordner erstellt: " + ausgabePfad);
        }
    }
    
    /**
     * Führt die Migration für alle konfigurierten Tabellen durch.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    public void migriere() throws SQLException, IOException {
        Logger.info("Starte vollständige Datenbank-Migration...");
        
        // 1. Tabellen migrieren
        List<String> tabellen;
        List<String> blacklist = konfiguration.getBlacklist();
        
        if (konfiguration.getAlleTabellen()) {
            Logger.info("Automatische Erkennung aller Tabellen aktiviert...");
            tabellen = ermittleAlleTabellen();
            Logger.info("Gefundene Tabellen: " + tabellen.size());
        } else {
            tabellen = konfiguration.getWhitelist();
            Logger.info("Verwende Whitelist mit " + tabellen.size() + " Tabellen");
        }
        
        Logger.info("Migriere Tabellen...");
        for (String tabelle : tabellen) {
            if (!blacklist.contains(tabelle)) {
                Logger.info("Beginne Migration der Tabelle: " + tabelle);
                migrierenTabelle(tabelle);
                tableMigrated(tabelle);
            } else {
                Logger.info("Überspringe Tabelle (in Blacklist): " + tabelle);
            }
        }
        
        // 2. Sequenzen migrieren
        if (konfiguration.isMigrierenSequenzen()) {
            Logger.info("Migriere Sequenzen...");
            migrierenSequenzen();
        }
        
        // 3. Indizes migrieren
        if (konfiguration.isMigrierenIndizes()) {
            Logger.info("Migriere Indizes...");
            migrierenIndizes(tabellen, blacklist);
        }
        
        // 4. Views migrieren
        if (konfiguration.isMigrierenViews()) {
            Logger.info("Migriere Views...");
            migrierenViews();
        }
        
        // 5. Triggers migrieren
        if (konfiguration.isMigrierenTriggers()) {
            Logger.info("Migriere Triggers...");
            migrierenTriggers(tabellen, blacklist);
        }
        
        // 6. Funktionen und Prozeduren migrieren
        if (konfiguration.isMigrierenFunktionen()) {
            Logger.info("Migriere Funktionen...");
            migrierenFunktionen();
        }
        
        if (konfiguration.isMigrierenProzeduren()) {
            Logger.info("Migriere Prozeduren...");
            migrierenProzeduren();
        }
        
        // 7. Synonyme migrieren
        if (konfiguration.isMigrierenSynonyme()) {
            Logger.info("Migriere Synonyme...");
            migrierenSynonyme();
        }
        
        Logger.info("Migration abgeschlossen!");
    }
    
    /**
     * Wird aufgerufen, wenn eine Tabelle migriert wurde.
     * Kann in Unterklassen überschrieben werden, um den Fortschritt zu verfolgen.
     * 
     * @param tabelle Der Name der migrierten Tabelle
     */
    protected void tableMigrated(String tabelle) {
        // Standardimplementierung tut nichts
    }
    
    /**
     * Migriert eine einzelne Tabelle.
     * 
     * @param tabellenname Der Name der zu migrierenden Tabelle
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenTabelle(String tabellenname) throws SQLException, IOException {
        // 1. CREATE TABLE-Definition erzeugen
        String createTableSql = erzeugeCreateTable(tabellenname);
        speichereSQL(tabellenname + "_create.sql", createTableSql);
        Logger.info("CREATE TABLE-SQL für " + tabellenname + " erzeugt");
        
        // 2. INSERT-Statements erzeugen
        String insertSql = erzeugeInserts(tabellenname);
        speichereSQL(tabellenname + "_inserts.sql", insertSql);
        Logger.info("INSERT-Statements für " + tabellenname + " erzeugt");
    }
    
    /**
     * Erzeugt das CREATE TABLE-Statement für PostgreSQL.
     * 
     * @param tabellenname Der Name der Tabelle
     * @return Das erzeugte CREATE TABLE-Statement
     * @throws SQLException Bei Datenbankfehlern
     */
    private String erzeugeCreateTable(String tabellenname) throws SQLException {
        Logger.info("Erzeuge CREATE TABLE-Statement für " + tabellenname);
        StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE ").append(tabellenname).append(" (\n");
          List<String> spaltenDefinitionen = new ArrayList<>();
        List<String> ignorierteSpalten = konfiguration.getIgnorierteSpalten(tabellenname);
        
        // Tabellendefinition aus Oracle abrufen
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tabellenname + " WHERE 1=0")) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int spaltenAnzahl = metaData.getColumnCount();
            
            // Zusätzliche Informationen über die Spalten holen
            DatabaseMetaData dbMetaData = oracleConnection.getMetaData();
            Map<String, String> spaltenInfo = new HashMap<>();
            
            try (ResultSet columnsRs = dbMetaData.getColumns(null, null, tabellenname.toUpperCase(), null)) {
                while (columnsRs.next()) {
                    String spaltenName = columnsRs.getString("COLUMN_NAME");
                    String typName = columnsRs.getString("TYPE_NAME");
                    int spaltenSize = columnsRs.getInt("COLUMN_SIZE");
                    int dezimalStellen = columnsRs.getInt("DECIMAL_DIGITS");
                    String nullbar = columnsRs.getString("IS_NULLABLE");
                    
                    String oracleDatentyp = typName;
                    if (typName.equals("NUMBER") && spaltenSize > 0) {
                        if (dezimalStellen > 0) {
                            oracleDatentyp = "NUMBER(" + spaltenSize + "," + dezimalStellen + ")";
                        } else {
                            oracleDatentyp = "NUMBER(" + spaltenSize + ")";
                        }
                    } else if (typName.equals("VARCHAR2") || typName.equals("CHAR")) {
                        oracleDatentyp = typName + "(" + spaltenSize + ")";
                    }
                    
                    spaltenInfo.put(spaltenName, oracleDatentyp + ":" + nullbar);
                }
            }
            
            // Durch alle Spalten gehen und CREATE TABLE-Statement erstellen
            for (int i = 1; i <= spaltenAnzahl; i++) {
                String spaltenName = metaData.getColumnName(i);
                
                // Ignorierte Spalten überspringen
                if (ignorierteSpalten.contains(spaltenName)) {
                    Logger.info("Spalte wird ignoriert: " + spaltenName);
                    continue;
                }
                
                String oracleDatentyp = "";
                String nullbarkeit = "YES";
                
                if (spaltenInfo.containsKey(spaltenName)) {
                    String[] teile = spaltenInfo.get(spaltenName).split(":");
                    oracleDatentyp = teile[0];
                    if (teile.length > 1) {
                        nullbarkeit = teile[1];
                    }
                } else {
                    // Fallback, wenn keine detaillierten Informationen verfügbar sind
                    oracleDatentyp = metaData.getColumnTypeName(i);
                    int precision = metaData.getPrecision(i);
                    int scale = metaData.getScale(i);
                    
                    if (oracleDatentyp.equals("NUMBER") && precision > 0) {
                        if (scale > 0) {
                            oracleDatentyp = "NUMBER(" + precision + "," + scale + ")";
                        } else {
                            oracleDatentyp = "NUMBER(" + precision + ")";
                        }
                    } else if (oracleDatentyp.equals("VARCHAR2") || oracleDatentyp.equals("CHAR")) {
                        oracleDatentyp = oracleDatentyp + "(" + precision + ")";
                    }
                }
                
                // Datentyp-Mapping durchführen
                String postgresDatentyp = mappeOracleZuPostgresDatentyp(oracleDatentyp, spaltenName);
                
                // Spaltendefinition erstellen
                StringBuilder spaltenDef = new StringBuilder();
                spaltenDef.append("    ").append(spaltenName).append(" ").append(postgresDatentyp);
                
                if (nullbarkeit.equals("NO")) {
                    spaltenDef.append(" NOT NULL");
                }
                
                spaltenDefinitionen.add(spaltenDef.toString());
            }
        }
        
        // Auch die Primärschlüssel hinzufügen
        try (ResultSet pkRs = oracleConnection.getMetaData().getPrimaryKeys(null, null, tabellenname.toUpperCase())) {
            Map<String, List<String>> primärschlüssel = new HashMap<>();
            
            while (pkRs.next()) {
                String pkName = pkRs.getString("PK_NAME");
                String spaltenName = pkRs.getString("COLUMN_NAME");
                
                // Ignorierte Spalten auch beim Primärschlüssel überspringen
                if (ignorierteSpalten.contains(spaltenName)) {
                    continue;
                }
                
                if (!primärschlüssel.containsKey(pkName)) {
                    primärschlüssel.put(pkName, new ArrayList<>());
                }
                primärschlüssel.get(pkName).add(spaltenName);
            }
            
            // Primärschlüsseldefinitionen hinzufügen
            for (String pkName : primärschlüssel.keySet()) {
                List<String> pkSpalten = primärschlüssel.get(pkName);
                if (!pkSpalten.isEmpty()) {
                    String pkDef = "    CONSTRAINT " + pkName + " PRIMARY KEY (" + String.join(", ", pkSpalten) + ")";
                    spaltenDefinitionen.add(pkDef);
                }
            }
        }
        
        // Fremdschlüssel hinzufügen
        try (ResultSet fkRs = oracleConnection.getMetaData().getImportedKeys(null, null, tabellenname.toUpperCase())) {
            Map<String, List<String[]>> fremdschlüssel = new HashMap<>();
            
            while (fkRs.next()) {
                String fkName = fkRs.getString("FK_NAME");
                String spaltenName = fkRs.getString("FKCOLUMN_NAME");
                String pkTabelle = fkRs.getString("PKTABLE_NAME");
                String pkSpalte = fkRs.getString("PKCOLUMN_NAME");
                short updateRule = fkRs.getShort("UPDATE_RULE");
                short deleteRule = fkRs.getShort("DELETE_RULE");
                
                // Ignorierte Spalten auch beim Fremdschlüssel überspringen
                if (ignorierteSpalten.contains(spaltenName)) {
                    continue;
                }
                
                if (!fremdschlüssel.containsKey(fkName)) {
                    fremdschlüssel.put(fkName, new ArrayList<>());
                }
                
                // Spalteninformationen und Referenzinformationen speichern
                fremdschlüssel.get(fkName).add(new String[] { spaltenName, pkTabelle, pkSpalte, 
                                                             String.valueOf(updateRule), String.valueOf(deleteRule) });
            }
            
            // Fremdschlüsseldefinitionen hinzufügen
            for (String fkName : fremdschlüssel.keySet()) {
                List<String[]> fkSpalten = fremdschlüssel.get(fkName);
                if (!fkSpalten.isEmpty()) {
                    // Fremdschlüsselspalten und Referenzspalten sammeln
                    List<String> fkSpaltenNamen = new ArrayList<>();
                    String pkTabelle = fkSpalten.get(0)[1]; // Erste Referenztabelle nehmen (sollte für alle gleich sein)
                    List<String> pkSpaltenNamen = new ArrayList<>();
                    
                    // ON UPDATE/DELETE Regeln bestimmen (sollten für alle Spalten des FK gleich sein)
                    short updateRule = Short.parseShort(fkSpalten.get(0)[3]);
                    short deleteRule = Short.parseShort(fkSpalten.get(0)[4]);
                    
                    for (String[] fkInfo : fkSpalten) {
                        fkSpaltenNamen.add(fkInfo[0]);
                        pkSpaltenNamen.add(fkInfo[2]);
                    }
                    
                    // Fremdschlüssel-Constraint erstellen
                    StringBuilder fkDef = new StringBuilder();
                    fkDef.append("    CONSTRAINT ").append(fkName)
                         .append(" FOREIGN KEY (").append(String.join(", ", fkSpaltenNamen)).append(")")
                         .append(" REFERENCES ").append(pkTabelle)
                         .append(" (").append(String.join(", ", pkSpaltenNamen)).append(")");
                    
                    // ON DELETE Regel hinzufügen
                    switch (deleteRule) {
                        case DatabaseMetaData.importedKeyCascade:
                            fkDef.append(" ON DELETE CASCADE");
                            break;
                        case DatabaseMetaData.importedKeySetNull:
                            fkDef.append(" ON DELETE SET NULL");
                            break;
                        case DatabaseMetaData.importedKeySetDefault:
                            fkDef.append(" ON DELETE SET DEFAULT");
                            break;
                        case DatabaseMetaData.importedKeyRestrict:
                            fkDef.append(" ON DELETE RESTRICT");
                            break;
                        case DatabaseMetaData.importedKeyNoAction:
                            fkDef.append(" ON DELETE NO ACTION");
                            break;
                    }
                    
                    // ON UPDATE Regel hinzufügen
                    switch (updateRule) {
                        case DatabaseMetaData.importedKeyCascade:
                            fkDef.append(" ON UPDATE CASCADE");
                            break;
                        case DatabaseMetaData.importedKeySetNull:
                            fkDef.append(" ON UPDATE SET NULL");
                            break;
                        case DatabaseMetaData.importedKeySetDefault:
                            fkDef.append(" ON UPDATE SET DEFAULT");
                            break;
                        case DatabaseMetaData.importedKeyRestrict:
                            fkDef.append(" ON UPDATE RESTRICT");
                            break;
                        case DatabaseMetaData.importedKeyNoAction:
                            fkDef.append(" ON UPDATE NO ACTION");
                            break;
                    }
                    
                    spaltenDefinitionen.add(fkDef.toString());
                    Logger.info("Fremdschlüssel erkannt: " + fkName + ", referenziert " + pkTabelle);
                }
            }
        }
        
        // Alle Spaltendefinitionen zusammenfügen
        createSql.append(String.join(",\n", spaltenDefinitionen));
        createSql.append("\n);\n");
        
        return createSql.toString();
    }
    
    /**
     * Mappt einen Oracle-Datentyp zu einem PostgreSQL-Datentyp gemäß Konfiguration.
     * 
     * @param oracleDatentyp Der Oracle-Datentyp
     * @return Der entsprechende PostgreSQL-Datentyp
     */
    private String mappeOracleZuPostgresDatentyp(String oracleDatentyp, String spaltenName) {
        Map<String, String> mapping = konfiguration.getDatentypMapping();
        
        // Prüfen, ob die Spalte basierend auf ihrem Namen als Boolean behandelt werden soll
        if (konfiguration.istBooleanSpalte(spaltenName)) {
            Logger.info("Spalte mit Boolean-Präfix erkannt: " + spaltenName + " -> BOOLEAN");
            return "BOOLEAN";
        }
        
        // Exakte Übereinstimmung prüfen
        if (mapping.containsKey(oracleDatentyp)) {
            return mapping.get(oracleDatentyp);
        }
        
        // Standardmappings für häufige Typen
        if (oracleDatentyp.startsWith("NUMBER(1)")) {
            return "BOOLEAN";
        } else if (oracleDatentyp.startsWith("NUMBER")) {
            // Prüfen auf NUMBER(p,s)
            if (oracleDatentyp.contains(",")) {
                return "NUMERIC" + oracleDatentyp.substring(6); // NUMBER(p,s) -> NUMERIC(p,s)
            } else if (oracleDatentyp.contains("(")) {
                int precision = Integer.parseInt(oracleDatentyp.replaceAll("[^0-9]", ""));
                if (precision <= 4) {
                    return "SMALLINT";
                } else if (precision <= 9) {
                    return "INTEGER";
                } else if (precision <= 18) {
                    return "BIGINT";
                } else {
                    return "NUMERIC" + oracleDatentyp.substring(6); // NUMBER(p) -> NUMERIC(p)
                }
            } else {
                return "NUMERIC"; // Einfaches NUMBER ohne Argumente
            }
        } else if (oracleDatentyp.startsWith("VARCHAR2")) {
            return "VARCHAR" + oracleDatentyp.substring(7); // VARCHAR2(n) -> VARCHAR(n)
        } else if (oracleDatentyp.startsWith("CHAR")) {
            return oracleDatentyp; // CHAR(n) bleibt CHAR(n)
        } else if (oracleDatentyp.equals("DATE")) {
            return "DATE";
        } else if (oracleDatentyp.startsWith("TIMESTAMP")) {
            return oracleDatentyp; // TIMESTAMP bleibt TIMESTAMP
        } else if (oracleDatentyp.equals("CLOB")) {
            return "TEXT";
        } else if (oracleDatentyp.equals("BLOB")) {
            return "BYTEA";
        }
        
        // Fallback für nicht erkannte Typen
        Logger.warnung("Unbekannter Datentyp: " + oracleDatentyp + ", verwende TEXT als Standard");
        return "TEXT";
    }
    
    /**
     * Erzeugt INSERT-Statements für die Daten einer Tabelle.
     * 
     * @param tabellenname Der Name der Tabelle
     * @return Die erzeugten INSERT-Statements
     * @throws SQLException Bei Datenbankfehlern
     */
    private String erzeugeInserts(String tabellenname) throws SQLException {
        List<String> ignorierteSpalten = konfiguration.getIgnorierteSpalten(tabellenname);
        
        // SQL-Abfrage erstellen, die nur die gewünschten Spalten enthält
        String abfrageSpalten = "*";
        
        if (!ignorierteSpalten.isEmpty()) {
            // Liste der Spaltennamen abrufen, die nicht ignoriert werden sollen
            List<String> alleSpalten = new ArrayList<>();
            List<String> gewünschteSpalten = new ArrayList<>();
            
            try (Statement stmt = oracleConnection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + tabellenname + " WHERE 1=0")) {
                
                ResultSetMetaData metaData = rs.getMetaData();
                int spaltenAnzahl = metaData.getColumnCount();
                
                for (int i = 1; i <= spaltenAnzahl; i++) {
                    String spaltenName = metaData.getColumnName(i);
                    alleSpalten.add(spaltenName);
                    
                    if (!ignorierteSpalten.contains(spaltenName)) {
                        gewünschteSpalten.add(spaltenName);
                    }
                }
            }
            
            abfrageSpalten = String.join(", ", gewünschteSpalten);
        }
        
        StringBuilder insertBuilder = new StringBuilder();
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + abfrageSpalten + " FROM " + tabellenname)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int spaltenAnzahl = metaData.getColumnCount();
            
            // Liste der Spaltennamen für die INSERT-Statements erstellen
            List<String> spaltenListe = new ArrayList<>();
            Map<Integer, String> spaltentypen = new HashMap<>();
            
            for (int i = 1; i <= spaltenAnzahl; i++) {
                String spaltenName = metaData.getColumnName(i);
                spaltenListe.add(spaltenName);
                spaltentypen.put(i, metaData.getColumnTypeName(i));
            }
            
            String spaltenTeil = "(" + String.join(", ", spaltenListe) + ")";
            
            // INSERT-Statements für jede Zeile erstellen
            int zeilenZähler = 0;
            while (rs.next()) {
                List<String> werte = new ArrayList<>();
                
                for (int i = 1; i <= spaltenAnzahl; i++) {
                    String spaltentyp = spaltentypen.get(i);
                    Object wert = rs.getObject(i);
                    
                    if (rs.wasNull() || wert == null) {
                        werte.add("NULL");
                    } else {
                        // Typspezifische Formatierung und Transformation
                        String formatiertWert = formatierteWert(wert, spaltentyp);
                        werte.add(formatiertWert);
                    }
                }
                
                String werteTeil = "(" + String.join(", ", werte) + ")";
                insertBuilder.append("INSERT INTO ").append(tabellenname).append(" ").append(spaltenTeil)
                           .append(" VALUES ").append(werteTeil).append(";\n");
                
                zeilenZähler++;
                if (zeilenZähler % 1000 == 0) {
                    Logger.info("Verarbeite Zeile " + zeilenZähler + " für Tabelle " + tabellenname);
                }
            }
            
            Logger.info("Insgesamt " + zeilenZähler + " Zeilen für Tabelle " + tabellenname + " verarbeitet");
        }
        
        return insertBuilder.toString();
    }
    
    /**
     * Formatiert einen Wert für ein PostgreSQL-INSERT-Statement.
     * 
     * @param wert Der zu formatierende Wert
     * @param spaltentyp Der Oracle-Datentyp der Spalte
     * @return Der formatierte Wert als String
     */
    private String formatierteWert(Object wert, String spaltentyp) {
        // Transformationen gemäß Konfiguration durchführen
        if (wert instanceof Number && spaltentyp.equals("NUMBER")) {
            // Boolesche Transformation prüfen (0/1 -> false/true)
            if (wert.toString().equals("0") || wert.toString().equals("1")) {
                Map<String, String> transformation = konfiguration.getWertetransformation("NUMBER(1)");
                if (transformation != null && transformation.containsKey(wert.toString())) {
                    return transformation.get(wert.toString());
                }
            }
        }
        
        // Standardformatierung nach Datentyp
        if (wert instanceof String || wert instanceof Character) {
            return "'" + escapeStringWert(wert.toString()) + "'";
        } else if (wert instanceof java.sql.Date || wert instanceof java.sql.Timestamp) {
            return "'" + wert.toString() + "'";
        } else {
            return wert.toString();
        }
    }
    
    /**
     * Escaped einen String-Wert für SQL.
     * 
     * @param wert Der zu escapende String
     * @return Der escapte String
     */
    private String escapeStringWert(String wert) {
        return wert.replace("'", "''");
    }
    
    /**
     * Speichert ein SQL-Skript in eine Datei.
     * 
     * @param dateiname Der Name der zu erstellenden Datei
     * @param sql Der SQL-Inhalt
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void speichereSQL(String dateiname, String sql) throws IOException {
        String pfad = ausgabePfad + File.separator + dateiname;
        try (PrintWriter writer = new PrintWriter(new FileWriter(pfad))) {
            writer.print(sql);
        }
        Logger.info("SQL-Datei gespeichert: " + pfad);
    }
    
    /**
     * Gibt eine Übersicht aller Fremdschlüssel für eine Tabelle zurück.
     * 
     * @param tabellenname Der Name der Tabelle
     * @return Eine Map mit Fremdschlüsselnamen und deren Details
     * @throws SQLException Bei Datenbankfehlern
     */
    public Map<String, List<Map<String, String>>> getFremdschlüsselInfo(String tabellenname) throws SQLException {
        Map<String, List<Map<String, String>>> fremdschlüsselInfos = new HashMap<>();
        
        try (ResultSet fkRs = oracleConnection.getMetaData().getImportedKeys(null, null, tabellenname.toUpperCase())) {
            while (fkRs.next()) {
                String fkName = fkRs.getString("FK_NAME");
                String spaltenName = fkRs.getString("FKCOLUMN_NAME");
                String pkTabelle = fkRs.getString("PKTABLE_NAME");
                String pkSpalte = fkRs.getString("PKCOLUMN_NAME");
                short updateRule = fkRs.getShort("UPDATE_RULE");
                short deleteRule = fkRs.getShort("DELETE_RULE");
                
                if (!fremdschlüsselInfos.containsKey(fkName)) {
                    fremdschlüsselInfos.put(fkName, new ArrayList<>());
                }
                
                Map<String, String> spaltenInfo = new HashMap<>();
                spaltenInfo.put("quellSpalte", spaltenName);
                spaltenInfo.put("zielTabelle", pkTabelle);
                spaltenInfo.put("zielSpalte", pkSpalte);
                spaltenInfo.put("updateRegel", getFkRuleAsString(updateRule));
                spaltenInfo.put("deleteRegel", getFkRuleAsString(deleteRule));
                
                fremdschlüsselInfos.get(fkName).add(spaltenInfo);
            }
        }
        
        return fremdschlüsselInfos;
    }
    
    /**
     * Konvertiert eine Fremdschlüsselregel in einen lesbaren String.
     * 
     * @param rule Die Regelkonstante aus DatabaseMetaData
     * @return Der lesbare Name der Regel
     */
    private String getFkRuleAsString(short rule) {
        switch (rule) {
            case DatabaseMetaData.importedKeyCascade:
                return "CASCADE";
            case DatabaseMetaData.importedKeySetNull:
                return "SET NULL";
            case DatabaseMetaData.importedKeySetDefault:
                return "SET DEFAULT";
            case DatabaseMetaData.importedKeyRestrict:
                return "RESTRICT";
            case DatabaseMetaData.importedKeyNoAction:
                return "NO ACTION";
            default:
                return "UNKNOWN";
        }
    }
    
    /**
     * Gibt eine Übersicht aller Primärschlüssel für eine Tabelle zurück.
     * 
     * @param tabellenname Der Name der Tabelle
     * @return Eine Map mit Primärschlüsselnamen und deren Spalten
     * @throws SQLException Bei Datenbankfehlern
     */
    public Map<String, List<String>> getPrimärschlüsselInfo(String tabellenname) throws SQLException {
        Map<String, List<String>> primärschlüsselInfos = new HashMap<>();
        
        try (ResultSet pkRs = oracleConnection.getMetaData().getPrimaryKeys(null, null, tabellenname.toUpperCase())) {
            while (pkRs.next()) {
                String pkName = pkRs.getString("PK_NAME");
                String spaltenName = pkRs.getString("COLUMN_NAME");
                
                if (!primärschlüsselInfos.containsKey(pkName)) {
                    primärschlüsselInfos.put(pkName, new ArrayList<>());
                }
                
                primärschlüsselInfos.get(pkName).add(spaltenName);
            }
        }
        
        return primärschlüsselInfos;
    }
    
    /**
     * Migriert Sequenzen von Oracle zu PostgreSQL.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenSequenzen() throws SQLException, IOException {
        Logger.info("Extrahiere Sequenzen aus Oracle...");
        StringBuilder sequenzenSql = new StringBuilder();
        sequenzenSql.append("-- Sequenzen\n\n");
        
        String sequenzQuery = "SELECT sequence_name, min_value, max_value, increment_by, last_number, " +
                             "cache_size, cycle_flag FROM user_sequences";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sequenzQuery)) {
            
            while (rs.next()) {
                String sequenzName = rs.getString("sequence_name");
                long minValue = rs.getLong("min_value");
                long maxValue = rs.getLong("max_value");
                long increment = rs.getLong("increment_by");
                long lastNumber = rs.getLong("last_number");
                int cacheSize = rs.getInt("cache_size");
                String cycleFlag = rs.getString("cycle_flag");
                
                // PostgreSQL-Sequenz erstellen
                sequenzenSql.append("CREATE SEQUENCE ").append(sequenzName.toLowerCase())
                           .append(konfiguration.getSequenceSuffix()).append("\n");
                sequenzenSql.append("    START WITH ").append(lastNumber).append("\n");
                sequenzenSql.append("    INCREMENT BY ").append(increment).append("\n");
                sequenzenSql.append("    MINVALUE ").append(minValue).append("\n");
                sequenzenSql.append("    MAXVALUE ").append(maxValue).append("\n");
                sequenzenSql.append("    CACHE ").append(cacheSize).append("\n");
                
                if ("Y".equals(cycleFlag)) {
                    sequenzenSql.append("    CYCLE");
                } else {
                    sequenzenSql.append("    NO CYCLE");
                }
                sequenzenSql.append(";\n\n");
                
                Logger.info("Sequenz migriert: " + sequenzName);
            }
        }
        
        speichereSQL("sequenzen.sql", sequenzenSql.toString());
        Logger.info("Sequenzen-Migration abgeschlossen");
    }
    
    /**
     * Migriert Indizes von Oracle zu PostgreSQL.
     * 
     * @param tabellen Liste der Tabellen
     * @param blacklist Liste der ausgeschlossenen Tabellen
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenIndizes(List<String> tabellen, List<String> blacklist) throws SQLException, IOException {
        Logger.info("Extrahiere Indizes aus Oracle...");
        StringBuilder indizesSql = new StringBuilder();
        indizesSql.append("-- Indizes\n\n");
        
        for (String tabelle : tabellen) {
            if (blacklist.contains(tabelle)) continue;
            
            String indexQuery = "SELECT i.index_name, i.uniqueness, ic.column_name, ic.column_position " +
                               "FROM user_indexes i, user_ind_columns ic " +
                               "WHERE i.index_name = ic.index_name " +
                               "AND i.table_name = ? " +
                               "AND i.index_type = 'NORMAL' " +
                               "ORDER BY i.index_name, ic.column_position";
            
            try (PreparedStatement pstmt = oracleConnection.prepareStatement(indexQuery)) {
                pstmt.setString(1, tabelle.toUpperCase());
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    Map<String, List<String>> indexSpalten = new HashMap<>();
                    Map<String, Boolean> indexUnique = new HashMap<>();
                    
                    while (rs.next()) {
                        String indexName = rs.getString("index_name");
                        String uniqueness = rs.getString("uniqueness");
                        String spaltenName = rs.getString("column_name");
                        
                        if (!indexSpalten.containsKey(indexName)) {
                            indexSpalten.put(indexName, new ArrayList<>());
                            indexUnique.put(indexName, "UNIQUE".equals(uniqueness));
                        }
                        
                        indexSpalten.get(indexName).add(spaltenName);
                    }
                    
                    // Indizes erstellen
                    for (String indexName : indexSpalten.keySet()) {
                        if (indexName.startsWith("SYS_") || indexName.contains("PK_")) {
                            continue; // System-Indizes überspringen
                        }
                        
                        String pgIndexName = konfiguration.getIndexPrefix() + indexName.toLowerCase();
                        
                        if (indexUnique.get(indexName)) {
                            indizesSql.append("CREATE UNIQUE INDEX ");
                        } else {
                            indizesSql.append("CREATE INDEX ");
                        }
                        
                        indizesSql.append(pgIndexName).append(" ON ").append(tabelle.toLowerCase())
                                 .append(" (");
                        
                        List<String> spalten = indexSpalten.get(indexName);
                        for (int i = 0; i < spalten.size(); i++) {
                            if (i > 0) indizesSql.append(", ");
                            indizesSql.append(spalten.get(i).toLowerCase());
                        }
                        
                        indizesSql.append(");\n");
                        Logger.info("Index migriert: " + indexName + " -> " + pgIndexName);
                    }
                }
            }
        }
        
        speichereSQL("indizes.sql", indizesSql.toString());
        Logger.info("Index-Migration abgeschlossen");
    }
    
    /**
     * Migriert Views von Oracle zu PostgreSQL.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenViews() throws SQLException, IOException {
        Logger.info("Extrahiere Views aus Oracle...");
        StringBuilder viewsSql = new StringBuilder();
        viewsSql.append("-- Views\n\n");
        
        String viewQuery = "SELECT view_name, text FROM user_views";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(viewQuery)) {
            
            while (rs.next()) {
                String viewName = rs.getString("view_name");
                String viewText = rs.getString("text");
                
                // Oracle-spezifische Syntax zu PostgreSQL konvertieren
                String pgViewText = konvertiereViewSQL(viewText);
                
                viewsSql.append("CREATE OR REPLACE VIEW ").append(viewName.toLowerCase())
                        .append(" AS\n").append(pgViewText).append(";\n\n");
                
                Logger.info("View migriert: " + viewName);
            }
        }
        
        speichereSQL("views.sql", viewsSql.toString());
        Logger.info("View-Migration abgeschlossen");
    }
    
    /**
     * Migriert Triggers von Oracle zu PostgreSQL.
     * 
     * @param tabellen Liste der Tabellen
     * @param blacklist Liste der ausgeschlossenen Tabellen
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenTriggers(List<String> tabellen, List<String> blacklist) throws SQLException, IOException {
        Logger.info("Extrahiere Triggers aus Oracle...");
        StringBuilder triggersSql = new StringBuilder();
        triggersSql.append("-- Triggers (Hinweis: Manuelle Anpassung für PostgreSQL erforderlich)\n\n");
        
        for (String tabelle : tabellen) {
            if (blacklist.contains(tabelle)) continue;
            
            String triggerQuery = "SELECT trigger_name, trigger_type, triggering_event, trigger_body " +
                                 "FROM user_triggers WHERE table_name = ?";
            
            try (PreparedStatement pstmt = oracleConnection.prepareStatement(triggerQuery)) {
                pstmt.setString(1, tabelle.toUpperCase());
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String triggerName = rs.getString("trigger_name");
                        String triggerType = rs.getString("trigger_type");
                        String triggerEvent = rs.getString("triggering_event");
                        String triggerBody = rs.getString("trigger_body");
                        
                        triggersSql.append("-- Original Oracle Trigger: ").append(triggerName).append("\n");
                        triggersSql.append("-- Typ: ").append(triggerType).append(", Event: ").append(triggerEvent).append("\n");
                        triggersSql.append("-- WARNUNG: Trigger-Code muss manuell für PostgreSQL angepasst werden!\n");
                        triggersSql.append("/*\n").append(triggerBody).append("\n*/\n\n");
                        
                        Logger.info("Trigger dokumentiert: " + triggerName + " (manuelle Anpassung erforderlich)");
                    }
                }
            }
        }
        
        speichereSQL("triggers.sql", triggersSql.toString());
        Logger.info("Trigger-Migration abgeschlossen (manuelle Anpassung erforderlich)");
    }
    
    /**
     * Migriert Funktionen von Oracle zu PostgreSQL.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenFunktionen() throws SQLException, IOException {
        Logger.info("Extrahiere Funktionen aus Oracle...");
        StringBuilder funktionenSql = new StringBuilder();
        funktionenSql.append("-- Funktionen (Hinweis: Manuelle Anpassung für PostgreSQL erforderlich)\n\n");
        
        String funktionQuery = "SELECT object_name, object_type FROM user_objects WHERE object_type = 'FUNCTION'";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(funktionQuery)) {
            
            while (rs.next()) {
                String funktionName = rs.getString("object_name");
                
                // Funktions-Source abrufen
                String sourceQuery = "SELECT text FROM user_source WHERE name = ? AND type = 'FUNCTION' ORDER BY line";
                try (PreparedStatement pstmt = oracleConnection.prepareStatement(sourceQuery)) {
                    pstmt.setString(1, funktionName);
                    
                    try (ResultSet sourceRs = pstmt.executeQuery()) {
                        funktionenSql.append("-- Original Oracle Funktion: ").append(funktionName).append("\n");
                        funktionenSql.append("-- WARNUNG: Funktions-Code muss manuell für PostgreSQL angepasst werden!\n");
                        funktionenSql.append("/*\n");
                        
                        while (sourceRs.next()) {
                            funktionenSql.append(sourceRs.getString("text"));
                        }
                        
                        funktionenSql.append("*/\n\n");
                        Logger.info("Funktion dokumentiert: " + funktionName + " (manuelle Anpassung erforderlich)");
                    }
                }
            }
        }
        
        speichereSQL("funktionen.sql", funktionenSql.toString());
        Logger.info("Funktionen-Migration abgeschlossen (manuelle Anpassung erforderlich)");
    }
    
    /**
     * Migriert Prozeduren von Oracle zu PostgreSQL.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenProzeduren() throws SQLException, IOException {
        Logger.info("Extrahiere Prozeduren aus Oracle...");
        StringBuilder prozedureSql = new StringBuilder();
        prozedureSql.append("-- Prozeduren (Hinweis: Manuelle Anpassung für PostgreSQL erforderlich)\n\n");
        
        String prozedurQuery = "SELECT object_name, object_type FROM user_objects WHERE object_type = 'PROCEDURE'";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(prozedurQuery)) {
            
            while (rs.next()) {
                String prozedurName = rs.getString("object_name");
                
                // Prozedur-Source abrufen
                String sourceQuery = "SELECT text FROM user_source WHERE name = ? AND type = 'PROCEDURE' ORDER BY line";
                try (PreparedStatement pstmt = oracleConnection.prepareStatement(sourceQuery)) {
                    pstmt.setString(1, prozedurName);
                    
                    try (ResultSet sourceRs = pstmt.executeQuery()) {
                        prozedureSql.append("-- Original Oracle Prozedur: ").append(prozedurName).append("\n");
                        prozedureSql.append("-- WARNUNG: Prozedur-Code muss manuell für PostgreSQL angepasst werden!\n");
                        prozedureSql.append("/*\n");
                        
                        while (sourceRs.next()) {
                            prozedureSql.append(sourceRs.getString("text"));
                        }
                        
                        prozedureSql.append("*/\n\n");
                        Logger.info("Prozedur dokumentiert: " + prozedurName + " (manuelle Anpassung erforderlich)");
                    }
                }
            }
        }
        
        speichereSQL("prozeduren.sql", prozedureSql.toString());
        Logger.info("Prozeduren-Migration abgeschlossen (manuelle Anpassung erforderlich)");
    }
    
    /**
     * Migriert Synonyme von Oracle zu PostgreSQL.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenSynonyme() throws SQLException, IOException {
        Logger.info("Extrahiere Synonyme aus Oracle...");
        StringBuilder synonymeSql = new StringBuilder();
        synonymeSql.append("-- Synonyme (PostgreSQL unterstützt keine Synonyme - als Views implementiert)\n\n");
        
        String synonymQuery = "SELECT synonym_name, table_name, table_owner FROM user_synonyms";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(synonymQuery)) {
            
            while (rs.next()) {
                String synonymName = rs.getString("synonym_name");
                String tableName = rs.getString("table_name");
                String tableOwner = rs.getString("table_owner");
                
                // Als View implementieren, da PostgreSQL keine Synonyme hat
                synonymeSql.append("-- Synonym ").append(synonymName).append(" -> ").append(tableName).append("\n");
                synonymeSql.append("CREATE OR REPLACE VIEW ").append(synonymName.toLowerCase())
                          .append(" AS SELECT * FROM ");
                
                if (tableOwner != null && !tableOwner.isEmpty()) {
                    synonymeSql.append(tableOwner.toLowerCase()).append(".");
                }
                
                synonymeSql.append(tableName.toLowerCase()).append(";\n\n");
                
                Logger.info("Synonym als View migriert: " + synonymName + " -> " + tableName);
            }
        }
        
        speichereSQL("synonyme.sql", synonymeSql.toString());
        Logger.info("Synonyme-Migration abgeschlossen (als Views implementiert)");
    }
    
    /**
     * Konvertiert Oracle-View-SQL zu PostgreSQL-kompatiblem SQL.
     * 
     * @param oracleSQL Das Oracle-View-SQL
     * @return Das konvertierte PostgreSQL-SQL
     */
    private String konvertiereViewSQL(String oracleSQL) {
        // Basis-Konvertierungen für Views
        String pgSQL = oracleSQL;
        
        // Oracle-spezifische Funktionen zu PostgreSQL konvertieren
        pgSQL = pgSQL.replaceAll("\\bNVL\\(", "COALESCE(");
        pgSQL = pgSQL.replaceAll("\\bNVL2\\(([^,]+),([^,]+),([^)]+)\\)", "CASE WHEN $1 IS NOT NULL THEN $2 ELSE $3 END");
        pgSQL = pgSQL.replaceAll("\\bDECODE\\(", "CASE ");
        pgSQL = pgSQL.replaceAll("\\bTO_CHAR\\(", "CAST(");
        pgSQL = pgSQL.replaceAll("\\bTO_NUMBER\\(", "CAST(");
        pgSQL = pgSQL.replaceAll("\\bTO_DATE\\(", "TO_TIMESTAMP(");
        pgSQL = pgSQL.replaceAll("\\bSYSDATE\\b", "CURRENT_TIMESTAMP");
        pgSQL = pgSQL.replaceAll("\\bROWNUM\\b", "ROW_NUMBER() OVER()");
        pgSQL = pgSQL.replaceAll("\\|\\|", "||"); // Concat-Operator bleibt gleich
        
        // Datentyp-Konvertierungen
        for (Map.Entry<String, String> entry : konfiguration.getDatentypMapping().entrySet()) {
            String oracleTyp = entry.getKey();
            String pgTyp = entry.getValue();
            pgSQL = pgSQL.replaceAll("\\b" + oracleTyp + "\\b", pgTyp);
        }
        
        return pgSQL;
    }
}
