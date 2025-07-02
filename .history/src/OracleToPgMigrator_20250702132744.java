import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
        List<String> tabellen = konfiguration.getWhitelist();
        List<String> blacklist = konfiguration.getBlacklist();
        
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
}
