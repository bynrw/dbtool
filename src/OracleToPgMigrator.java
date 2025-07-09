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
        
        // Unterordner für Datenbankobjekte erstellen, falls konfiguriert
        if (konfiguration.isOrdnerErstellen()) {
            erstelleUnterordner();
        }
    }
    
    /**
     * Erstellt die Unterordner für verschiedene Datenbankobjekte.
     */
    private void erstelleUnterordner() {
        if (konfiguration.isSequenzenMigrieren()) {
            File sequenzenDir = new File(ausgabePfad, konfiguration.getOrdnerSequenzen());
            if (!sequenzenDir.exists()) {
                sequenzenDir.mkdirs();
                Logger.info("Sequenzen-Ordner erstellt: " + sequenzenDir.getPath());
            }
        }
        
        if (konfiguration.isIndizesMigrieren()) {
            File indizesDir = new File(ausgabePfad, konfiguration.getOrdnerIndizes());
            if (!indizesDir.exists()) {
                indizesDir.mkdirs();
                Logger.info("Indizes-Ordner erstellt: " + indizesDir.getPath());
            }
        }
        
        if (konfiguration.isConstraintsMigrieren()) {
            File constraintsDir = new File(ausgabePfad, konfiguration.getOrdnerConstraints());
            if (!constraintsDir.exists()) {
                constraintsDir.mkdirs();
                Logger.info("Constraints-Ordner erstellt: " + constraintsDir.getPath());
            }
        }
        
        if (konfiguration.isViewsMigrieren()) {
            File viewsDir = new File(ausgabePfad, konfiguration.getOrdnerViews());
            if (!viewsDir.exists()) {
                viewsDir.mkdirs();
                Logger.info("Views-Ordner erstellt: " + viewsDir.getPath());
            }
        }
    }
    
    /**
     * Führt die Migration für alle konfigurierten Tabellen durch.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    public void migriere() throws SQLException, IOException {
        List<String> tabellen;
        
        if (konfiguration.isAlleTabellenMigrieren()) {
            // Alle Tabellen aus der Datenbank ermitteln
            tabellen = ermittleAlleTabellenAusDatenbank();
            Logger.info("Alle Tabellen migrieren: " + tabellen.size() + " Tabellen gefunden");
        } else {
            // Nur Tabellen aus der Whitelist
            tabellen = konfiguration.getWhitelist();
            Logger.info("Whitelist-Tabellen migrieren: " + tabellen.size() + " Tabellen konfiguriert");
        }
        
        for (String tabelle : tabellen) {
            if (konfiguration.sollTabelleMigriert(tabelle)) {
                Logger.info("Beginne Migration der Tabelle: " + tabelle);
                migrierenTabelle(tabelle);
            } else {
                Logger.info("Überspringe Tabelle (in Blacklist oder Präfix-Blacklist): " + tabelle);
            }
        }
        
        // Zusätzliche Datenbankobjekte migrieren
        if (konfiguration.isSequenzenMigrieren()) {
            migrierenSequenzen();
        }
        
        if (konfiguration.isIndizesMigrieren()) {
            migrierenIndizes();
        }
        
        if (konfiguration.isConstraintsMigrieren()) {
            migrierenConstraints();
        }
        
        if (konfiguration.isViewsMigrieren()) {
            migrierenViews();
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
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")) {
            
            while (rs.next()) {
                tabellen.add(rs.getString("TABLE_NAME"));
            }
        }
        
        return tabellen;
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
            Map<String, String> spaltenDefaults = new HashMap<>();
            
            try (ResultSet columnsRs = dbMetaData.getColumns(null, null, tabellenname.toUpperCase(), null)) {
                while (columnsRs.next()) {
                    String spaltenName = columnsRs.getString("COLUMN_NAME");
                    String typName = columnsRs.getString("TYPE_NAME");
                    int spaltenSize = columnsRs.getInt("COLUMN_SIZE");
                    int dezimalStellen = columnsRs.getInt("DECIMAL_DIGITS");
                    String nullbar = columnsRs.getString("IS_NULLABLE");
                    String defaultValue = columnsRs.getString("COLUMN_DEF");
                    
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
                    
                    // Default-Werte speichern
                    if (konfiguration.isSpaltenDefaultWerteUebertragen() && defaultValue != null && !defaultValue.trim().isEmpty()) {
                        spaltenDefaults.put(spaltenName, defaultValue.trim());
                    }
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
                
                // NULL/NOT NULL Constraints hinzufügen
                if (konfiguration.isSpaltenNullConstraintsUebertragen()) {
                    if (nullbarkeit.equals("NO")) {
                        spaltenDef.append(" NOT NULL");
                    }
                }
                
                // Default-Werte hinzufügen
                if (konfiguration.isSpaltenDefaultWerteUebertragen() && spaltenDefaults.containsKey(spaltenName)) {
                    String defaultValue = spaltenDefaults.get(spaltenName);
                    // Oracle-Default-Werte in PostgreSQL-Format konvertieren
                    defaultValue = konvertiereOracleDefaultZuPostgres(defaultValue);
                    spaltenDef.append(" DEFAULT ").append(defaultValue);
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
        
        // Alle Spaltendefinitionen zusammenfügen
        createSql.append(String.join(",\n", spaltenDefinitionen));
        createSql.append("\n);\n");
        
        // Spalten-Kommentare hinzufügen
        createSql.append(migriereSpaltenKommentare(tabellenname));
        
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
        
        // Spezielle Behandlung für NUMBER-Typen
        if (oracleDatentyp.startsWith("NUMBER(")) {
            // Extrahiere Precision und Scale aus NUMBER(p,s)
            String zahlenTeil = oracleDatentyp.substring(7, oracleDatentyp.length() - 1);
            String[] teile = zahlenTeil.split(",");
            
            if (teile.length >= 2) {
                try {
                    int precision = Integer.parseInt(teile[0].trim());
                    int scale = Integer.parseInt(teile[1].trim());
                    
                    // NUMBER(1,0) -> BOOLEAN (spezifisch)
                    if (precision == 1 && scale == 0) {
                        return "BOOLEAN";
                    }
                    
                    // Alle anderen NUMBER-Felder mit Scale 0 -> BIGINT
                    if (scale == 0) {
                        return "BIGINT";
                    }
                    
                    // NUMBER-Felder mit Dezimalstellen -> NUMERIC
                    return "NUMERIC";
                    
                } catch (NumberFormatException e) {
                    Logger.info("Fehler beim Parsen von NUMBER-Typ: " + oracleDatentyp);
                }
            }
            
            // Für NUMBER(p) ohne Scale - behandle als Scale 0
            if (teile.length == 1) {
                // Alle NUMBER(p) -> BIGINT
                return "BIGINT";
            }
        }
        
        // Behandle einfaches NUMBER ohne Klammern
        if (oracleDatentyp.equals("NUMBER")) {
            return "BIGINT";
        }
        
        // Exakte Übereinstimmung aus Properties prüfen
        if (mapping.containsKey(oracleDatentyp)) {
            return mapping.get(oracleDatentyp);
        }
        
        // Standardmappings für andere Typen
        if (oracleDatentyp.startsWith("VARCHAR2")) {
            return "VARCHAR" + oracleDatentyp.substring(7); // VARCHAR2(n) -> VARCHAR(n)
        } else if (oracleDatentyp.startsWith("CHAR")) {
            return oracleDatentyp; // CHAR(n) bleibt CHAR(n)
        }
        
        // Fallback für nicht erkannte Typen
        Logger.info("Unbekannter Datentyp: " + oracleDatentyp + ", verwende TEXT als Standard");
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
     * Migriert alle Sequenzen aus der Oracle-Datenbank und verhindert Überläufe.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenSequenzen() throws SQLException, IOException {
        Logger.info("Beginne Migration der Sequenzen");
        
        String sql = "SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CACHE_SIZE, CYCLE_FLAG, LAST_NUMBER " +
                    "FROM USER_SEQUENCES ORDER BY SEQUENCE_NAME";
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("-- Sequenzen Migration\n");
        sqlBuilder.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n");
        sqlBuilder.append("-- Überlauf-Schutz für PostgreSQL implementiert\n\n");
        
        // PostgreSQL BIGINT Grenzen
        final long PG_BIGINT_MIN = -9223372036854775808L;
        final long PG_BIGINT_MAX = 9223372036854775807L;
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String sequenzName = rs.getString("SEQUENCE_NAME");
                long minValue = rs.getLong("MIN_VALUE");
                long maxValue = rs.getLong("MAX_VALUE");
                long incrementBy = rs.getLong("INCREMENT_BY");
                int cache = rs.getInt("CACHE_SIZE");
                String cycle = rs.getString("CYCLE_FLAG");
                long lastNumber = rs.getLong("LAST_NUMBER");
                
                // Überlauf-Prüfung und Anpassung
                long adjustedMinValue = Math.max(minValue, PG_BIGINT_MIN);
                long adjustedMaxValue = Math.min(maxValue, PG_BIGINT_MAX);
                long adjustedStartValue = Math.max(lastNumber, adjustedMinValue);
                
                // Warnung bei Anpassungen
                if (minValue != adjustedMinValue) {
                    sqlBuilder.append("-- WARNUNG: MIN_VALUE angepasst von ").append(minValue)
                             .append(" auf ").append(adjustedMinValue).append(" (PostgreSQL Limit)\n");
                    Logger.info("Sequenz " + sequenzName + ": MIN_VALUE angepasst von " + minValue + " auf " + adjustedMinValue);
                }
                
                if (maxValue != adjustedMaxValue) {
                    sqlBuilder.append("-- WARNUNG: MAX_VALUE angepasst von ").append(maxValue)
                             .append(" auf ").append(adjustedMaxValue).append(" (PostgreSQL Limit)\n");
                    Logger.info("Sequenz " + sequenzName + ": MAX_VALUE angepasst von " + maxValue + " auf " + adjustedMaxValue);
                }
                
                // Prüfung auf potenzielle Überläufe
                if (incrementBy > 0 && adjustedStartValue > (adjustedMaxValue - incrementBy)) {
                    sqlBuilder.append("-- KRITISCH: Sequenz ").append(sequenzName)
                             .append(" könnte nach wenigen Inkrementen überlaufen!\n");
                    Logger.info("WARNUNG: Sequenz " + sequenzName + " könnte schnell überlaufen!");
                }
                
                sqlBuilder.append("CREATE SEQUENCE ").append(sequenzName);
                sqlBuilder.append("\n    START WITH ").append(adjustedStartValue);
                sqlBuilder.append("\n    INCREMENT BY ").append(incrementBy);
                sqlBuilder.append("\n    MINVALUE ").append(adjustedMinValue);
                sqlBuilder.append("\n    MAXVALUE ").append(adjustedMaxValue);
                sqlBuilder.append("\n    CACHE ").append(cache);
                
                if ("Y".equals(cycle)) {
                    sqlBuilder.append("\n    CYCLE");
                } else {
                    sqlBuilder.append("\n    NO CYCLE");
                }
                
                sqlBuilder.append(";\n");
                
                // Originalwerte als Kommentar hinzufügen
                sqlBuilder.append("-- Original Oracle Werte: MIN=").append(minValue)
                         .append(", MAX=").append(maxValue)
                         .append(", LAST=").append(lastNumber).append("\n\n");
            }
        }
        
        if (konfiguration.isOrdnerErstellen()) {
            speichereSQL(konfiguration.getOrdnerSequenzen() + File.separator + "sequences.sql", sqlBuilder.toString());
        } else {
            speichereSQL("sequences.sql", sqlBuilder.toString());
        }
        
        Logger.info("Sequenzen-Migration abgeschlossen");
    }
    
    /**
     * Migriert alle Indizes aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenIndizes() throws SQLException, IOException {
        Logger.info("Beginne Migration der Indizes");
        
        String sql = "SELECT i.INDEX_NAME, i.TABLE_NAME, i.UNIQUENESS, " +
                    "LISTAGG(ic.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) AS COLUMNS " +
                    "FROM USER_INDEXES i " +
                    "JOIN USER_IND_COLUMNS ic ON i.INDEX_NAME = ic.INDEX_NAME " +
                    "WHERE i.INDEX_TYPE = 'NORMAL' AND i.GENERATED = 'N' " +
                    "GROUP BY i.INDEX_NAME, i.TABLE_NAME, i.UNIQUENESS " +
                    "ORDER BY i.TABLE_NAME, i.INDEX_NAME";
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("-- Indizes Migration\n");
        sqlBuilder.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n\n");
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String uniqueness = rs.getString("UNIQUENESS");
                String columns = rs.getString("COLUMNS");
                
                sqlBuilder.append("CREATE ");
                
                if ("UNIQUE".equals(uniqueness)) {
                    sqlBuilder.append("UNIQUE ");
                }
                
                sqlBuilder.append("INDEX ").append(indexName);
                sqlBuilder.append(" ON ").append(tableName);
                sqlBuilder.append(" (").append(columns).append(");\n\n");
            }
        }
        
        if (konfiguration.isOrdnerErstellen()) {
            speichereSQL(konfiguration.getOrdnerIndizes() + File.separator + "indexes.sql", sqlBuilder.toString());
        } else {
            speichereSQL("indexes.sql", sqlBuilder.toString());
        }
        
        Logger.info("Indizes-Migration abgeschlossen");
    }
    
    /**
     * Migriert alle Constraints aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenConstraints() throws SQLException, IOException {
        Logger.info("Beginne Migration der Constraints");
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("-- Constraints Migration\n");
        sqlBuilder.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n\n");
        
        // Primary Key Constraints
        sqlBuilder.append("-- Primary Key Constraints\n");
        migrierePrimaryKeyConstraints(sqlBuilder);
        
        // Foreign Key Constraints
        sqlBuilder.append("\n-- Foreign Key Constraints\n");
        migriereForeignKeyConstraints(sqlBuilder);
        
        // Check Constraints
        sqlBuilder.append("\n-- Check Constraints\n");
        migriereCheckConstraints(sqlBuilder);
        
        if (konfiguration.isOrdnerErstellen()) {
            speichereSQL(konfiguration.getOrdnerConstraints() + File.separator + "constraints.sql", sqlBuilder.toString());
        } else {
            speichereSQL("constraints.sql", sqlBuilder.toString());
        }
        
        Logger.info("Constraints-Migration abgeschlossen");
    }
    
    /**
     * Migriert Primary Key Constraints.
     */
    private void migrierePrimaryKeyConstraints(StringBuilder sqlBuilder) throws SQLException {
        String sql = "SELECT c.CONSTRAINT_NAME, c.TABLE_NAME, " +
                    "LISTAGG(cc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY cc.POSITION) AS COLUMNS " +
                    "FROM USER_CONSTRAINTS c " +
                    "JOIN USER_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME " +
                    "WHERE c.CONSTRAINT_TYPE = 'P' " +
                    "GROUP BY c.CONSTRAINT_NAME, c.TABLE_NAME " +
                    "ORDER BY c.TABLE_NAME";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String columns = rs.getString("COLUMNS");
                
                sqlBuilder.append("ALTER TABLE ").append(tableName);
                sqlBuilder.append(" ADD CONSTRAINT ").append(constraintName);
                sqlBuilder.append(" PRIMARY KEY (").append(columns).append(");\n");
            }
        }
    }
    
    /**
     * Migriert Foreign Key Constraints.
     */
    private void migriereForeignKeyConstraints(StringBuilder sqlBuilder) throws SQLException {
        String sql = "SELECT c.CONSTRAINT_NAME, c.TABLE_NAME, c.R_CONSTRAINT_NAME, " +
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
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String columns = rs.getString("COLUMNS");
                String rTableName = rs.getString("R_TABLE_NAME");
                String rColumns = rs.getString("R_COLUMNS");
                
                sqlBuilder.append("ALTER TABLE ").append(tableName);
                sqlBuilder.append(" ADD CONSTRAINT ").append(constraintName);
                sqlBuilder.append(" FOREIGN KEY (").append(columns).append(")");
                sqlBuilder.append(" REFERENCES ").append(rTableName);
                sqlBuilder.append(" (").append(rColumns).append(");\n");
            }
        }
    }
    
    /**
     * Migriert Check Constraints.
     */
    private void migriereCheckConstraints(StringBuilder sqlBuilder) throws SQLException {
        String sql = "SELECT CONSTRAINT_NAME, TABLE_NAME, SEARCH_CONDITION " +
                    "FROM USER_CONSTRAINTS " +
                    "WHERE CONSTRAINT_TYPE = 'C' AND GENERATED = 'USER NAME' " +
                    "ORDER BY TABLE_NAME";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String tableName = rs.getString("TABLE_NAME");
                String searchCondition = rs.getString("SEARCH_CONDITION");
                
                sqlBuilder.append("ALTER TABLE ").append(tableName);
                sqlBuilder.append(" ADD CONSTRAINT ").append(constraintName);
                sqlBuilder.append(" CHECK (").append(searchCondition).append(");\n");
            }
        }
    }
    
    /**
     * Migriert alle Views aus der Oracle-Datenbank.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Ein-/Ausgabefehlern
     */
    private void migrierenViews() throws SQLException, IOException {
        Logger.info("Beginne Migration der Views");
        
        String sql = "SELECT VIEW_NAME, TEXT FROM USER_VIEWS ORDER BY VIEW_NAME";
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("-- Views Migration\n");
        sqlBuilder.append("-- Erstellt von Oracle-zu-PostgreSQL Migration\n\n");
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String viewName = rs.getString("VIEW_NAME");
                String viewText = rs.getString("TEXT");
                
                sqlBuilder.append("CREATE OR REPLACE VIEW ").append(viewName).append(" AS\n");
                sqlBuilder.append(viewText);
                sqlBuilder.append(";\n\n");
            }
        }
        
        if (konfiguration.isOrdnerErstellen()) {
            speichereSQL(konfiguration.getOrdnerViews() + File.separator + "views.sql", sqlBuilder.toString());
        } else {
            speichereSQL("views.sql", sqlBuilder.toString());
        }
        
        Logger.info("Views-Migration abgeschlossen");
    }
    
    /**
     * Konvertiert Oracle-Default-Werte in PostgreSQL-Format.
     * 
     * @param oracleDefault Der Oracle-Default-Wert
     * @return Der PostgreSQL-Default-Wert
     */
    private String konvertiereOracleDefaultZuPostgres(String oracleDefault) {
        if (oracleDefault == null || oracleDefault.trim().isEmpty()) {
            return "";
        }
        
        String defaultValue = oracleDefault.trim();
        
        // SYSDATE -> CURRENT_TIMESTAMP
        if (defaultValue.toUpperCase().equals("SYSDATE")) {
            return "CURRENT_TIMESTAMP";
        }
        
        // USER -> CURRENT_USER
        if (defaultValue.toUpperCase().equals("USER")) {
            return "CURRENT_USER";
        }
        
        // SYS_GUID() -> gen_random_uuid()
        if (defaultValue.toUpperCase().equals("SYS_GUID()")) {
            return "gen_random_uuid()";
        }
        
        // Sequenz-Aufrufe: SEQUENCE_NAME.NEXTVAL -> nextval('SEQUENCE_NAME')
        if (defaultValue.toUpperCase().contains(".NEXTVAL")) {
            String sequenceName = defaultValue.substring(0, defaultValue.toUpperCase().indexOf(".NEXTVAL"));
            return "nextval('" + sequenceName + "')";
        }
        
        // Strings sind bereits in Anführungszeichen
        // Numerische Werte bleiben gleich
        return defaultValue;
    }
    
    /**
     * Migriert Spalten-Kommentare.
     * 
     * @param tabellenname Der Name der Tabelle
     * @return SQL-Statements für Spalten-Kommentare
     * @throws SQLException Bei Datenbankfehlern
     */
    private String migriereSpaltenKommentare(String tabellenname) throws SQLException {
        if (!konfiguration.isSpaltenKommentareUebertragen()) {
            return "";
        }
        
        StringBuilder kommentarSql = new StringBuilder();
        kommentarSql.append("\n-- Spalten-Kommentare für Tabelle ").append(tabellenname).append("\n");
        
        String sql = "SELECT COLUMN_NAME, COMMENTS " +
                    "FROM USER_COL_COMMENTS " +
                    "WHERE TABLE_NAME = '" + tabellenname.toUpperCase() + "' " +
                    "AND COMMENTS IS NOT NULL " +
                    "ORDER BY COLUMN_NAME";
        
        try (Statement stmt = oracleConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String spaltenName = rs.getString("COLUMN_NAME");
                String kommentar = rs.getString("COMMENTS");
                
                // Ignorierte Spalten überspringen
                List<String> ignorierteSpalten = konfiguration.getIgnorierteSpalten(tabellenname);
                if (ignorierteSpalten.contains(spaltenName)) {
                    continue;
                }
                
                if (kommentar != null && !kommentar.trim().isEmpty()) {
                    kommentarSql.append("COMMENT ON COLUMN ").append(tabellenname).append(".")
                               .append(spaltenName).append(" IS '")
                               .append(kommentar.replace("'", "''")).append("';\n");
                }
            }
        }
        
        return kommentarSql.toString();
    }
}
