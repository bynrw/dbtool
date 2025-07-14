package de.dbmigration.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import de.dbmigration.util.Logger;

/**
 * Konfigurationsklasse zum Einlesen und Verwalten der Konfigurationseinstellungen
 * für die Datenbankmigration von Oracle zu PostgreSQL.
 */
public class Konfiguration {
    // Konstanten für Konfigurationsschlüssel
    private static final String KEY_ORACLE_URL = "oracle.url";
    private static final String KEY_ORACLE_BENUTZER = "oracle.benutzer";
    private static final String KEY_ORACLE_PASSWORT = "oracle.passwort";
    private static final String KEY_POSTGRES_URL = "postgres.url";
    private static final String KEY_POSTGRES_BENUTZER = "postgres.benutzer";
    private static final String KEY_POSTGRES_PASSWORT = "postgres.passwort";
    private static final String KEY_AUSGABE_PFAD = "ausgabe.pfad";
    private static final String KEY_ORDNER_ERSTELLEN = "ordner.erstellen";
    private static final String KEY_SEQUENZEN_MIGRIEREN = "sequenzen.migrieren";
    private static final String KEY_INDIZES_MIGRIEREN = "indizes.migrieren";
    private static final String KEY_CONSTRAINTS_MIGRIEREN = "constraints.migrieren";
    private static final String KEY_VIEWS_MIGRIEREN = "views.migrieren";
    private static final String KEY_ALLE_TABELLEN_MIGRIEREN = "alle.tabellen.migrieren";
    
    // Datenbank-Verbindungsinformationen
    private String oracleUrl;
    private String oracleBenutzer;
    private String oraclePasswort;
    private String postgresUrl;
    private String postgresBenutzer;
    private String postgresPasswort;
    
    // Dynamische Werte (zur Laufzeit gesetzt)
    private String datenbankName;
    private String umgebung;
    private String zeitstempel;
    
    // Listen für Tabellennamen
    private List<String> whitelist; // Tabellen, die migriert werden sollen
    private List<String> blacklist; // Tabellen, die nicht migriert werden sollen
    private List<String> blacklistPraefixe; // Präfixe für Tabellen, die nicht migriert werden sollen
    
    // Map für Spalten, die pro Tabelle nicht migriert werden sollen
    private Map<String, List<String>> ignorierteSpalten;
    
    // Map für Datentyp-Mappings (Oracle -> PostgreSQL)
    private Map<String, String> datentypMapping;
    
    // Map für Wertetransformationen
    private Map<String, Map<String, String>> wertetransformationen;
    
    // Ausgabepfad für SQL-Dateien
    private String ausgabePfad;
    
    // Ordnernamen für verschiedene Objekttypen
    private String ordnerTablesCreate = "tables_create";
    private String ordnerTablesInserts = "tables_inserts";
    private String ordnerSequenzen = "sequences";
    private String ordnerIndizes = "indexes";
    private String ordnerConstraints = "constraints";
    private String ordnerViews = "views";
    
    // Migration von zusätzlichen Datenbankobjekten
    private boolean sequenzenMigrieren;
    private boolean indizesMigrieren;
    private boolean constraintsMigrieren;
    private boolean viewsMigrieren;
    private boolean alleTabellenMigrieren;
    
    // Spalten-Optionen
    private boolean spaltenNullConstraintsUebertragen;
    private boolean spaltenDefaultWerteUebertragen;
    private boolean spaltenKommentareUebertragen;
    
    // Ordnerstruktur
    private boolean ordnerErstellen;
    
    /**
     * Konstruktor, der die Konfigurationsdatei einliest.
     * 
     * @param dateiPfad Pfad zur Konfigurationsdatei
     * @throws IOException wenn die Konfigurationsdatei nicht gelesen werden kann
     */
    public Konfiguration(String dateiPfad) throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(dateiPfad)) {
            properties.load(fis);
        }
        
        // Datenbank-Verbindungsinformationen
        this.oracleUrl = properties.getProperty(KEY_ORACLE_URL, "");
        this.oracleBenutzer = properties.getProperty(KEY_ORACLE_BENUTZER, "");
        this.oraclePasswort = properties.getProperty(KEY_ORACLE_PASSWORT, "");
        this.postgresUrl = properties.getProperty(KEY_POSTGRES_URL, "");
        this.postgresBenutzer = properties.getProperty(KEY_POSTGRES_BENUTZER, "");
        this.postgresPasswort = properties.getProperty(KEY_POSTGRES_PASSWORT, "");
        
        // Ausgabepfad
        this.ausgabePfad = properties.getProperty(KEY_AUSGABE_PFAD, "output");
        
        // Ordnerstrukturen
        this.ordnerErstellen = Boolean.parseBoolean(properties.getProperty(KEY_ORDNER_ERSTELLEN, "true"));
        
        // Migration von zusätzlichen Datenbankobjekten
        this.sequenzenMigrieren = Boolean.parseBoolean(properties.getProperty(KEY_SEQUENZEN_MIGRIEREN, "true"));
        this.indizesMigrieren = Boolean.parseBoolean(properties.getProperty(KEY_INDIZES_MIGRIEREN, "true"));
        this.constraintsMigrieren = Boolean.parseBoolean(properties.getProperty(KEY_CONSTRAINTS_MIGRIEREN, "true"));
        this.viewsMigrieren = Boolean.parseBoolean(properties.getProperty(KEY_VIEWS_MIGRIEREN, "true"));
        this.alleTabellenMigrieren = Boolean.parseBoolean(properties.getProperty(KEY_ALLE_TABELLEN_MIGRIEREN, "true"));
        
        // Spaltenoptionen
        this.spaltenNullConstraintsUebertragen = Boolean.parseBoolean(properties.getProperty("spalten.null.constraints.uebertragen", "true"));
        this.spaltenDefaultWerteUebertragen = Boolean.parseBoolean(properties.getProperty("spalten.default.werte.uebertragen", "true"));
        this.spaltenKommentareUebertragen = Boolean.parseBoolean(properties.getProperty("spalten.kommentare.uebertragen", "true"));
        
        // Optionale Ordnernamen
        this.ordnerTablesCreate = properties.getProperty("ordner.tables.create", "tables_create");
        this.ordnerTablesInserts = properties.getProperty("ordner.tables.inserts", "tables_inserts");
        this.ordnerSequenzen = properties.getProperty("ordner.sequenzen", "sequences");
        this.ordnerIndizes = properties.getProperty("ordner.indizes", "indexes");
        this.ordnerConstraints = properties.getProperty("ordner.constraints", "constraints");
        this.ordnerViews = properties.getProperty("ordner.views", "views");
        
        // Listen für Tabellenfilter initialisieren
        initializeTableLists(properties);
        
        // Map für ignorierte Spalten initialisieren
        initializeIgnoredColumns(properties);
        
        // Map für Datentyp-Mappings initialisieren
        initializeDatatypeMappings(properties);
        
        // Map für Wertetransformationen initialisieren
        initializeValueTransformations(properties);
        
        Logger.info("Konfiguration aus Datei '" + dateiPfad + "' geladen");
    }
    
    /**
     * Initialisiert die Listen für Whitelist, Blacklist und Blacklist-Präfixe.
     * 
     * @param properties Die Properties aus der Konfigurationsdatei
     */
    private void initializeTableLists(Properties properties) {
        this.whitelist = new ArrayList<>();
        String whitelistStr = properties.getProperty("tabellen.whitelist", "");
        if (!whitelistStr.isEmpty()) {
            whitelist.addAll(Arrays.asList(whitelistStr.split(",")));
        }
        
        this.blacklist = new ArrayList<>();
        String blacklistStr = properties.getProperty("tabellen.blacklist", "");
        if (!blacklistStr.isEmpty()) {
            blacklist.addAll(Arrays.asList(blacklistStr.split(",")));
        }
        
        this.blacklistPraefixe = new ArrayList<>();
        String blacklistPraefixeStr = properties.getProperty("tabellen.blacklist.praefixe", "");
        if (!blacklistPraefixeStr.isEmpty()) {
            blacklistPraefixe.addAll(Arrays.asList(blacklistPraefixeStr.split(",")));
        }
    }
    
    /**
     * Initialisiert die Map für ignorierte Spalten.
     * 
     * @param properties Die Properties aus der Konfigurationsdatei
     */
    private void initializeIgnoredColumns(Properties properties) {
        this.ignorierteSpalten = new HashMap<>();
        
        // Format: tabelle.TABELLENNAME.ignorierte.spalten=spalte1,spalte2,...
        for (Object key : properties.keySet()) {
            String keyStr = key.toString();
            if (keyStr.startsWith("tabelle.") && keyStr.endsWith(".ignorierte.spalten")) {
                String tabelle = keyStr.substring(8, keyStr.length() - 19);
                String spaltenStr = properties.getProperty(keyStr, "");
                List<String> spalten = new ArrayList<>();
                if (!spaltenStr.isEmpty()) {
                    spalten.addAll(Arrays.asList(spaltenStr.split(",")));
                }
                ignorierteSpalten.put(tabelle, spalten);
            }
        }
    }
    
    /**
     * Initialisiert die Map für Datentyp-Mappings.
     * 
     * @param properties Die Properties aus der Konfigurationsdatei
     */
    private void initializeDatatypeMappings(Properties properties) {
        this.datentypMapping = new HashMap<>();
        
        // Format: datentyp.mapping.ORACLE_TYPE=POSTGRES_TYPE
        for (Object key : properties.keySet()) {
            String keyStr = key.toString();
            if (keyStr.startsWith("datentyp.mapping.")) {
                String oracleType = keyStr.substring(16);
                String postgresType = properties.getProperty(keyStr, "");
                if (!postgresType.isEmpty()) {
                    datentypMapping.put(oracleType, postgresType);
                }
            }
        }
        
        // Standard-Mappings hinzufügen, wenn nicht bereits konfiguriert
        addDefaultMappingIfAbsent("VARCHAR2", "VARCHAR");
        addDefaultMappingIfAbsent("NUMBER", "NUMERIC");
        addDefaultMappingIfAbsent("DATE", "TIMESTAMP");
        addDefaultMappingIfAbsent("TIMESTAMP", "TIMESTAMP");
        addDefaultMappingIfAbsent("CLOB", "TEXT");
        addDefaultMappingIfAbsent("BLOB", "BYTEA");
    }
    
    /**
     * Fügt ein Standard-Datentyp-Mapping hinzu, wenn es nicht bereits existiert.
     * 
     * @param oracleType Der Oracle-Datentyp
     * @param postgresType Der PostgreSQL-Datentyp
     */
    private void addDefaultMappingIfAbsent(String oracleType, String postgresType) {
        if (!datentypMapping.containsKey(oracleType)) {
            datentypMapping.put(oracleType, postgresType);
        }
    }
    
    /**
     * Initialisiert die Map für Wertetransformationen.
     * 
     * @param properties Die Properties aus der Konfigurationsdatei
     */
    private void initializeValueTransformations(Properties properties) {
        this.wertetransformationen = new HashMap<>();
        
        // Format: wertetransformation.DATATYPE.FROM=TO
        for (Object key : properties.keySet()) {
            String keyStr = key.toString();
            if (keyStr.startsWith("wertetransformation.")) {
                int lastDotIndex = keyStr.lastIndexOf('.');
                if (lastDotIndex > 0 && lastDotIndex < keyStr.length() - 1) {
                    String dataType = keyStr.substring(18, lastDotIndex);
                    String fromValue = keyStr.substring(lastDotIndex + 1);
                    String toValue = properties.getProperty(keyStr, "");
                    
                    if (!toValue.isEmpty()) {
                        if (!wertetransformationen.containsKey(dataType)) {
                            wertetransformationen.put(dataType, new HashMap<>());
                        }
                        wertetransformationen.get(dataType).put(fromValue, toValue);
                    }
                }
            }
        }
        
        // Standard-Transformationen für NUMBER(1) hinzufügen (0=false, 1=true)
        if (!wertetransformationen.containsKey("NUMBER(1)")) {
            Map<String, String> booleanTransformations = new HashMap<>();
            booleanTransformations.put("0", "false");
            booleanTransformations.put("1", "true");
            wertetransformationen.put("NUMBER(1)", booleanTransformations);
        }
    }
    
    /**
     * Prüft, ob eine Tabelle migriert werden soll.
     * 
     * @param tabellenName Der Name der Tabelle
     * @return true, wenn die Tabelle migriert werden soll, sonst false
     */
    public boolean sollTabelleMigriert(String tabellenName) {
        if (tabellenName == null || tabellenName.trim().isEmpty()) {
            return false;
        }
        
        // Prüfen, ob die Tabelle in der Blacklist ist
        if (blacklist.contains(tabellenName)) {
            return false;
        }
        
        // Prüfen, ob die Tabelle mit einem Präfix aus der Blacklist beginnt
        for (String praefix : blacklistPraefixe) {
            if (tabellenName.startsWith(praefix)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Liefert eine Liste von Spaltennamen, die für eine bestimmte Tabelle ignoriert werden sollen.
     * 
     * @param tabellenName Der Name der Tabelle
     * @return Liste der zu ignorierenden Spaltennamen (leer, wenn keine ignoriert werden sollen)
     */
    public List<String> getIgnorierteSpalten(String tabellenName) {
        List<String> result = ignorierteSpalten.get(tabellenName);
        return result != null ? result : new ArrayList<>();
    }
    
    /**
     * Liefert eine Map für Wertetransformationen für einen bestimmten Datentyp.
     * 
     * @param datentyp Der Datentyp
     * @return Map mit Wertetransformationen (from -> to)
     */
    public Map<String, String> getWertetransformation(String datentyp) {
        return wertetransformationen.getOrDefault(datentyp, new HashMap<>());
    }
    
    // Getter und Setter
    
    public String getOracleUrl() {
        return oracleUrl;
    }
    
    public String getOracleBenutzer() {
        return oracleBenutzer;
    }
    
    public String getOraclePasswort() {
        return oraclePasswort;
    }
    
    public String getPostgresUrl() {
        return postgresUrl;
    }
    
    public String getPostgresBenutzer() {
        return postgresBenutzer;
    }
    
    public String getPostgresPasswort() {
        return postgresPasswort;
    }
    
    public String getAusgabePfad() {
        return ausgabePfad;
    }
    
    public List<String> getWhitelist() {
        return whitelist;
    }
    
    public List<String> getBlacklist() {
        return blacklist;
    }
    
    public List<String> getBlacklistPraefixe() {
        return blacklistPraefixe;
    }
    
    public Map<String, String> getDatentypMapping() {
        return datentypMapping;
    }
    
    public boolean isSequenzenMigrieren() {
        return sequenzenMigrieren;
    }
    
    public boolean isIndizesMigrieren() {
        return indizesMigrieren;
    }
    
    public boolean isConstraintsMigrieren() {
        return constraintsMigrieren;
    }
    
    public boolean isViewsMigrieren() {
        return viewsMigrieren;
    }
    
    public boolean isAlleTabellenMigrieren() {
        return alleTabellenMigrieren;
    }
    
    public boolean isOrdnerErstellen() {
        return ordnerErstellen;
    }
    
    public String getOrdnerTablesCreate() {
        return ordnerTablesCreate;
    }
    
    public String getOrdnerTablesInserts() {
        return ordnerTablesInserts;
    }
    
    public String getOrdnerSequenzen() {
        return ordnerSequenzen;
    }
    
    public String getOrdnerIndizes() {
        return ordnerIndizes;
    }
    
    public String getOrdnerConstraints() {
        return ordnerConstraints;
    }
    
    public String getOrdnerViews() {
        return ordnerViews;
    }
    
    public boolean isSpaltenNullConstraintsUebertragen() {
        return spaltenNullConstraintsUebertragen;
    }
    
    public boolean isSpaltenDefaultWerteUebertragen() {
        return spaltenDefaultWerteUebertragen;
    }
    
    public boolean isSpaltenKommentareUebertragen() {
        return spaltenKommentareUebertragen;
    }
    
    public String getDatenbankName() {
        return datenbankName;
    }
    
    public void setDatenbankName(String datenbankName) {
        this.datenbankName = datenbankName;
    }
    
    public String getUmgebung() {
        return umgebung;
    }
    
    public void setUmgebung(String umgebung) {
        this.umgebung = umgebung;
    }
    
    public String getZeitstempel() {
        return zeitstempel;
    }
    
    public void setZeitstempel(String zeitstempel) {
        this.zeitstempel = zeitstempel;
    }
}
