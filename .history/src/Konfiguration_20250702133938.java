import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Konfigurationsklasse zum Einlesen und Verwalten der Konfigurationseinstellungen
 * für die Datenbankmigration von Oracle zu PostgreSQL.
 */
public class Konfiguration {
    // Datenbank-Verbindungsinformationen
    private String oracleUrl;
    private String oracleBenutzer;
    private String oraclePasswort;
    private String postgresUrl;
    private String postgresBenutzer;
    private String postgresPasswort;
    
    // Listen für Tabellennamen
    private List<String> whitelist; // Tabellen, die migriert werden sollen
    private List<String> blacklist; // Tabellen, die nicht migriert werden sollen
    
    // Map für Spalten, die pro Tabelle nicht migriert werden sollen
    private Map<String, List<String>> ignorierteSpalten;
    
    // Map für Datentyp-Mappings (Oracle -> PostgreSQL)
    private Map<String, String> datentypMapping;
    
    // Map für Wertetransformationen
    private Map<String, Map<String, String>> wertetransformationen;
    
    // Ausgabepfad für SQL-Dateien
    private String ausgabePfad;
    
    // Liste der Präfixe für Boolean-Spalten
    private List<String> booleanPraefixe;
    // Liste der Suffixe für Boolean-Spalten
    private List<String> booleanSuffixe;
    // Liste exakter Spaltennamen, die als Boolean behandelt werden sollen
    private List<String> booleanNamen;
    
    // Option für automatische Tabellenerkennung
    private boolean alleTabellen;
    
    // Neue Eigenschaften für erweiterte Migration
    private boolean migrierenIndizes;
    private boolean migrierenSequenzen;
    private boolean migrierenViews;
    private boolean migrierenTriggers;
    private boolean migrierenFunktionen;
    private boolean migrierenProzeduren;
    private boolean migrierenSynonyme;
    private boolean migrierenConstraints;
    
    // Namens-Mappings
    private String schemaMapping;
    private String indexPrefix;
    private String sequenceSuffix;
    private String constraintPrefix;
    
    /**
     * Konstruktor, der die Konfiguration aus einer Datei einliest.
     * 
     * @param konfigDatei Pfad zur Konfigurationsdatei
     * @throws IOException wenn die Datei nicht gelesen werden kann
     */
    public Konfiguration(String konfigDatei) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(konfigDatei));
        
        // Datenbankverbindungen einlesen
        oracleUrl = props.getProperty("oracle.url");
        oracleBenutzer = props.getProperty("oracle.benutzer");
        oraclePasswort = props.getProperty("oracle.passwort");
        postgresUrl = props.getProperty("postgres.url");
        postgresBenutzer = props.getProperty("postgres.benutzer");
        postgresPasswort = props.getProperty("postgres.passwort");
        
        // Ausgabepfad einlesen
        ausgabePfad = props.getProperty("ausgabe.pfad", "./output/");
        
        // Whitelist und Blacklist einlesen
        whitelist = leseListeEin(props, "tabellen.whitelist");
        blacklist = leseListeEin(props, "tabellen.blacklist");
        
        // Ignorierte Spalten einlesen
        ignorierteSpalten = new HashMap<>();
        for (String tabelle : whitelist) {
            List<String> spalten = leseListeEin(props, "tabelle." + tabelle + ".ignorierte_spalten");
            if (!spalten.isEmpty()) {
                ignorierteSpalten.put(tabelle, spalten);
            }
        }
        
        // Datentyp-Mappings einlesen
        datentypMapping = new HashMap<>();
        String mappingString = props.getProperty("datentyp.mapping", "");
        String[] mappings = mappingString.split(";");
        for (String mapping : mappings) {
            if (!mapping.trim().isEmpty()) {
                String[] teile = mapping.split("->");
                if (teile.length == 2) {
                    datentypMapping.put(teile[0].trim(), teile[1].trim());
                }
            }
        }
        
        // Standard-Mappings hinzufügen, falls nicht in Konfiguration definiert
        if (!datentypMapping.containsKey("NUMBER(1)")) {
            datentypMapping.put("NUMBER(1)", "BOOLEAN");
        }
        
        // Wertetransformationen einlesen
        wertetransformationen = new HashMap<>();
        for (String oracleTyp : datentypMapping.keySet()) {
            String pgTyp = datentypMapping.get(oracleTyp);
            
            // Beispiel für NUMBER(1) -> BOOLEAN Transformation
            if (pgTyp.equals("BOOLEAN")) {
                Map<String, String> wertMap = new HashMap<>();
                wertMap.put("0", "false");
                wertMap.put("1", "true");
                wertetransformationen.put(oracleTyp, wertMap);
            }
            
            // Weitere benutzerdefinierte Transformationen aus der Konfiguration laden
            String transformKey = "transform." + oracleTyp.replaceAll("[^a-zA-Z0-9]", "_");
            String transformString = props.getProperty(transformKey, "");
            if (!transformString.isEmpty()) {
                Map<String, String> wertMap = wertetransformationen.getOrDefault(oracleTyp, new HashMap<>());
                String[] wertPaare = transformString.split(";");
                for (String paar : wertPaare) {
                    if (!paar.trim().isEmpty()) {
                        String[] werte = paar.split("->");
                        if (werte.length == 2) {
                            wertMap.put(werte[0].trim(), werte[1].trim());
                        }
                    }
                }
                wertetransformationen.put(oracleTyp, wertMap);
            }
        }
        
        // Boolean-Spalten-Konfiguration einlesen
        booleanPraefixe = leseListeEin(props, "spalte.praefixe.boolean");
        booleanSuffixe = leseListeEin(props, "spalte.suffixe.boolean");
        booleanNamen = leseListeEin(props, "spalte.namen.boolean");
        
        // Erweiterte Migration-Optionen einlesen
        migrierenIndizes = Boolean.parseBoolean(props.getProperty("migration.indizes", "true"));
        migrierenSequenzen = Boolean.parseBoolean(props.getProperty("migration.sequenzen", "true"));
        migrierenViews = Boolean.parseBoolean(props.getProperty("migration.views", "true"));
        migrierenTriggers = Boolean.parseBoolean(props.getProperty("migration.triggers", "false"));
        migrierenFunktionen = Boolean.parseBoolean(props.getProperty("migration.funktionen", "false"));
        migrierenProzeduren = Boolean.parseBoolean(props.getProperty("migration.prozeduren", "false"));
        migrierenSynonyme = Boolean.parseBoolean(props.getProperty("migration.synonyme", "false"));
        migrierenConstraints = Boolean.parseBoolean(props.getProperty("migration.constraints", "true"));
        
        // Namens-Mappings einlesen
        schemaMapping = props.getProperty("schema.mapping", "");
        indexPrefix = props.getProperty("index.prefix", "idx_");
        sequenceSuffix = props.getProperty("sequence.suffix", "_seq");
        constraintPrefix = props.getProperty("constraint.prefix", "");
        
        // Option für automatische Tabellenerkennung
        alleTabellen = Boolean.parseBoolean(props.getProperty("migration.alle_tabellen", "false"));
    }
    
    /**
     * Liest eine durch Komma getrennte Liste aus den Properties ein.
     */
    private List<String> leseListeEin(Properties props, String key) {
        List<String> liste = new ArrayList<>();
        String wert = props.getProperty(key, "");
        if (!wert.trim().isEmpty()) {
            for (String element : wert.split(",")) {
                String trimmed = element.trim();
                if (!trimmed.isEmpty()) {
                    liste.add(trimmed);
                }
            }
        }
        return liste;
    }
    
    // Getter-Methoden
    public String getOracleUrl() { return oracleUrl; }
    public String getOracleBenutzer() { return oracleBenutzer; }
    public String getOraclePasswort() { return oraclePasswort; }
    public String getPostgresUrl() { return postgresUrl; }
    public String getPostgresBenutzer() { return postgresBenutzer; }
    public String getPostgresPasswort() { return postgresPasswort; }
    
    public List<String> getWhitelist() { return whitelist; }
    public List<String> getBlacklist() { return blacklist; }
    
    public Map<String, List<String>> getIgnorierteSpalten() { return ignorierteSpalten; }
    public List<String> getIgnorierteSpalten(String tabelle) {
        return ignorierteSpalten.getOrDefault(tabelle, new ArrayList<>());
    }
    
    public Map<String, String> getDatentypMapping() { return datentypMapping; }
    
    public Map<String, Map<String, String>> getWertetransformationen() { return wertetransformationen; }
    public Map<String, String> getWertetransformation(String datentyp) {
        return wertetransformationen.getOrDefault(datentyp, new HashMap<>());
    }
    
    public String getAusgabePfad() { return ausgabePfad; }
    
    public List<String> getBooleanPraefixe() { return booleanPraefixe; }
    public List<String> getBooleanSuffixe() { return booleanSuffixe; }
    public List<String> getBooleanNamen() { return booleanNamen; }
    
    public boolean isMigrierenIndizes() { return migrierenIndizes; }
    public boolean isMigrierenSequenzen() { return migrierenSequenzen; }
    public boolean isMigrierenViews() { return migrierenViews; }
    public boolean isMigrierenTriggers() { return migrierenTriggers; }
    public boolean isMigrierenFunktionen() { return migrierenFunktionen; }
    public boolean isMigrierenProzeduren() { return migrierenProzeduren; }
    public boolean isMigrierenSynonyme() { return migrierenSynonyme; }
    public boolean isMigrierenConstraints() { return migrierenConstraints; }
    
    public String getSchemaMapping() { return schemaMapping; }
    public String getIndexPrefix() { return indexPrefix; }
    public String getSequenceSuffix() { return sequenceSuffix; }
    public String getConstraintPrefix() { return constraintPrefix; }
    
    /**
     * Prüft, ob eine Spalte basierend auf dem Namen als Boolean behandelt werden soll.
     * 
     * @param spaltenName Name der Spalte
     * @return true, wenn die Spalte als Boolean behandelt werden soll
     */
    public boolean istBooleanSpalte(String spaltenName) {
        // Prüfen auf exakte Übereinstimmung
        for (String name : booleanNamen) {
            if (spaltenName.equalsIgnoreCase(name)) {
                return true;
            }
        }
        
        // Prüfen auf Präfix
        for (String prefix : booleanPraefixe) {
            if (spaltenName.toLowerCase().startsWith(prefix.toLowerCase())) {
                return true;
            }
        }
        
        // Prüfen auf Suffix
        for (String suffix : booleanSuffixe) {
            if (spaltenName.toLowerCase().endsWith(suffix.toLowerCase())) {
                return true;
            }
        }
        
        return false;
    }

    public boolean isAlleTabellen() {
        return alleTabellen;
    }
}
