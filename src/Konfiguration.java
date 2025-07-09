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
    private List<String> blacklistPraefixe; // Präfixe für Tabellen, die nicht migriert werden sollen
    
    // Map für Spalten, die pro Tabelle nicht migriert werden sollen
    private Map<String, List<String>> ignorierteSpalten;
    
    // Map für Datentyp-Mappings (Oracle -> PostgreSQL)
    private Map<String, String> datentypMapping;
    
    // Map für Wertetransformationen
    private Map<String, Map<String, String>> wertetransformationen;
    
    // Ausgabepfad für SQL-Dateien
    private String ausgabePfad;
    
    // Migration von zusätzlichen Datenbankobjekten
    private boolean sequenzenMigrieren;
    private boolean indizesMigrieren;
    private boolean constraintsMigrieren;
    private boolean viewsMigrieren;
    
    // Ordner für zusätzliche Datenbankobjekte
    private boolean ordnerErstellen;
    private String ordnerSequenzen;
    private String ordnerIndizes;
    private String ordnerConstraints;
    private String ordnerViews;
    
    // Alle Tabellen migrieren
    private boolean alleTabellenMigrieren;
    
    // Spalten-Eigenschaften übertragen
    private boolean spaltenNullConstraintsUebertragen;
    private boolean spaltenDefaultWerteUebertragen;
    private boolean spaltenKommentareUebertragen;
    
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
        
        // Migration von zusätzlichen Datenbankobjekten
        sequenzenMigrieren = Boolean.parseBoolean(props.getProperty("sequenzen.migrieren", "false"));
        indizesMigrieren = Boolean.parseBoolean(props.getProperty("indizes.migrieren", "false"));
        constraintsMigrieren = Boolean.parseBoolean(props.getProperty("constraints.migrieren", "false"));
        viewsMigrieren = Boolean.parseBoolean(props.getProperty("views.migrieren", "false"));
        
        // Ordner für zusätzliche Datenbankobjekte
        ordnerErstellen = Boolean.parseBoolean(props.getProperty("ordner.erstellen", "false"));
        ordnerSequenzen = props.getProperty("ordner.sequenzen", "sequences");
        ordnerIndizes = props.getProperty("ordner.indizes", "indices");
        ordnerConstraints = props.getProperty("ordner.constraints", "constraints");
        ordnerViews = props.getProperty("ordner.views", "views");
        
        // Alle Tabellen migrieren
        alleTabellenMigrieren = Boolean.parseBoolean(props.getProperty("alle.tabellen.migrieren", "false"));
        
        // Spalten-Eigenschaften übertragen
        spaltenNullConstraintsUebertragen = Boolean.parseBoolean(props.getProperty("spalten.null_constraints.uebertragen", "false"));
        spaltenDefaultWerteUebertragen = Boolean.parseBoolean(props.getProperty("spalten.default_werte.uebertragen", "false"));
        spaltenKommentareUebertragen = Boolean.parseBoolean(props.getProperty("spalten.kommentare.uebertragen", "false"));
        
        // Whitelist und Blacklist einlesen
        whitelist = leseListeEin(props, "tabellen.whitelist");
        blacklist = leseListeEin(props, "tabellen.blacklist");
        blacklistPraefixe = leseListeEin(props, "tabellen.blacklist.praefixe");
        
        // Alle Tabellen migrieren
        alleTabellenMigrieren = Boolean.parseBoolean(props.getProperty("alle.tabellen.migrieren", "false"));
        
        // Ignorierte Spalten einlesen
        ignorierteSpalten = new HashMap<>();
        List<String> tabellenFuerIgnoriert = alleTabellenMigrieren ? new ArrayList<>() : whitelist;
        
        // Bei "alle Tabellen migrieren" müssen wir die Tabellen für ignorierte Spalten aus den Properties lesen
        if (alleTabellenMigrieren) {
            for (String key : props.stringPropertyNames()) {
                if (key.startsWith("tabelle.") && key.endsWith(".ignorierte_spalten")) {
                    String tabelle = key.substring(8, key.length() - 19); // "tabelle." und ".ignorierte_spalten" entfernen
                    tabellenFuerIgnoriert.add(tabelle);
                }
            }
        }
        
        for (String tabelle : tabellenFuerIgnoriert) {
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
        
        // Boolean-Präfixe einlesen (nicht mehr verwendet)
        // booleanPraefixe = leseListeEin(props, "spalte.praefixe.boolean");
        // booleanSuffixe = leseListeEin(props, "spalte.suffixe.boolean");
        // booleanNamen = leseListeEin(props, "spalte.namen.boolean");
        
        // Standard-Präfixe hinzufügen, falls nicht in Konfiguration definiert (nicht mehr verwendet)
        // if (booleanPraefixe.isEmpty()) {
        //     booleanPraefixe.add("ist");
        // }
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
    public List<String> getBlacklistPraefixe() { return blacklistPraefixe; }
    
    /**
     * Prüft, ob eine Tabelle migriert werden soll.
     * 
     * @param tabellenName Name der Tabelle
     * @return true, wenn die Tabelle migriert werden soll
     */
    public boolean sollTabelleMigriert(String tabellenName) {
        // Prüfung auf Blacklist
        if (blacklist.contains(tabellenName)) {
            return false;
        }
        
        // Prüfung auf Blacklist-Präfixe
        for (String prefix : blacklistPraefixe) {
            if (tabellenName.startsWith(prefix)) {
                return false;
            }
        }
        
        return true;
    }
    
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
    
    // Getter für zusätzliche Datenbankobjekte
    public boolean isSequenzenMigrieren() { return sequenzenMigrieren; }
    public boolean isIndizesMigrieren() { return indizesMigrieren; }
    public boolean isConstraintsMigrieren() { return constraintsMigrieren; }
    public boolean isViewsMigrieren() { return viewsMigrieren; }
    
    // Getter für Ordner-Konfiguration
    public boolean isOrdnerErstellen() { return ordnerErstellen; }
    public String getOrdnerSequenzen() { return ordnerSequenzen; }
    public String getOrdnerIndizes() { return ordnerIndizes; }
    public String getOrdnerConstraints() { return ordnerConstraints; }
    public String getOrdnerViews() { return ordnerViews; }
    
    // Getter für Tabellen-Migration
    public boolean isAlleTabellenMigrieren() { return alleTabellenMigrieren; }
    
    // Getter für Spalten-Eigenschaften
    public boolean isSpaltenNullConstraintsUebertragen() { return spaltenNullConstraintsUebertragen; }
    public boolean isSpaltenDefaultWerteUebertragen() { return spaltenDefaultWerteUebertragen; }
    public boolean isSpaltenKommentareUebertragen() { return spaltenKommentareUebertragen; }
}
