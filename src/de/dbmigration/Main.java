package de.dbmigration;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;

import de.dbmigration.core.Konfiguration;
import de.dbmigration.core.OracleToPgMigrator;
import de.dbmigration.db.DatabaseManager;
import de.dbmigration.ui.InputHandler;
import de.dbmigration.util.Logger;

/**
 * Hauptklasse zur Ausführung der Oracle-zu-PostgreSQL-Migration.
 */
public class Main {
    /**
     * Einstiegspunkt für das Migrationstool.
     * 
     * @param args Kommandozeilenargumente:
     *             - Ohne Parameter: Interaktive Auswahl von Netzwerk und Umgebung
     *             - Mit Parameter: Pfad zur Konfigurationsdatei oder Netzwerk- und Umgebungsparameter
     *             - Format für Netzwerk und Umgebung: "netz=lvn umgebung=prod"
     */
    public static void main(String[] args) {
        // Ausgabeordner für Logdateien sicherstellen
        File logDir = new File("logs");
        if (!logDir.exists()) {
            logDir.mkdir();
        }
        
        try {
            // Logger initialisieren
            String logDateiName = "logs/migration_" + System.currentTimeMillis() + ".log";
            Logger.initialisieren(logDateiName);
            
            // InputHandler für Benutzereingaben
            InputHandler inputHandler = new InputHandler();
            
            String netzwerkName = null;
            String umgebungName = null;
            String konfigDatei = null;
            
            // Prüfen auf Parameter für Netzwerk und Umgebung
            if (args.length >= 2) {
                for (String arg : args) {
                    if (arg.toLowerCase().startsWith("netz=")) {
                        netzwerkName = arg.substring(5).trim();
                    } else if (arg.toLowerCase().startsWith("umgebung=")) {
                        umgebungName = arg.substring(9).trim();
                    }
                }
            }
            
            // Wenn Netzwerk und Umgebung als Parameter übergeben wurden
            if (netzwerkName != null && umgebungName != null) {
                System.out.println("Verwende Parameter: Netzwerk=" + netzwerkName + ", Umgebung=" + umgebungName);
                Logger.info("Verwende Parameter: Netzwerk=" + netzwerkName + ", Umgebung=" + umgebungName);
                
                // Überprüfen, ob die angegebene Netzwerk/Umgebung-Kombination gültig ist
                Map<String, List<String>> netzwerke = initializeNetworkEnvironments();
                if (!netzwerke.containsKey(netzwerkName)) {
                    System.out.println("Fehler: Ungültiges Netzwerk '" + netzwerkName + "'. Erlaubte Werte: " + netzwerke.keySet());
                    System.exit(1);
                }
                
                List<String> erlaubteUmgebungen = netzwerke.get(netzwerkName);
                if (!erlaubteUmgebungen.contains(umgebungName)) {
                    System.out.println("Fehler: Ungültige Umgebung '" + umgebungName + "' für Netzwerk '" + netzwerkName + 
                                       "'. Erlaubte Werte: " + erlaubteUmgebungen);
                    System.exit(1);
                }
            }
            // Wenn nur ein Parameter übergeben wurde, wird dieser als Konfigurationsdatei interpretiert
            else if (args.length == 1 && !args[0].contains("=")) {
                konfigDatei = args[0];
                System.out.println("Verwende angegebene Konfigurationsdatei: " + konfigDatei);
                Logger.info("Verwende angegebene Konfigurationsdatei: " + konfigDatei);
            }
            // Sonst interaktive Auswahl von Netzwerk und Umgebung
            else {
                Map<String, List<String>> netzwerke = initializeNetworkEnvironments();
                String[] selection = inputHandler.showNetworkAndEnvironmentMenu(netzwerke);
                netzwerkName = selection[0];
                umgebungName = selection[1];
            }
            
            // Konfigurationsdatei bestimmen, falls noch nicht gesetzt
            if (konfigDatei == null) {
                konfigDatei = "config_" + netzwerkName + "_" + umgebungName + ".properties";
                
                // Überprüfen, ob die gewählte Konfigurationsdatei existiert
                File konfigFile = new File(konfigDatei);
                if (!konfigFile.exists()) {
                    System.out.println("Konfigurationsdatei " + konfigDatei + " nicht gefunden!");
                    System.out.println("Verwende Standard-Konfigurationsdatei: config.properties");
                    konfigDatei = "config.properties";
                }
            }
            
            Logger.info("Migration gestartet mit Konfiguration: " + konfigDatei);
            
            // Konfiguration laden
            Konfiguration konfiguration = new Konfiguration(konfigDatei);
            Logger.info("Konfiguration erfolgreich geladen");
            
            // Datenbankname und Umgebung setzen (aus der Auswahl)
            String datenbankName = netzwerkName;
            String umgebung = umgebungName;
            
            // Zeitstempel für die Migration generieren
            LocalDateTime jetzt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
            String zeitstempel = jetzt.format(formatter);
            
            // Werte in der Konfiguration aktualisieren
            konfiguration.setDatenbankName(datenbankName);
            konfiguration.setUmgebung(umgebung);
            konfiguration.setZeitstempel(zeitstempel);
            
            Logger.info("Migration für Datenbank: " + datenbankName + ", Umgebung: " + umgebung + ", Zeitstempel: " + zeitstempel);
            inputHandler.close();
            
            // Datenbankverbindungen herstellen
            try (DatabaseManager dbManager = new DatabaseManager()) {
                Connection oracleConnection = dbManager.verbindeZuOracle(
                    konfiguration.getOracleUrl(), 
                    konfiguration.getOracleBenutzer(), 
                    konfiguration.getOraclePasswort()
                );
                
                // Migrator erstellen und Migration durchführen
                OracleToPgMigrator migrator = new OracleToPgMigrator(konfiguration, oracleConnection);
                migrator.migriere();
                
                Logger.info("Migration erfolgreich abgeschlossen");
                
            } catch (SQLException e) {
                Logger.fehler("Datenbankfehler während der Migration", e);
                System.exit(2);
            }
        } catch (IOException e) {
            System.err.println("Fehler beim Lesen der Konfigurationsdatei: " + e.getMessage());
            e.printStackTrace();
            System.exit(3);
        } catch (Exception e) {
            System.err.println("Unerwarteter Fehler: " + e.getMessage());
            e.printStackTrace();
            System.exit(4);
        } finally {
            // Logger schließen
            Logger.schliessen();
        }
    }
    
    /**
     * Initialisiert die Netzwerk- und Umgebungsstrukturen für das Auswahlmenü
     * 
     * @return Eine Map mit Netzwerknamen als Schlüssel und Listen von Umgebungen als Werte
     */
    private static Map<String, List<String>> initializeNetworkEnvironments() {
        Map<String, List<String>> netzwerke = new LinkedHashMap<>();
        
        // LVN Netzwerk mit Umgebungen (an erster Stelle)
        List<String> lvnUmgebungen = new ArrayList<>();
        lvnUmgebungen.add("entwicklung");
        lvnUmgebungen.add("qs");
        lvnUmgebungen.add("prod");
        netzwerke.put("lvn", lvnUmgebungen);
        
        // SB2 Netzwerk mit Umgebungen (an zweiter Stelle)
        List<String> sb2Umgebungen = new ArrayList<>();
        sb2Umgebungen.add("qs");
        sb2Umgebungen.add("prod");
        netzwerke.put("sb2", sb2Umgebungen);
        
        return netzwerke;
    }
}
