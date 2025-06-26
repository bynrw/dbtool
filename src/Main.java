import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Hauptklasse zur Ausführung der Oracle-zu-PostgreSQL-Migration.
 */
public class Main {
    /**
     * Einstiegspunkt für das Migrationstool.
     * 
     * @param args Kommandozeilenargumente (erster Parameter sollte der Pfad zur Konfigurationsdatei sein)
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Verwendung: java Main <Pfad zur Konfigurationsdatei>");
            System.exit(1);
        }
        
        String konfigDatei = args[0];
        
        // Ausgabeordner für Logdateien sicherstellen
        File logDir = new File("logs");
        if (!logDir.exists()) {
            logDir.mkdir();
        }
        
        try {
            // Logger initialisieren
            String logDateiName = "logs/migration_" + System.currentTimeMillis() + ".log";
            Logger.initialisieren(logDateiName);
            
            Logger.info("Migration gestartet mit Konfiguration: " + konfigDatei);
            
            // Konfiguration laden
            Konfiguration konfiguration = new Konfiguration(konfigDatei);
            Logger.info("Konfiguration erfolgreich geladen");
            
            // Datenbankverbindungen herstellen
            DbVerbindung dbVerbindung = new DbVerbindung();
            Connection oracleConnection = null;
            
            try {
                oracleConnection = dbVerbindung.verbindeZuOracle(
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
            } finally {
                // Datenbankverbindungen schließen
                dbVerbindung.schliesseVerbindungen();
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
}
