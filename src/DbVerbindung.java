import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Klasse zur Verwaltung von Datenbankverbindungen.
 */
public class DbVerbindung {
    private Connection oracleConnection;
    private Connection postgresConnection;
    
    /**
     * Stellt eine Verbindung zur Oracle-Datenbank her.
     * 
     * @param url JDBC-URL der Oracle-Datenbank
     * @param benutzer Benutzername für die Datenbank
     * @param passwort Passwort für die Datenbank
     * @return Die Datenbankverbindung
     * @throws SQLException Bei Verbindungsfehlern
     */
    public Connection verbindeZuOracle(String url, String benutzer, String passwort) throws SQLException {
        try {
            // Laden des Oracle-JDBC-Treibers
            Class.forName("oracle.jdbc.OracleDriver");
            Logger.info("Oracle JDBC-Treiber geladen");
        } catch (ClassNotFoundException e) {
            Logger.fehler("Oracle JDBC-Treiber konnte nicht geladen werden", e);
            throw new SQLException("Oracle JDBC-Treiber nicht gefunden", e);
        }
        
        Logger.info("Verbinde zu Oracle-Datenbank: " + url);
        oracleConnection = DriverManager.getConnection(url, benutzer, passwort);
        Logger.info("Verbindung zur Oracle-Datenbank hergestellt");
        
        return oracleConnection;
    }
    
    /**
     * Stellt eine Verbindung zur PostgreSQL-Datenbank her.
     * 
     * @param url JDBC-URL der PostgreSQL-Datenbank
     * @param benutzer Benutzername für die Datenbank
     * @param passwort Passwort für die Datenbank
     * @return Die Datenbankverbindung
     * @throws SQLException Bei Verbindungsfehlern
     */
    public Connection verbindeZuPostgres(String url, String benutzer, String passwort) throws SQLException {
        try {
            // Laden des PostgreSQL-JDBC-Treibers
            Class.forName("org.postgresql.Driver");
            Logger.info("PostgreSQL JDBC-Treiber geladen");
        } catch (ClassNotFoundException e) {
            Logger.fehler("PostgreSQL JDBC-Treiber konnte nicht geladen werden", e);
            throw new SQLException("PostgreSQL JDBC-Treiber nicht gefunden", e);
        }
        
        Logger.info("Verbinde zu PostgreSQL-Datenbank: " + url);
        postgresConnection = DriverManager.getConnection(url, benutzer, passwort);
        Logger.info("Verbindung zur PostgreSQL-Datenbank hergestellt");
        
        return postgresConnection;
    }
    
    /**
     * Schließt alle Datenbankverbindungen.
     */
    public void schliesseVerbindungen() {
        try {
            if (oracleConnection != null && !oracleConnection.isClosed()) {
                oracleConnection.close();
                Logger.info("Oracle-Datenbankverbindung geschlossen");
            }
        } catch (SQLException e) {
            Logger.warnung("Fehler beim Schließen der Oracle-Datenbankverbindung: " + e.getMessage());
        }
        
        try {
            if (postgresConnection != null && !postgresConnection.isClosed()) {
                postgresConnection.close();
                Logger.info("PostgreSQL-Datenbankverbindung geschlossen");
            }
        } catch (SQLException e) {
            Logger.warnung("Fehler beim Schließen der PostgreSQL-Datenbankverbindung: " + e.getMessage());
        }
    }
}
