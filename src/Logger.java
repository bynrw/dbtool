import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Logger-Klasse zur Protokollierung von Fehlern und Fortschritten während der Datenbankmigration.
 */
public class Logger {
    private static PrintWriter logWriter;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /**
     * Initialisiert den Logger mit einer Logdatei.
     * 
     * @param logDatei Pfad zur Logdatei
     * @throws IOException wenn die Logdatei nicht erstellt werden kann
     */
    public static void initialisieren(String logDatei) throws IOException {
        logWriter = new PrintWriter(new FileWriter(logDatei, true));
        info("Logger initialisiert");
    }
    
    /**
     * Schließt den Logger.
     */
    public static void schliessen() {
        if (logWriter != null) {
            info("Logger wird geschlossen");
            logWriter.close();
        }
    }
    
    /**
     * Protokolliert eine Informationsmeldung.
     * 
     * @param nachricht Die zu protokollierende Nachricht
     */
    public static void info(String nachricht) {
        logSchreiben("INFO", nachricht);
    }
    
    /**
     * Protokolliert eine Warnmeldung.
     * 
     * @param nachricht Die zu protokollierende Warnmeldung
     */
    public static void warnung(String nachricht) {
        logSchreiben("WARNUNG", nachricht);
    }
    
    /**
     * Protokolliert eine Fehlermeldung.
     * 
     * @param nachricht Die zu protokollierende Fehlermeldung
     */
    public static void fehler(String nachricht) {
        logSchreiben("FEHLER", nachricht);
    }
    
    /**
     * Protokolliert eine Fehlermeldung mit Ausnahmedetails.
     * 
     * @param nachricht Die zu protokollierende Fehlermeldung
     * @param e Die aufgetretene Exception
     */
    public static void fehler(String nachricht, Exception e) {
        fehler(nachricht + ": " + e.getMessage());
        e.printStackTrace(logWriter);
        logWriter.flush();
    }
    
    /**
     * Schreibt eine formatierte Nachricht in die Logdatei.
     * 
     * @param level Das Log-Level (INFO, WARNUNG, FEHLER)
     * @param nachricht Die zu protokollierende Nachricht
     */
    private static void logSchreiben(String level, String nachricht) {
        String timestamp = dateFormat.format(new Date());
        String logEintrag = String.format("[%s] [%s] %s", timestamp, level, nachricht);
        System.out.println(logEintrag);
        
        if (logWriter != null) {
            logWriter.println(logEintrag);
            logWriter.flush();
        }
    }
}
