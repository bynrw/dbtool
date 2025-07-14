package de.dbmigration.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Logger-Klasse zur Protokollierung von Fehlern und Fortschritten während der Datenbankmigration.
 */
public class Logger {
    private static PrintWriter logWriter;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String LOG_PREFIX_INFO = "[INFO] ";
    private static final String LOG_PREFIX_WARNING = "[WARNUNG] ";
    private static final String LOG_PREFIX_ERROR = "[FEHLER] ";
    
    private Logger() {
        // Private Konstruktor, um Instanziierung zu verhindern
    }
    
    /**
     * Initialisiert den Logger mit einer Logdatei.
     * 
     * @param logDateiPfad Pfad zur Logdatei
     * @throws IOException wenn die Logdatei nicht erstellt werden kann
     */
    public static void initialisieren(String logDateiPfad) throws IOException {
        logWriter = new PrintWriter(new FileWriter(logDateiPfad, true));
        info("Logger initialisiert");
    }
    
    /**
     * Schließt den Logger.
     */
    public static void schliessen() {
        if (logWriter != null) {
            info("Logger wird geschlossen");
            logWriter.flush();
            logWriter.close();
        }
    }
    
    /**
     * Protokolliert eine Informationsmeldung.
     * 
     * @param nachricht Die zu protokollierende Nachricht
     */
    public static void info(String nachricht) {
        log(LOG_PREFIX_INFO + nachricht);
        System.out.println(formatLogEntry(LOG_PREFIX_INFO, nachricht));
    }
    
    /**
     * Protokolliert eine Warnmeldung.
     * 
     * @param nachricht Die zu protokollierende Warnung
     */
    public static void warnung(String nachricht) {
        log(LOG_PREFIX_WARNING + nachricht);
        System.out.println(formatLogEntry(LOG_PREFIX_WARNING, nachricht));
    }
    
    /**
     * Protokolliert eine Fehlermeldung.
     * 
     * @param nachricht Die zu protokollierende Fehlermeldung
     */
    public static void fehler(String nachricht) {
        log(LOG_PREFIX_ERROR + nachricht);
        System.err.println(formatLogEntry(LOG_PREFIX_ERROR, nachricht));
    }
    
    /**
     * Protokolliert eine Fehlermeldung mit Ausnahme.
     * 
     * @param nachricht Die zu protokollierende Fehlermeldung
     * @param exception Die zu protokollierende Exception
     */
    public static void fehler(String nachricht, Throwable exception) {
        log(LOG_PREFIX_ERROR + nachricht + ": " + exception.getMessage());
        System.err.println(formatLogEntry(LOG_PREFIX_ERROR, nachricht + ": " + exception.getMessage()));
        
        if (logWriter != null) {
            exception.printStackTrace(logWriter);
            exception.printStackTrace(System.err);
        }
    }
    
    /**
     * Protokolliert eine Nachricht mit Zeitstempel.
     * 
     * @param nachricht Die zu protokollierende Nachricht
     */
    private static void log(String nachricht) {
        if (logWriter != null) {
            logWriter.println(formatLogEntry("", nachricht));
            logWriter.flush();
        }
    }
    
    /**
     * Formatiert einen Log-Eintrag mit aktuellem Zeitstempel.
     * 
     * @param prefix Präfix für den Logeintrag (INFO, WARNUNG, FEHLER)
     * @param nachricht Die zu formatierende Nachricht
     * @return Formatierter Log-Eintrag
     */
    private static String formatLogEntry(String prefix, String nachricht) {
        return String.format("[%s] %s%s", 
                LocalDateTime.now().format(DATE_FORMATTER),
                prefix,
                nachricht);
    }
}
