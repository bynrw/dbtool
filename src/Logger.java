import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Stack;

/**
 * Logger-Klasse zur Protokollierung von Fehlern und Fortschritten während der Datenbankmigration.
 * Verbesserte Version mit Kontextinformationen und Thread-Informationen.
 */
public class Logger {
    private static PrintWriter logWriter;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static LogHandler guiHandler;
    private static boolean debugMode = false;
    private static final ThreadLocal<Stack<String>> contextStack = new ThreadLocal<Stack<String>>() {
        @Override
        protected Stack<String> initialValue() {
            return new Stack<>();
        }
    };
    
    /**
     * Interface für den LogHandler
     */
    public interface LogHandler {
        void log(String message);
    }
    
    /**
     * Setzt einen LogHandler für GUI-Ausgaben
     * 
     * @param handler Der LogHandler für die GUI
     */
    public static void setLogHandler(LogHandler handler) {
        guiHandler = handler;
    }
    
    /**
     * Aktiviert oder deaktiviert den Debug-Modus
     * 
     * @param enable true um Debug-Modus zu aktivieren, false zum Deaktivieren
     */
    public static void setDebugMode(boolean enable) {
        debugMode = enable;
        info("Debug-Modus " + (enable ? "aktiviert" : "deaktiviert"));
    }
    
    /**
     * Fügt einen Kontext zum Logger hinzu. Nützlich um den Ausführungskontext zu verfolgen.
     * 
     * @param context Der hinzuzufügende Kontext
     */
    public static void pushContext(String context) {
        contextStack.get().push(context);
    }
    
    /**
     * Entfernt den letzten hinzugefügten Kontext
     */
    public static void popContext() {
        Stack<String> stack = contextStack.get();
        if (!stack.isEmpty()) {
            stack.pop();
        }
    }
    
    /**
     * Initialisiert den Logger mit einer Logdatei.
     * 
     * @param logDatei Pfad zur Logdatei
     * @throws IOException wenn die Logdatei nicht erstellt werden kann
     */
    public static void initialisieren(String logDatei) throws IOException {
        logWriter = new PrintWriter(new FileWriter(logDatei, true));
        info("Logger initialisiert - Log-Datei: " + logDatei);
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
     * Protokolliert eine Debug-Meldung. Wird nur ausgegeben, wenn Debug-Mode aktiv ist.
     * 
     * @param nachricht Die zu protokollierende Debug-Nachricht
     */
    public static void debug(String nachricht) {
        if (debugMode) {
            logSchreiben("DEBUG", nachricht);
        }
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
        if (logWriter != null) {
            e.printStackTrace(logWriter);
            logWriter.flush();
        } else {
            e.printStackTrace();
        }
    }
    
    /**
     * Schreibt eine formatierte Nachricht in die Logdatei.
     * 
     * @param level Das Log-Level (INFO, WARNUNG, FEHLER)
     * @param nachricht Die zu protokollierende Nachricht
     */
    private static void logSchreiben(String level, String nachricht) {
        String timestamp = dateFormat.format(new Date());
        String threadName = Thread.currentThread().getName();
        String context = "";
        
        // Kontext hinzufügen, falls vorhanden
        Stack<String> stack = contextStack.get();
        if (!stack.isEmpty()) {
            context = "[" + String.join(" > ", stack) + "] ";
        }
        
        String logEintrag = String.format("[%s] [%s] [%s] %s%s", timestamp, level, threadName, context, nachricht);
        System.out.println(logEintrag);
        
        if (logWriter != null) {
            logWriter.println(logEintrag);
            logWriter.flush();
        }
        
        if (guiHandler != null) {
            guiHandler.log(logEintrag);
        }
    }
}
