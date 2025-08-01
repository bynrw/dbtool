package de.dbmigration;
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.List;

public class SQLImporter {
    
    // PostgreSQL Datenbank-Konfiguration - HIER DEINE DATEN EINTRAGEN!
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";  // <- Dein Datenbankname
    private static final String DB_USER = "postgres";                                  // <- Dein PostgreSQL User
    private static final String DB_PASSWORD = "123456";                         // <- Dein Passwort
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        // Automatisch migriete_daten durchsuchen
        List<String> migrationOrdner = findeAlleMigrationOrdner();
        
        if (migrationOrdner.isEmpty()) {
            System.err.println("FEHLER: Keine Migration-Ordner in 'migriete_daten' gefunden!");
            scanner.close();
            return;
        }
        
        // Zeige alle gefundenen Ordner
        System.out.println("Verfügbare Migration-Ordner:");
        for (int i = 0; i < migrationOrdner.size(); i++) {
            System.out.println((i + 1) + " = " + migrationOrdner.get(i));
        }
        
        System.out.print("Wähle Ordner (1-" + migrationOrdner.size() + "): ");
        int choice = Integer.parseInt(scanner.nextLine()) - 1;
        
        if (choice < 0 || choice >= migrationOrdner.size()) {
            System.err.println("FEHLER: Ungültige Auswahl!");
            scanner.close();
            return;
        }
        
        String gewaehlterOrdner = migrationOrdner.get(choice);
        
        // Zeige verfügbare Unterordner
        List<String> verfuegbareOrdner = findeVerfuegbareUnterordner(gewaehlterOrdner);
        
        if (verfuegbareOrdner.isEmpty()) {
            System.err.println("FEHLER: Keine SQL-Ordner gefunden!");
            scanner.close();
            return;
        }
        
        System.out.println("\nVerfügbare SQL-Ordner:");
        for (int i = 0; i < verfuegbareOrdner.size(); i++) {
            System.out.println((i + 1) + " = " + verfuegbareOrdner.get(i));
        }
        System.out.println((verfuegbareOrdner.size() + 1) + " = Alle Ordner");
        
        System.out.print("Wähle (1-" + (verfuegbareOrdner.size() + 1) + " oder mehrere mit Komma): ");
        String input = scanner.nextLine();
        
        String[] ausgewaehlteOrdner;
        
        if (input.contains(",")) {
            // Mehrere Ordner ausgewählt
            String[] indices = input.split(",");
            ausgewaehlteOrdner = new String[indices.length];
            for (int i = 0; i < indices.length; i++) {
                int idx = Integer.parseInt(indices[i].trim()) - 1;
                if (idx >= 0 && idx < verfuegbareOrdner.size()) {
                    ausgewaehlteOrdner[i] = verfuegbareOrdner.get(idx);
                }
            }
        } else {
            int idx = Integer.parseInt(input) - 1;
            if (idx == verfuegbareOrdner.size()) {
                // Alle Ordner
                ausgewaehlteOrdner = verfuegbareOrdner.toArray(new String[0]);
            } else if (idx >= 0 && idx < verfuegbareOrdner.size()) {
                // Einzelner Ordner
                ausgewaehlteOrdner = new String[]{verfuegbareOrdner.get(idx)};
            } else {
                System.err.println("FEHLER: Ungültige Auswahl!");
                scanner.close();
                return;
            }
        }
        
        try {
            importSQLFiles(gewaehlterOrdner, ausgewaehlteOrdner);
            System.out.println("INFO: Import erfolgreich abgeschlossen!");
        } catch (Exception e) {
            System.err.println("FEHLER beim Import: " + e.getMessage());
        }
        
        scanner.close();
    }
    
    private static List<String> findeAlleMigrationOrdner() {
        List<String> ordner = new ArrayList<>();
        
        // Suche in verschiedenen möglichen Pfaden
        String[] suchPfade = {
            "migriete_daten",
            "dbtool-main/migriete_daten", 
            "dbtool-main\\migriete_daten",
            "./migriete_daten",
            "../migriete_daten"
        };
        
        System.out.println("INFO: Suche migriete_daten Ordner...");
        
        for (String pfad : suchPfade) {
            File migrieteOrdner = new File(pfad);
            System.out.println("   Prüfe Verzeichnis: " + migrieteOrdner.getAbsolutePath());
            
            if (migrieteOrdner.exists() && migrieteOrdner.isDirectory()) {
                System.out.println("   INFO: Verzeichnis gefunden!");
                
                File[] unterordner = migrieteOrdner.listFiles(File::isDirectory);
                if (unterordner != null) {
                    for (File ordner1 : unterordner) {
                        String vollPfad = pfad + File.separator + ordner1.getName();
                        ordner.add(vollPfad);
                        System.out.println("      -> " + vollPfad);
                    }
                }
                break; // Ersten gefundenen Pfad verwenden
            }
        }
        
        return ordner;
    }
    
    private static List<String> findeVerfuegbareUnterordner(String basePath) {
        List<String> ordner = new ArrayList<>();
        File baseDir = new File(basePath);
        
        if (!baseDir.exists()) {
            return ordner;
        }
        
        File[] unterordner = baseDir.listFiles(File::isDirectory);
        if (unterordner != null) {
            for (File ordner1 : unterordner) {
                ordner.add(ordner1.getName());
            }
            // Sortiere alphabetisch
            Collections.sort(ordner);
        }
        
        return ordner;
    }
    
    public static void importSQLFiles(String basePath, String[] ordnerZuImportieren) throws SQLException, IOException {
        // PostgreSQL Driver laden
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("FEHLER: PostgreSQL JDBC Driver nicht gefunden!");
            throw new SQLException("Driver nicht gefunden", e);
        }
        
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        conn.setAutoCommit(true);
        
        System.out.println("INFO: Datenbankverbindung erfolgreich hergestellt");
        
        int gesamteDateien = 0;
        int erfolgreicheDateien = 0;
        List<String> fehlgeschlageneTabellen = new ArrayList<>();
        
        for (String ordner : ordnerZuImportieren) {
            if (ordner == null) continue;
            
            String ordnerPfad = basePath + File.separator + ordner;
            File ordnerFile = new File(ordnerPfad);
            
            if (!ordnerFile.exists()) {
                System.out.println("WARNUNG: Ordner '" + ordner + "' nicht gefunden - überspringe");
                continue;
            }
            
            System.out.println("\nINFO: Verarbeite Ordner: " + ordner);
            
            File[] sqlDateien = ordnerFile.listFiles((dir, name) -> name.toLowerCase().endsWith(".sql"));
            
            if (sqlDateien != null && sqlDateien.length > 0) {
                Arrays.sort(sqlDateien);
                
                for (File sqlDatei : sqlDateien) {
                    System.out.println("   INFO: Führe SQL-Datei aus: " + sqlDatei.getName());
                    boolean dateiErfolgreich = fuehreSQLDateiAus(conn, sqlDatei);
                    gesamteDateien++;
                    
                    if (dateiErfolgreich) {
                        erfolgreicheDateien++;
                    } else {
                        // Extrahiere Tabellenname aus Dateiname (entferne .sql)
                        String tabellenName = sqlDatei.getName().replaceAll("\\.sql$", "");
                        fehlgeschlageneTabellen.add(tabellenName);
                    }
                }
            } else {
                System.out.println("   WARNUNG: Keine SQL-Dateien gefunden");
            }
        }
        
        conn.close();
        
        // Detaillierte Statistik ausgeben
        System.out.println("\n" + "=".repeat(60));
        System.out.println("MIGRATION ABGESCHLOSSEN - ZUSAMMENFASSUNG");
        System.out.println("=".repeat(60));
        System.out.println("Gesamt verarbeitete Dateien: " + gesamteDateien);
        System.out.println("Erfolgreich migriert: " + erfolgreicheDateien);
        System.out.println("Fehlgeschlagen: " + fehlgeschlageneTabellen.size());
        
        if (!fehlgeschlageneTabellen.isEmpty()) {
            System.out.println("\nFEHLGESCHLAGENE TABELLEN/DATEIEN:");
            for (String tabelle : fehlgeschlageneTabellen) {
                System.out.println("  - " + tabelle);
            }
        }
        
        if (fehlgeschlageneTabellen.isEmpty()) {
            System.out.println("\nSTATUS: Alle Dateien erfolgreich migriert!");
        } else {
            System.err.println("\nSTATUS: Migration mit Fehlern abgeschlossen!");
        }
        System.out.println("=".repeat(60));
    }
    
    private static boolean fuehreSQLDateiAus(Connection conn, File sqlDatei) throws IOException, SQLException {
        String inhalt = Files.readString(sqlDatei.toPath());

        // Bereinige den Inhalt - entferne nur echte Kommentarzeilen, nicht COMMENT ON statements
        String bereinigterInhalt = bereinigeKommentare(inhalt);

        // Statement aufteilen
        String[] statements = bereinigterInhalt.split("(?<!\\\\);"); // nur echtes Semikolon (nicht escaped)

        boolean alleStatementsErfolgreich = true;
        int erfolgreicheStatements = 0;
        int fehlgeschlageneStatements = 0;

        try (Statement stmt = conn.createStatement()) {
            for (String statement : statements) {
                String sql = statement.trim();
                if (!sql.isEmpty()) {
                    try {
                        stmt.execute(sql);
                        erfolgreicheStatements++;
                        System.out.println("      INFO: Statement erfolgreich ausgeführt");
                    } catch (SQLException e) {
                        fehlgeschlageneStatements++;
                        alleStatementsErfolgreich = false;
                        System.err.println("      FEHLER: " + e.getMessage());
                        System.err.println("      SQL: " + sql.substring(0, Math.min(sql.length(), 100)) + "...");
                        // Optional: throw e; // Um bei Fehler komplett abzubrechen
                    }
                }
            }
        }
        
        // Zusammenfassung für die Datei ausgeben
        if (fehlgeschlageneStatements > 0) {
            System.err.println("   DATEI-ZUSAMMENFASSUNG: " + erfolgreicheStatements + " erfolgreich, " + 
                             fehlgeschlageneStatements + " fehlgeschlagen");
        } else {
            System.out.println("   DATEI-ZUSAMMENFASSUNG: Alle " + erfolgreicheStatements + " Statements erfolgreich");
        }
        
        return alleStatementsErfolgreich;
    }
    
    /**
     * Bereinigt SQL-Kommentare, aber behält COMMENT ON statements bei
     */
    private static String bereinigeKommentare(String sql) {
        StringBuilder result = new StringBuilder();
        String[] lines = sql.split("\n");
        boolean inBlockComment = false;
        
        for (String line : lines) {
            String trimmedLine = line.trim();
            
            // Prüfe ob es ein COMMENT ON statement ist - diese behalten wir
            if (trimmedLine.toUpperCase().startsWith("COMMENT ON")) {
                result.append(line).append("\n");
                continue;
            }
            
            // Block-Kommentare (/* ... */)
            if (trimmedLine.contains("/*")) {
                if (trimmedLine.contains("*/")) {
                    // Ein-Zeilen Block-Kommentar - entfernen
                    String beforeComment = line.substring(0, line.indexOf("/*"));
                    String afterComment = "";
                    int endIndex = line.indexOf("*/") + 2;
                    if (endIndex < line.length()) {
                        afterComment = line.substring(endIndex);
                    }
                    String cleanedLine = beforeComment + afterComment;
                    if (!cleanedLine.trim().isEmpty()) {
                        result.append(cleanedLine).append("\n");
                    }
                } else {
                    // Beginn eines mehrzeiligen Block-Kommentars
                    inBlockComment = true;
                    String beforeComment = line.substring(0, line.indexOf("/*"));
                    if (!beforeComment.trim().isEmpty()) {
                        result.append(beforeComment).append("\n");
                    }
                }
                continue;
            }
            
            if (inBlockComment) {
                if (trimmedLine.contains("*/")) {
                    // Ende des Block-Kommentars
                    inBlockComment = false;
                    int endIndex = line.indexOf("*/") + 2;
                    if (endIndex < line.length()) {
                        String afterComment = line.substring(endIndex);
                        if (!afterComment.trim().isEmpty()) {
                            result.append(afterComment).append("\n");
                        }
                    }
                }
                continue;
            }
            
            // Zeilen-Kommentare (--) - aber nur wenn sie am Anfang der Zeile stehen
            if (trimmedLine.startsWith("--")) {
                continue; // Überspringe diese Zeile
            }
            
            // Entferne Inline-Kommentare (-- am Ende der Zeile)
            if (line.contains("--")) {
                int commentIndex = line.indexOf("--");
                // Prüfe ob das -- in einem String steht (einfache Prüfung)
                String beforeComment = line.substring(0, commentIndex);
                long singleQuotes = beforeComment.chars().filter(ch -> ch == '\'').count();
                long doubleQuotes = beforeComment.chars().filter(ch -> ch == '"').count();
                
                // Wenn ungerade Anzahl von Anführungszeichen, dann ist -- in einem String
                if (singleQuotes % 2 == 0 && doubleQuotes % 2 == 0) {
                    line = beforeComment.trim();
                }
            }
            
            if (!line.trim().isEmpty()) {
                result.append(line).append("\n");
            }
        }
        
        return result.toString();
    }

}
