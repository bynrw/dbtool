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
            System.err.println("❌ Keine Migration-Ordner in 'migriete_daten' gefunden!");
            scanner.close();
            return;
        }
        
        // Zeige alle gefundenen Ordner
        System.out.println("📁 Migration-Ordner:");
        for (int i = 0; i < migrationOrdner.size(); i++) {
            System.out.println((i + 1) + " = " + migrationOrdner.get(i));
        }
        
        System.out.print("Wähle Ordner (1-" + migrationOrdner.size() + "): ");
        int choice = Integer.parseInt(scanner.nextLine()) - 1;
        
        if (choice < 0 || choice >= migrationOrdner.size()) {
            System.err.println("❌ Ungültige Auswahl!");
            scanner.close();
            return;
        }
        
        String gewaehlterOrdner = migrationOrdner.get(choice);
        
        // Zeige verfügbare Unterordner
        List<String> verfuegbareOrdner = findeVerfuegbareUnterordner(gewaehlterOrdner);
        
        if (verfuegbareOrdner.isEmpty()) {
            System.err.println("❌ Keine SQL-Ordner gefunden!");
            scanner.close();
            return;
        }
        
        System.out.println("\n📂 Verfügbare SQL-Ordner:");
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
                System.err.println("❌ Ungültige Auswahl!");
                scanner.close();
                return;
            }
        }
        
        try {
            importSQLFiles(gewaehlterOrdner, ausgewaehlteOrdner);
            System.out.println("🎉 Import erfolgreich abgeschlossen!");
        } catch (Exception e) {
            System.err.println("❌ Fehler beim Import: " + e.getMessage());
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
        
        System.out.println("🔍 Suche migriete_daten Ordner...");
        
        for (String pfad : suchPfade) {
            File migrieteOrdner = new File(pfad);
            System.out.println("   Prüfe: " + migrieteOrdner.getAbsolutePath());
            
            if (migrieteOrdner.exists() && migrieteOrdner.isDirectory()) {
                System.out.println("   ✅ Gefunden!");
                
                File[] unterordner = migrieteOrdner.listFiles(File::isDirectory);
                if (unterordner != null) {
                    for (File ordner1 : unterordner) {
                        String vollPfad = pfad + File.separator + ordner1.getName();
                        ordner.add(vollPfad);
                        System.out.println("      → " + vollPfad);
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
            System.err.println("❌ PostgreSQL JDBC Driver nicht gefunden!");
            throw new SQLException("Driver nicht gefunden", e);
        }
        
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        conn.setAutoCommit(true);
        
        System.out.println("✅ Datenbankverbindung hergestellt");
        
        int gesamteDateien = 0;
        
        for (String ordner : ordnerZuImportieren) {
            if (ordner == null) continue;
            
            String ordnerPfad = basePath + File.separator + ordner;
            File ordnerFile = new File(ordnerPfad);
            
            if (!ordnerFile.exists()) {
                System.out.println("⚠️  Ordner '" + ordner + "' nicht gefunden - überspringe");
                continue;
            }
            
            System.out.println("\n📁 Verarbeite: " + ordner);
            
            File[] sqlDateien = ordnerFile.listFiles((dir, name) -> name.toLowerCase().endsWith(".sql"));
            
            if (sqlDateien != null && sqlDateien.length > 0) {
                Arrays.sort(sqlDateien);
                
                for (File sqlDatei : sqlDateien) {
                    System.out.println("   📄 " + sqlDatei.getName());
                    fuehreSQLDateiAus(conn, sqlDatei);
                    gesamteDateien++;
                }
            } else {
                System.out.println("   ⚠️ Keine SQL-Dateien gefunden");
            }
        }
        
        conn.close();
        System.out.println("\n🏁 " + gesamteDateien + " Dateien importiert");
    }
    
    private static void fuehreSQLDateiAus(Connection conn, File sqlDatei) throws IOException, SQLException {
        String inhalt = Files.readString(sqlDatei.toPath());
        String[] statements = inhalt.split(";");
        
        Statement stmt = conn.createStatement();
        
        for (String statement : statements) {
            String sql = statement.trim();
            
            if (!sql.isEmpty() && !sql.startsWith("--")) {
                try {
                    stmt.execute(sql);
                    System.out.println("      ✅ OK");
                } catch (SQLException e) {
                    System.err.println("      ❌ Fehler: " + e.getMessage());
                }
            }
        }
        
        stmt.close();
    }
}