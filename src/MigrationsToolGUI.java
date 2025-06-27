import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * Hauptklasse für das Oracle zu PostgreSQL Migrationstool mit GUI.
 */
public class MigrationsToolGUI {
    
    /**
     * Hauptmethode zum Starten der GUI-Anwendung.
     * 
     * @param args Kommandozeilenargumente (werden nicht verwendet)
     */
    public static void main(String[] args) {
        try {
            // Look and Feel des Betriebssystems verwenden
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        SwingUtilities.invokeLater(() -> {
            MigrationsGUI gui = new MigrationsGUI();
            gui.setVisible(true);
        });
    }
}
