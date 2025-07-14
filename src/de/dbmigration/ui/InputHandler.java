package de.dbmigration.ui;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

/**
 * Klasse zum Einlesen von Benutzereingaben über die Konsole
 */
public class InputHandler implements AutoCloseable {
    private final Scanner scanner;
    
    /**
     * Initialisiert einen neuen InputHandler mit dem System.in InputStream
     */
    public InputHandler() {
        this.scanner = new Scanner(System.in);
    }
    
    /**
     * Liest eine Eingabe mit einem bestimmten Prompt ein
     * 
     * @param prompt Die Eingabeaufforderung
     * @return Die eingegebene Zeichenkette
     */
    public String readInput(String prompt) {
        System.out.print(prompt + ": ");
        return scanner.nextLine().trim();
    }
    
    /**
     * Liest eine Eingabe mit einem bestimmten Prompt ein, mit Defaultwert
     * 
     * @param prompt Die Eingabeaufforderung
     * @param defaultValue Der Standardwert, der verwendet wird, wenn keine Eingabe erfolgt
     * @return Die eingegebene Zeichenkette oder der Standardwert
     */
    public String readInput(String prompt, String defaultValue) {
        System.out.print(prompt + " [" + defaultValue + "]: ");
        String input = scanner.nextLine().trim();
        return input.isEmpty() ? defaultValue : input;
    }
    
    /**
     * Zeigt ein Auswahlmenü an und liest die Auswahl des Benutzers ein
     * 
     * @param title Titel des Menüs
     * @param options Liste der Optionen
     * @return Index der ausgewählten Option (0-basiert)
     */
    public int showMenu(String title, List<String> options) {
        if (options == null || options.isEmpty()) {
            throw new IllegalArgumentException("Optionsliste darf nicht leer sein");
        }
        
        System.out.println("\n" + title + ":");
        for (int i = 0; i < options.size(); i++) {
            System.out.println((i + 1) + ". " + options.get(i));
        }
        
        int selection = -1;
        while (selection < 0 || selection >= options.size()) {
            try {
                System.out.print("\nBitte wählen Sie eine Option (1-" + options.size() + "): ");
                String input = scanner.nextLine().trim();
                selection = Integer.parseInt(input) - 1;
                
                if (selection < 0 || selection >= options.size()) {
                    System.out.println("Ungültige Auswahl. Bitte wählen Sie eine Nummer zwischen 1 und " + options.size());
                }
            } catch (NumberFormatException e) {
                System.out.println("Ungültige Eingabe. Bitte geben Sie eine Nummer ein.");
            }
        }
        
        return selection;
    }
    
    /**
     * Zeigt ein verschachteltes Menü mit Netzwerken und Umgebungen an
     * 
     * @param netzwerke Map mit Netzwerknamen als Schlüssel und Listen von Umgebungen als Werte
     * @return Ein Array mit [Netzwerkname, Umgebungsname]
     */
    public String[] showNetworkAndEnvironmentMenu(Map<String, List<String>> netzwerke) {
        if (netzwerke == null || netzwerke.isEmpty()) {
            throw new IllegalArgumentException("Netzwerke-Map darf nicht leer sein");
        }
        
        // Netzwerkmenü anzeigen
        List<String> netzwerkNamen = new ArrayList<>(netzwerke.keySet());
        int netzwerkIndex = showMenu("Netzwerk auswählen", netzwerkNamen);
        String netzwerkName = netzwerkNamen.get(netzwerkIndex);
        
        // Umgebungsmenü für das ausgewählte Netzwerk anzeigen
        List<String> umgebungen = netzwerke.get(netzwerkName);
        if (umgebungen == null || umgebungen.isEmpty()) {
            throw new IllegalStateException("Keine Umgebungen für das gewählte Netzwerk gefunden: " + netzwerkName);
        }
        
        int umgebungIndex = showMenu("Umgebung für " + netzwerkName + " auswählen", umgebungen);
        String umgebungName = umgebungen.get(umgebungIndex);
        
        return new String[] {netzwerkName, umgebungName};
    }
    
    /**
     * Schließt den Scanner
     */
    @Override
    public void close() {
        scanner.close();
    }
}
