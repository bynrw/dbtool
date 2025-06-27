import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.List;

/**
 * Grafische Benutzeroberfläche für das Oracle zu PostgreSQL Migrationstool.
 */
public class MigrationsGUI extends JFrame {
    private JTextField oracleUrlField;
    private JTextField oracleUserField;
    private JPasswordField oraclePasswordField;
    private JTextField postgresUrlField;
    private JTextField postgresUserField;
    private JPasswordField postgresPasswordField;
    private JTextField outputPathField;
    private JTextArea whitelistTextArea;
    private JTextArea blacklistTextArea;
    private DefaultTableModel ignoredColumnsModel;
    private JTable ignoredColumnsTable;
    private JButton addIgnoredColumnButton;
    private JButton removeIgnoredColumnButton;
    private DefaultTableModel datatypeMappingsModel;
    private JButton addDatatypeMappingButton;
    private JButton removeDatatypeMappingButton;
    private DefaultTableModel transformationsModel;
    private JTable transformationsTable;
    private JButton addTransformationButton;
    private JButton removeTransformationButton;
    private JTextArea booleanPrefixesArea;
    private JTextArea booleanSuffixesArea;
    private JTextArea booleanNamesArea;
    private JButton saveConfigButton;
    private JButton loadConfigButton;
    private JButton startMigrationButton;
    private JTextArea logArea;
    
    // Private Attribute für den PK/FK-Tab
    private DefaultTableModel primärschlüsselModel;
    private JTable primärschlüsselTable;
    private DefaultTableModel fremdschlüsselModel;
    private JTable fremdschlüsselTable;
    private JButton anzeigenPkFkButton;
    private JComboBox<String> tabellenAuswahlComboBox;

    // Fortschrittsanzeige für Migration
    private JProgressBar progressBar;
    private JLabel progressLabel;

    /**
     * Konstruktor zum Erstellen der GUI.
     */
    public MigrationsGUI() {
        setTitle("Oracle zu PostgreSQL Migrationstool");
        setSize(800, 700);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        // Hauptpanel mit Tabs erstellen
        JTabbedPane tabbedPane = new JTabbedPane();
        
        // Tab für Datenbankverbindungen
        tabbedPane.addTab("Datenbankverbindungen", createConnectionPanel());
        
        // Tab für Tabellenkonfiguration
        tabbedPane.addTab("Tabellen", createTablesPanel());
        
        // Tab für Datentypzuordnungen
        tabbedPane.addTab("Datentypen", createDatatypesPanel());
        
        // Tab für Transformationen
        tabbedPane.addTab("Transformationen", createTransformationsPanel());
        
        // Tab für Boolean-Erkennung
        tabbedPane.addTab("Boolean-Erkennung", createBooleanDetectionPanel());
        
        // Tab für Logs und Ausführung
        tabbedPane.addTab("Migration", createMigrationPanel());
        
        // Tab für PK/FK-Informationen
        tabbedPane.addTab("PK/FK-Informationen", createPkFkPanel());
        
        // Buttons am unteren Rand
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        
        loadConfigButton = new JButton("Konfiguration laden");
        loadConfigButton.addActionListener(e -> loadConfiguration());
        buttonPanel.add(loadConfigButton);
        
        saveConfigButton = new JButton("Konfiguration speichern");
        saveConfigButton.addActionListener(e -> saveConfiguration());
        buttonPanel.add(saveConfigButton);
        
        startMigrationButton = new JButton("Migration starten");
        startMigrationButton.addActionListener(e -> startMigration());
        buttonPanel.add(startMigrationButton);
        
        // Fortschrittsanzeige vorbereiten
        progressBar = new JProgressBar(0, 100);
        progressBar.setStringPainted(true);
        progressLabel = new JLabel("Bereit");
        
        JPanel progressPanel = new JPanel(new BorderLayout(5, 0));
        progressPanel.add(progressLabel, BorderLayout.WEST);
        progressPanel.add(progressBar, BorderLayout.CENTER);
        progressPanel.setBorder(new EmptyBorder(5, 10, 5, 10));
        
        // Hauptlayout
        setLayout(new BorderLayout());
        add(tabbedPane, BorderLayout.CENTER);
        
        // Panel für Buttons und Fortschrittsanzeige
        JPanel southPanel = new JPanel(new BorderLayout());
        southPanel.add(progressPanel, BorderLayout.NORTH);
        southPanel.add(buttonPanel, BorderLayout.SOUTH);
        add(southPanel, BorderLayout.SOUTH);
    }
    
    /**
     * Erstellt das Panel für die Datenbankverbindungen.
     */
    private JPanel createConnectionPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        JPanel formPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5);
        
        // Oracle-Verbindung
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2;
        formPanel.add(new JLabel("<html><b>Oracle-Datenbankverbindung</b></html>"), gbc);
        
        gbc.gridwidth = 1;
        gbc.gridx = 0;
        gbc.gridy = 1;
        formPanel.add(new JLabel("URL:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        oracleUrlField = new JTextField("jdbc:oracle:thin:@//localhost:1521/ORCLPDB");
        formPanel.add(oracleUrlField, gbc);
        
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.weightx = 0.0;
        formPanel.add(new JLabel("Benutzer:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        oracleUserField = new JTextField("system");
        formPanel.add(oracleUserField, gbc);
        
        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.weightx = 0.0;
        formPanel.add(new JLabel("Passwort:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        oraclePasswordField = new JPasswordField("oracle");
        formPanel.add(oraclePasswordField, gbc);
        
        // PostgreSQL-Verbindung
        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(20, 5, 5, 5);
        formPanel.add(new JLabel("<html><b>PostgreSQL-Datenbankverbindung</b></html>"), gbc);
        gbc.insets = new Insets(5, 5, 5, 5);
        
        gbc.gridwidth = 1;
        gbc.gridx = 0;
        gbc.gridy = 5;
        formPanel.add(new JLabel("URL:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        postgresUrlField = new JTextField("jdbc:postgresql://localhost:5432/postgres");
        formPanel.add(postgresUrlField, gbc);
        
        gbc.gridx = 0;
        gbc.gridy = 6;
        gbc.weightx = 0.0;
        formPanel.add(new JLabel("Benutzer:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        postgresUserField = new JTextField("postgres");
        formPanel.add(postgresUserField, gbc);
        
        gbc.gridx = 0;
        gbc.gridy = 7;
        gbc.weightx = 0.0;
        formPanel.add(new JLabel("Passwort:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        postgresPasswordField = new JPasswordField("postgres");
        formPanel.add(postgresPasswordField, gbc);
        
        // Ausgabepfad
        gbc.gridx = 0;
        gbc.gridy = 8;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(20, 5, 5, 5);
        formPanel.add(new JLabel("<html><b>Ausgabe-Konfiguration</b></html>"), gbc);
        gbc.insets = new Insets(5, 5, 5, 5);
        
        gbc.gridwidth = 1;
        gbc.gridx = 0;
        gbc.gridy = 9;
        formPanel.add(new JLabel("Pfad:"), gbc);
        
        gbc.gridx = 1;
        gbc.weightx = 1.0;
        outputPathField = new JTextField("./migrierte_daten");
        formPanel.add(outputPathField, gbc);
        
        gbc.gridx = 2;
        gbc.weightx = 0.0;
        JButton browseButton = new JButton("...");
        browseButton.addActionListener(e -> {
            JFileChooser chooser = new JFileChooser();
            chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            int result = chooser.showOpenDialog(MigrationsGUI.this);
            if (result == JFileChooser.APPROVE_OPTION) {
                outputPathField.setText(chooser.getSelectedFile().getAbsolutePath());
            }
        });
        formPanel.add(browseButton, gbc);
        
        panel.add(formPanel, BorderLayout.NORTH);
        
        // Leerer Platz für flexibles Layout
        panel.add(new JPanel(), BorderLayout.CENTER);
        
        return panel;
    }
    
    /**
     * Erstellt das Panel für die Tabellenkonfiguration.
     */
    private JPanel createTablesPanel() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        // Oberes Panel für Whitelist und Blacklist
        JPanel topPanel = new JPanel(new GridLayout(1, 2, 10, 0));
        
        // Whitelist
        JPanel whitelistPanel = new JPanel(new BorderLayout());
        whitelistPanel.add(new JLabel("Tabellen für Migration (Whitelist):"), BorderLayout.NORTH);
        whitelistTextArea = new JTextArea();
        whitelistTextArea.setLineWrap(true);
        whitelistTextArea.addCaretListener(e -> updateTableList());
        JScrollPane whitelistScroll = new JScrollPane(whitelistTextArea);
        whitelistPanel.add(whitelistScroll, BorderLayout.CENTER);
        topPanel.add(whitelistPanel);
        
        // Blacklist
        JPanel blacklistPanel = new JPanel(new BorderLayout());
        blacklistPanel.add(new JLabel("Tabellen ausschließen (Blacklist):"), BorderLayout.NORTH);
        blacklistTextArea = new JTextArea();
        blacklistTextArea.setLineWrap(true);
        JScrollPane blacklistScroll = new JScrollPane(blacklistTextArea);
        blacklistPanel.add(blacklistScroll, BorderLayout.CENTER);
        topPanel.add(blacklistPanel);
        
        // Unteres Panel für ignorierte Spalten
        JPanel bottomPanel = new JPanel(new BorderLayout());
        bottomPanel.add(new JLabel("Ignorierte Spalten pro Tabelle:"), BorderLayout.NORTH);
        
        ignoredColumnsModel = new DefaultTableModel() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return true;
            }
        };
        ignoredColumnsModel.addColumn("Tabelle");
        ignoredColumnsModel.addColumn("Ignorierte Spalten (durch Kommas getrennt)");
        
        ignoredColumnsTable = new JTable(ignoredColumnsModel);
        ignoredColumnsTable.getColumnModel().getColumn(0).setPreferredWidth(150);
        ignoredColumnsTable.getColumnModel().getColumn(1).setPreferredWidth(450);
        
        JScrollPane tableScroll = new JScrollPane(ignoredColumnsTable);
        bottomPanel.add(tableScroll, BorderLayout.CENTER);
        
        // Buttons für die Tabelle
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        addIgnoredColumnButton = new JButton("Hinzufügen");
        addIgnoredColumnButton.addActionListener(e -> {
            ignoredColumnsModel.addRow(new Object[]{"", ""});
        });
        buttonPanel.add(addIgnoredColumnButton);
        
        removeIgnoredColumnButton = new JButton("Entfernen");
        removeIgnoredColumnButton.addActionListener(e -> {
            int selectedRow = ignoredColumnsTable.getSelectedRow();
            if (selectedRow != -1) {
                ignoredColumnsModel.removeRow(selectedRow);
            }
        });
        buttonPanel.add(removeIgnoredColumnButton);
        
        bottomPanel.add(buttonPanel, BorderLayout.SOUTH);
        
        // Hauptlayout
        panel.add(topPanel, BorderLayout.NORTH);
        panel.add(bottomPanel, BorderLayout.CENTER);
        
        return panel;
    }
    
    /**
     * Erstellt das Panel für die Datentypzuordnungen.
     */
    private JPanel createDatatypesPanel() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        panel.add(new JLabel("Datentyp-Mappings (Oracle -> PostgreSQL):"), BorderLayout.NORTH);
        
        datatypeMappingsModel = new DefaultTableModel() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return true;
            }
        };
        datatypeMappingsModel.addColumn("Oracle-Datentyp");
        datatypeMappingsModel.addColumn("PostgreSQL-Datentyp");
        
        JTable datatypeMappingsTable = new JTable(datatypeMappingsModel);
        datatypeMappingsTable.getColumnModel().getColumn(0).setPreferredWidth(300);
        datatypeMappingsTable.getColumnModel().getColumn(1).setPreferredWidth(300);
        
        JScrollPane tableScroll = new JScrollPane(datatypeMappingsTable);
        panel.add(tableScroll, BorderLayout.CENTER);
        
        // Buttons für die Tabelle
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        addDatatypeMappingButton = new JButton("Hinzufügen");
        addDatatypeMappingButton.addActionListener(e -> {
            datatypeMappingsModel.addRow(new Object[]{"", ""});
        });
        buttonPanel.add(addDatatypeMappingButton);
        
        removeDatatypeMappingButton = new JButton("Entfernen");
        removeDatatypeMappingButton.addActionListener(e -> {
            int selectedRow = datatypeMappingsTable.getSelectedRow();
            if (selectedRow != -1) {
                datatypeMappingsModel.removeRow(selectedRow);
            }
        });
        buttonPanel.add(removeDatatypeMappingButton);
        
        // Standard-Mappings hinzufügen
        JButton addDefaultsButton = new JButton("Standard-Mappings hinzufügen");
        addDefaultsButton.addActionListener(e -> addDefaultDatatypeMappings());
        buttonPanel.add(addDefaultsButton);
        
        panel.add(buttonPanel, BorderLayout.SOUTH);
        
        return panel;
    }
    
    /**
     * Fügt Standard-Datentyp-Mappings zur Tabelle hinzu.
     */
    private void addDefaultDatatypeMappings() {
        // Bestehende Mappings löschen
        while (datatypeMappingsModel.getRowCount() > 0) {
            datatypeMappingsModel.removeRow(0);
        }
        
        // Standard-Mappings hinzufügen
        datatypeMappingsModel.addRow(new Object[]{"NUMBER(1)", "BOOLEAN"});
        datatypeMappingsModel.addRow(new Object[]{"VARCHAR2", "VARCHAR"});
        datatypeMappingsModel.addRow(new Object[]{"CLOB", "TEXT"});
        datatypeMappingsModel.addRow(new Object[]{"BLOB", "BYTEA"});
        datatypeMappingsModel.addRow(new Object[]{"DATE", "DATE"});
        datatypeMappingsModel.addRow(new Object[]{"TIMESTAMP", "TIMESTAMP"});
        datatypeMappingsModel.addRow(new Object[]{"LONG", "TEXT"});
        datatypeMappingsModel.addRow(new Object[]{"RAW", "BYTEA"});
        datatypeMappingsModel.addRow(new Object[]{"FLOAT", "DOUBLE PRECISION"});
        datatypeMappingsModel.addRow(new Object[]{"NUMBER(10,0)", "INTEGER"});
        datatypeMappingsModel.addRow(new Object[]{"NUMBER(19,0)", "BIGINT"});
    }
    
    /**
     * Erstellt das Panel für die Wertetransformationen.
     */
    private JPanel createTransformationsPanel() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        panel.add(new JLabel("Wertetransformationen:"), BorderLayout.NORTH);
        
        transformationsModel = new DefaultTableModel() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return true;
            }
        };
        transformationsModel.addColumn("Transformation");
        transformationsModel.addColumn("Original-Wert");
        transformationsModel.addColumn("Neuer Wert");
        
        transformationsTable = new JTable(transformationsModel);
        transformationsTable.getColumnModel().getColumn(0).setPreferredWidth(200);
        transformationsTable.getColumnModel().getColumn(1).setPreferredWidth(200);
        transformationsTable.getColumnModel().getColumn(2).setPreferredWidth(200);
        
        JScrollPane tableScroll = new JScrollPane(transformationsTable);
        panel.add(tableScroll, BorderLayout.CENTER);
        
        // Buttons für die Tabelle
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        addTransformationButton = new JButton("Hinzufügen");
        addTransformationButton.addActionListener(e -> {
            transformationsModel.addRow(new Object[]{"NUMBER(1)", "", ""});
        });
        buttonPanel.add(addTransformationButton);
        
        removeTransformationButton = new JButton("Entfernen");
        removeTransformationButton.addActionListener(e -> {
            int selectedRow = transformationsTable.getSelectedRow();
            if (selectedRow != -1) {
                transformationsModel.removeRow(selectedRow);
            }
        });
        buttonPanel.add(removeTransformationButton);
        
        // Standard-Transformationen hinzufügen
        JButton addDefaultsButton = new JButton("Standard-Transformationen hinzufügen");
        addDefaultsButton.addActionListener(e -> addDefaultTransformations());
        buttonPanel.add(addDefaultsButton);
        
        panel.add(buttonPanel, BorderLayout.SOUTH);
        
        return panel;
    }
    
    /**
     * Fügt Standard-Transformationen zur Tabelle hinzu.
     */
    private void addDefaultTransformations() {
        // Bestehende Transformationen löschen
        while (transformationsModel.getRowCount() > 0) {
            transformationsModel.removeRow(0);
        }
        
        // Standard-Transformationen hinzufügen
        transformationsModel.addRow(new Object[]{"NUMBER(1)", "0", "false"});
        transformationsModel.addRow(new Object[]{"NUMBER(1)", "1", "true"});
        transformationsModel.addRow(new Object[]{"NUMBER(1)", "-1", "true"});
        transformationsModel.addRow(new Object[]{"DATE", "01-JAN-1900", "1900-01-01"});
        transformationsModel.addRow(new Object[]{"DATE", "31-DEC-9999", "9999-12-31"});
    }
    
    /**
     * Erstellt das Panel für die Boolean-Erkennung.
     */
    private JPanel createBooleanDetectionPanel() {
        JPanel panel = new JPanel(new GridLayout(3, 1, 0, 10));
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        // Präfixe
        JPanel prefixPanel = new JPanel(new BorderLayout());
        prefixPanel.add(new JLabel("Spalten mit diesen Präfixen als BOOLEAN behandeln:"), BorderLayout.NORTH);
        booleanPrefixesArea = new JTextArea();
        booleanPrefixesArea.setText("ist");
        JScrollPane prefixScroll = new JScrollPane(booleanPrefixesArea);
        prefixPanel.add(prefixScroll, BorderLayout.CENTER);
        panel.add(prefixPanel);
        
        // Suffixe
        JPanel suffixPanel = new JPanel(new BorderLayout());
        suffixPanel.add(new JLabel("Spalten mit diesen Suffixen als BOOLEAN behandeln:"), BorderLayout.NORTH);
        booleanSuffixesArea = new JTextArea();
        booleanSuffixesArea.setText("FLAG");
        JScrollPane suffixScroll = new JScrollPane(booleanSuffixesArea);
        suffixPanel.add(suffixScroll, BorderLayout.CENTER);
        panel.add(suffixPanel);
        
        // Exakte Namen
        JPanel namesPanel = new JPanel(new BorderLayout());
        namesPanel.add(new JLabel("Spalten mit diesen exakten Namen als BOOLEAN behandeln:"), BorderLayout.NORTH);
        booleanNamesArea = new JTextArea();
        booleanNamesArea.setText("LFLAG");
        JScrollPane namesScroll = new JScrollPane(booleanNamesArea);
        namesPanel.add(namesScroll, BorderLayout.CENTER);
        panel.add(namesPanel);
        
        return panel;
    }
    
    /**
     * Erstellt das Panel für die Migration und Logs.
     */
    private JPanel createMigrationPanel() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        panel.add(new JLabel("Log-Ausgabe:"), BorderLayout.NORTH);
        
        logArea = new JTextArea();
        logArea.setEditable(false);
        JScrollPane logScroll = new JScrollPane(logArea);
        panel.add(logScroll, BorderLayout.CENTER);
        
        JButton clearButton = new JButton("Log löschen");
        clearButton.addActionListener(e -> logArea.setText(""));
        panel.add(clearButton, BorderLayout.SOUTH);
        
        return panel;
    }
    
    /**
     * Erstellt das Panel für die PK/FK-Informationen.
     */
    private JPanel createPkFkPanel() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        
        // Panel für Tabellenauswahl und Anzeige-Button
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        topPanel.add(new JLabel("Tabelle auswählen:"));
        
        tabellenAuswahlComboBox = new JComboBox<>();
        topPanel.add(tabellenAuswahlComboBox);
        
        anzeigenPkFkButton = new JButton("PK/FK-Informationen anzeigen");
        anzeigenPkFkButton.addActionListener(e -> loadPkFkInformation());
        topPanel.add(anzeigenPkFkButton);
        
        // Button zum Kopieren der SQL-Statements in die Zwischenablage
        JButton copyToClipboardButton = new JButton("Schlüssel-SQL kopieren");
        copyToClipboardButton.addActionListener(e -> copyPkFkSqlToClipboard());
        topPanel.add(copyToClipboardButton);
        
        panel.add(topPanel, BorderLayout.NORTH);
        
        // Panel für Primärschlüssel
        JPanel centerPanel = new JPanel(new GridLayout(2, 1, 0, 10));
        
        JPanel pkPanel = new JPanel(new BorderLayout());
        pkPanel.add(new JLabel("Primärschlüssel:"), BorderLayout.NORTH);
        
        primärschlüsselModel = new DefaultTableModel() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        primärschlüsselModel.addColumn("Tabelle");
        primärschlüsselModel.addColumn("Primärschlüssel-Name");
        primärschlüsselModel.addColumn("Spalte");
        primärschlüsselModel.addColumn("Position");
        
        primärschlüsselTable = new JTable(primärschlüsselModel);
        JScrollPane pkScroll = new JScrollPane(primärschlüsselTable);
        pkPanel.add(pkScroll, BorderLayout.CENTER);
        
        centerPanel.add(pkPanel);
        
        // Panel für Fremdschlüssel
        JPanel fkPanel = new JPanel(new BorderLayout());
        fkPanel.add(new JLabel("Fremdschlüssel:"), BorderLayout.NORTH);
        
        fremdschlüsselModel = new DefaultTableModel() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        fremdschlüsselModel.addColumn("Tabelle");
        fremdschlüsselModel.addColumn("Fremdschlüssel-Name");
        fremdschlüsselModel.addColumn("Quellspalte");
        fremdschlüsselModel.addColumn("Zieltabelle");
        fremdschlüsselModel.addColumn("Zielspalte");
        fremdschlüsselModel.addColumn("DELETE-Regel");
        fremdschlüsselModel.addColumn("UPDATE-Regel");
        
        fremdschlüsselTable = new JTable(fremdschlüsselModel);
        JScrollPane fkScroll = new JScrollPane(fremdschlüsselTable);
        fkPanel.add(fkScroll, BorderLayout.CENTER);
        
        centerPanel.add(fkPanel);
        
        panel.add(centerPanel, BorderLayout.CENTER);
        
        return panel;
    }
    
    /**
     * Kopiert die PK/FK-SQL-Statements in die Zwischenablage.
     */
    private void copyPkFkSqlToClipboard() {
        String selectedTable = (String) tabellenAuswahlComboBox.getSelectedItem();
        if (selectedTable == null || selectedTable.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, 
                    "Bitte wählen Sie eine Tabelle aus und laden Sie zuerst die PK/FK-Informationen.", 
                    "Validierungsfehler", JOptionPane.WARNING_MESSAGE);
            return;
        }
        
        StringBuilder sql = new StringBuilder();
        
        // Primärschlüssel
        if (primärschlüsselModel.getRowCount() > 0) {
            sql.append("-- Primärschlüssel für ").append(selectedTable).append("\n");
            
            Map<String, List<String>> pkMap = new HashMap<>();
            for (int i = 0; i < primärschlüsselModel.getRowCount(); i++) {
                String pkName = (String) primärschlüsselModel.getValueAt(i, 1);
                String column = (String) primärschlüsselModel.getValueAt(i, 2);
                
                if (!pkMap.containsKey(pkName)) {
                    pkMap.put(pkName, new ArrayList<>());
                }
                pkMap.get(pkName).add(column);
            }
            
            for (Map.Entry<String, List<String>> entry : pkMap.entrySet()) {
                sql.append("ALTER TABLE ").append(selectedTable).append(" ADD CONSTRAINT ")
                   .append(entry.getKey()).append(" PRIMARY KEY (")
                   .append(String.join(", ", entry.getValue())).append(");\n");
            }
            sql.append("\n");
        }
        
        // Fremdschlüssel
        if (fremdschlüsselModel.getRowCount() > 0) {
            sql.append("-- Fremdschlüssel für ").append(selectedTable).append("\n");
            
            Map<String, Map<String, Object>> fkMap = new HashMap<>();
            for (int i = 0; i < fremdschlüsselModel.getRowCount(); i++) {
                String fkName = (String) fremdschlüsselModel.getValueAt(i, 1);
                String sourceColumn = (String) fremdschlüsselModel.getValueAt(i, 2);
                String targetTable = (String) fremdschlüsselModel.getValueAt(i, 3);
                String targetColumn = (String) fremdschlüsselModel.getValueAt(i, 4);
                String deleteRule = (String) fremdschlüsselModel.getValueAt(i, 5);
                String updateRule = (String) fremdschlüsselModel.getValueAt(i, 6);
                
                if (!fkMap.containsKey(fkName)) {
                    Map<String, Object> details = new HashMap<>();
                    details.put("sourceColumns", new ArrayList<String>());
                    details.put("targetColumns", new ArrayList<String>());
                    details.put("targetTable", targetTable);
                    details.put("deleteRule", deleteRule);
                    details.put("updateRule", updateRule);
                    fkMap.put(fkName, details);
                }
                
                ((List<String>) fkMap.get(fkName).get("sourceColumns")).add(sourceColumn);
                ((List<String>) fkMap.get(fkName).get("targetColumns")).add(targetColumn);
            }
            
            for (Map.Entry<String, Map<String, Object>> entry : fkMap.entrySet()) {
                Map<String, Object> details = entry.getValue();
                sql.append("ALTER TABLE ").append(selectedTable)
                   .append(" ADD CONSTRAINT ").append(entry.getKey())
                   .append(" FOREIGN KEY (")
                   .append(String.join(", ", (List<String>) details.get("sourceColumns")))
                   .append(") REFERENCES ").append(details.get("targetTable")).append(" (")
                   .append(String.join(", ", (List<String>) details.get("targetColumns"))).append(")")
                   .append(" ON DELETE ").append(details.get("deleteRule"))
                   .append(" ON UPDATE ").append(details.get("updateRule"))
                   .append(";\n");
            }
        }
        
        if (sql.length() > 0) {
            // In die Zwischenablage kopieren
            java.awt.Toolkit.getDefaultToolkit().getSystemClipboard().setContents(
                new java.awt.datatransfer.StringSelection(sql.toString()), null);
            
            JOptionPane.showMessageDialog(this,
                    "SQL für Primär- und Fremdschlüssel wurde in die Zwischenablage kopiert.",
                    "SQL kopiert", JOptionPane.INFORMATION_MESSAGE);
        } else {
            JOptionPane.showMessageDialog(this,
                    "Keine PK/FK-Informationen gefunden. Bitte laden Sie zuerst die Informationen.",
                    "Keine Daten", JOptionPane.WARNING_MESSAGE);
        }
    }
    
    /**
     * Speichert die aktuelle Konfiguration in einer properties-Datei.
     */
    private void saveConfiguration() {
        JFileChooser chooser = new JFileChooser();
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("Properties-Dateien", "properties"));
        int result = chooser.showSaveDialog(this);
        
        if (result == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            if (!file.getName().toLowerCase().endsWith(".properties")) {
                file = new File(file.getAbsolutePath() + ".properties");
            }
            
            try (FileWriter writer = new FileWriter(file)) {
                // Datenbankverbindungen
                writer.write("# Oracle-Datenbankverbindung\n");
                writer.write("oracle.url=" + oracleUrlField.getText() + "\n");
                writer.write("oracle.benutzer=" + oracleUserField.getText() + "\n");
                writer.write("oracle.passwort=" + new String(oraclePasswordField.getPassword()) + "\n\n");
                
                writer.write("# PostgreSQL-Datenbankverbindung\n");
                writer.write("postgres.url=" + postgresUrlField.getText() + "\n");
                writer.write("postgres.benutzer=" + postgresUserField.getText() + "\n");
                writer.write("postgres.passwort=" + new String(postgresPasswordField.getPassword()) + "\n\n");
                
                // Ausgabepfad
                writer.write("# Ausgabepfad für SQL-Dateien\n");
                writer.write("ausgabe.pfad=" + outputPathField.getText() + "\n\n");
                
                // Tabellen-Whitelist und Blacklist
                writer.write("# Tabellen für Migration (durch Kommas getrennt)\n");
                writer.write("tabellen.whitelist=" + whitelistTextArea.getText().replace("\n", ",") + "\n\n");
                
                writer.write("# Tabellen, die von der Migration ausgeschlossen werden sollen\n");
                writer.write("tabellen.blacklist=" + blacklistTextArea.getText().replace("\n", ",") + "\n\n");
                
                // Ignorierte Spalten
                writer.write("# Spalten, die pro Tabelle ignoriert werden sollen\n");
                for (int i = 0; i < ignoredColumnsModel.getRowCount(); i++) {
                    String tableName = (String) ignoredColumnsModel.getValueAt(i, 0);
                    String ignoredColumns = (String) ignoredColumnsModel.getValueAt(i, 1);
                    if (tableName != null && !tableName.isEmpty() && ignoredColumns != null && !ignoredColumns.isEmpty()) {
                        writer.write("tabelle." + tableName + ".ignorierte_spalten=" + ignoredColumns + "\n");
                    }
                }
                writer.write("\n");
                
                // Datentyp-Mappings
                writer.write("# Datentyp-Mappings (Oracle -> PostgreSQL)\n");
                StringBuilder mappings = new StringBuilder();
                for (int i = 0; i < datatypeMappingsModel.getRowCount(); i++) {
                    String oracleType = (String) datatypeMappingsModel.getValueAt(i, 0);
                    String pgType = (String) datatypeMappingsModel.getValueAt(i, 1);
                    if (oracleType != null && !oracleType.isEmpty() && pgType != null && !pgType.isEmpty()) {
                        if (mappings.length() > 0) {
                            mappings.append(";");
                        }
                        mappings.append(oracleType).append("->").append(pgType);
                    }
                }
                writer.write("datentyp.mapping=" + mappings.toString() + "\n\n");
                
                // Wertetransformationen
                writer.write("# Wertetransformationen für spezielle Typen\n");
                for (int i = 0; i < transformationsModel.getRowCount(); i++) {
                    String type = (String) transformationsModel.getValueAt(i, 0);
                    String fromValue = (String) transformationsModel.getValueAt(i, 1);
                    String toValue = (String) transformationsModel.getValueAt(i, 2);
                    
                    if (type != null && !type.isEmpty() && fromValue != null && !fromValue.isEmpty() 
                            && toValue != null && !toValue.isEmpty()) {
                        String key = "transform." + type.replaceAll("[^a-zA-Z0-9]", "_");
                        // Prüfen, ob es schon eine Transformation für diesen Typ gibt
                        String content = writer.toString();
                        if (content.contains(key)) {
                            writer.write(";" + fromValue + "->" + toValue);
                        } else {
                            writer.write(key + "=" + fromValue + "->" + toValue + "\n");
                        }
                    }
                }
                writer.write("\n");
                
                // Boolean-Erkennung
                writer.write("# Spalten mit bestimmten Präfixen/Suffixen zu BOOLEAN konvertieren (unabhängig vom Originaltyp)\n");
                writer.write("spalte.praefixe.boolean=" + booleanPrefixesArea.getText().replace("\n", ",") + "\n");
                writer.write("spalte.suffixe.boolean=" + booleanSuffixesArea.getText().replace("\n", ",") + "\n");
                writer.write("spalte.namen.boolean=" + booleanNamesArea.getText().replace("\n", ",") + "\n");
                
                logArea.append("Konfiguration gespeichert in " + file.getAbsolutePath() + "\n");
            } catch (IOException e) {
                JOptionPane.showMessageDialog(this, 
                        "Fehler beim Speichern der Konfiguration: " + e.getMessage(), 
                        "Fehler", JOptionPane.ERROR_MESSAGE);
                logArea.append("Fehler: " + e.getMessage() + "\n");
            }
        }
    }
    
    /**
     * Lädt eine Konfiguration aus einer properties-Datei.
     */
    private void loadConfiguration() {
        JFileChooser chooser = new JFileChooser();
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("Properties-Dateien", "properties"));
        int result = chooser.showOpenDialog(this);
        
        if (result == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            
            try {
                Properties props = new Properties();
                props.load(new java.io.FileReader(file));
                
                // Datenbankverbindungen
                oracleUrlField.setText(props.getProperty("oracle.url", ""));
                oracleUserField.setText(props.getProperty("oracle.benutzer", ""));
                oraclePasswordField.setText(props.getProperty("oracle.passwort", ""));
                
                postgresUrlField.setText(props.getProperty("postgres.url", ""));
                postgresUserField.setText(props.getProperty("postgres.benutzer", ""));
                postgresPasswordField.setText(props.getProperty("postgres.passwort", ""));
                
                // Ausgabepfad
                outputPathField.setText(props.getProperty("ausgabe.pfad", "./output/"));
                
                // Tabellen-Whitelist und Blacklist
                whitelistTextArea.setText(props.getProperty("tabellen.whitelist", "").replace(",", "\n"));
                blacklistTextArea.setText(props.getProperty("tabellen.blacklist", "").replace(",", "\n"));
                
                // Ignorierte Spalten
                while (ignoredColumnsModel.getRowCount() > 0) {
                    ignoredColumnsModel.removeRow(0);
                }
                
                for (String key : props.stringPropertyNames()) {
                    if (key.startsWith("tabelle.") && key.endsWith(".ignorierte_spalten")) {
                        String tableName = key.substring(8, key.length() - 19);
                        String ignoredColumns = props.getProperty(key, "");
                        ignoredColumnsModel.addRow(new Object[]{tableName, ignoredColumns});
                    }
                }
                
                // Datentyp-Mappings
                while (datatypeMappingsModel.getRowCount() > 0) {
                    datatypeMappingsModel.removeRow(0);
                }
                
                String mappingsString = props.getProperty("datentyp.mapping", "");
                if (!mappingsString.isEmpty()) {
                    String[] mappings = mappingsString.split(";");
                    for (String mapping : mappings) {
                        if (!mapping.trim().isEmpty()) {
                            String[] parts = mapping.split("->");
                            if (parts.length == 2) {
                                datatypeMappingsModel.addRow(new Object[]{parts[0].trim(), parts[1].trim()});
                            }
                        }
                    }
                }
                
                // Wertetransformationen
                while (transformationsModel.getRowCount() > 0) {
                    transformationsModel.removeRow(0);
                }
                
                for (String key : props.stringPropertyNames()) {
                    if (key.startsWith("transform.")) {
                        String type = key.substring(10).replaceAll("_", "");
                        String transformsStr = props.getProperty(key, "");
                        String[] transforms = transformsStr.split(";");
                        
                        for (String transform : transforms) {
                            if (!transform.trim().isEmpty()) {
                                String[] parts = transform.split("->");
                                if (parts.length == 2) {
                                    transformationsModel.addRow(new Object[]{type, parts[0].trim(), parts[1].trim()});
                                }
                            }
                        }
                    }
                }
                
                // Boolean-Erkennung
                booleanPrefixesArea.setText(props.getProperty("spalte.praefixe.boolean", "ist").replace(",", "\n"));
                booleanSuffixesArea.setText(props.getProperty("spalte.suffixe.boolean", "FLAG").replace(",", "\n"));
                booleanNamesArea.setText(props.getProperty("spalte.namen.boolean", "LFLAG").replace(",", "\n"));
                
                // Tabellenliste für PK/FK-Tab aktualisieren
                updateTableList();
                
                logArea.append("Konfiguration geladen aus " + file.getAbsolutePath() + "\n");
            } catch (IOException e) {
                JOptionPane.showMessageDialog(this, 
                        "Fehler beim Laden der Konfiguration: " + e.getMessage(), 
                        "Fehler", JOptionPane.ERROR_MESSAGE);
                logArea.append("Fehler: " + e.getMessage() + "\n");
            }
        }
    }
    
    /**
     * Aktualisiert die Tabellenliste in der ComboBox im PK/FK-Tab.
     */
    private void updateTableList() {
        if (tabellenAuswahlComboBox != null) { // Sicherstellen, dass die ComboBox bereits initialisiert wurde
            tabellenAuswahlComboBox.removeAllItems();
            
            String whitelistText = whitelistTextArea.getText();
            if (whitelistText != null && !whitelistText.isEmpty()) {
                String[] tabellen = whitelistText.split("[,\\s]+");
                for (String tabelle : tabellen) {
                    if (!tabelle.trim().isEmpty()) {
                        tabellenAuswahlComboBox.addItem(tabelle.trim());
                    }
                }
            }
        }
    }
    
    /**
     * Lädt PK/FK-Informationen für die ausgewählte Tabelle.
     */
    private void loadPkFkInformation() {
        String selectedTable = (String) tabellenAuswahlComboBox.getSelectedItem();
        if (selectedTable == null || selectedTable.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, 
                    "Bitte wählen Sie eine Tabelle aus.", 
                    "Keine Tabelle ausgewählt", JOptionPane.WARNING_MESSAGE);
            return;
        }
        
        try {
            // Temporäre Konfigurationsdatei erstellen
            File tempFile = File.createTempFile("pk_fk_info_", ".properties");
            tempFile.deleteOnExit();
            
            try (FileWriter writer = new FileWriter(tempFile)) {
                writer.write("oracle.url=" + oracleUrlField.getText() + "\n");
                writer.write("oracle.benutzer=" + oracleUserField.getText() + "\n");
                writer.write("oracle.passwort=" + new String(oraclePasswordField.getPassword()) + "\n");
                // Ausgabepfad wird benötigt, um die Konfiguration zu initialisieren
                writer.write("ausgabe.pfad=" + outputPathField.getText() + "\n");
            }
            
            // UI-Status aktualisieren
            anzeigenPkFkButton.setEnabled(false);
            
            // In separatem Thread ausführen
            new Thread(() -> {
                Connection oracleConnection = null;
                try {
                    // Vorherige Daten löschen
                    SwingUtilities.invokeLater(() -> {
                        while (primärschlüsselModel.getRowCount() > 0) {
                            primärschlüsselModel.removeRow(0);
                        }
                        while (fremdschlüsselModel.getRowCount() > 0) {
                            fremdschlüsselModel.removeRow(0);
                        }
                    });
                    
                    // Logger für die GUI konfigurieren
                    Logger.info("Lade PK/FK-Informationen für Tabelle: " + selectedTable);
                    
                    // Konfiguration laden
                    Konfiguration konfiguration = new Konfiguration(tempFile.getAbsolutePath());
                    
                    // Datenbankverbindungen herstellen
                    DbVerbindung dbVerbindung = new DbVerbindung();
                    
                    // Oracle-Verbindung herstellen
                    oracleConnection = dbVerbindung.verbindeZuOracle(
                        konfiguration.getOracleUrl(), 
                        konfiguration.getOracleBenutzer(), 
                        konfiguration.getOraclePasswort()
                    );
                    
                    // Migrator erstellen um die PK/FK-Informationen auszulesen
                    OracleToPgMigrator migrator = new OracleToPgMigrator(konfiguration, oracleConnection);
                    
                    // Primärschlüssel laden
                    Map<String, List<String>> pkInfos = migrator.getPrimärschlüsselInfo(selectedTable);
                    
                    SwingUtilities.invokeLater(() -> {
                        for (Map.Entry<String, List<String>> entry : pkInfos.entrySet()) {
                            String pkName = entry.getKey();
                            List<String> columns = entry.getValue();
                            
                            int position = 1;
                            for (String column : columns) {
                                primärschlüsselModel.addRow(new Object[] {
                                    selectedTable, 
                                    pkName,
                                    column,
                                    position++
                                });
                            }
                        }
                    });
                    
                    // Fremdschlüssel laden
                    Map<String, List<Map<String, String>>> fkInfos = migrator.getFremdschlüsselInfo(selectedTable);
                    
                    final boolean isEmpty = pkInfos.isEmpty() && fkInfos.isEmpty();
                    
                    SwingUtilities.invokeLater(() -> {
                        for (Map.Entry<String, List<Map<String, String>>> entry : fkInfos.entrySet()) {
                            String fkName = entry.getKey();
                            List<Map<String, String>> details = entry.getValue();
                            
                            for (Map<String, String> detail : details) {
                                fremdschlüsselModel.addRow(new Object[] {
                                    selectedTable,
                                    fkName,
                                    detail.get("quellSpalte"),
                                    detail.get("zielTabelle"),
                                    detail.get("zielSpalte"),
                                    detail.get("deleteRegel"),
                                    detail.get("updateRegel")
                                });
                            }
                        }
                        
                        // Meldung anzeigen wenn keine PK/FK gefunden wurden
                        if (isEmpty) {
                            JOptionPane.showMessageDialog(MigrationsGUI.this,
                                "Keine PK/FK-Informationen für Tabelle " + selectedTable + " gefunden.",
                                "Information", JOptionPane.INFORMATION_MESSAGE);
                        } else {
                            if (!pkInfos.isEmpty()) {
                                Logger.info("Primärschlüssel gefunden: " + pkInfos.size() + " für Tabelle " + selectedTable);
                            }
                            if (!fkInfos.isEmpty()) {
                                Logger.info("Fremdschlüssel gefunden: " + fkInfos.size() + " für Tabelle " + selectedTable);
                            }
                        }
                        
                        // UI aktualisieren
                        anzeigenPkFkButton.setEnabled(true);
                    });
                    
                } catch (Exception e) {
                    Logger.fehler("Fehler beim Laden der PK/FK-Informationen", e);
                    
                    SwingUtilities.invokeLater(() -> {
                        JOptionPane.showMessageDialog(MigrationsGUI.this, 
                                "Fehler beim Laden der PK/FK-Informationen: " + e.getMessage(), 
                                "Fehler", JOptionPane.ERROR_MESSAGE);
                        anzeigenPkFkButton.setEnabled(true);
                    });
                } finally {
                    // Datenbankverbindung schließen
                    if (oracleConnection != null) {
                        try {
                            oracleConnection.close();
                            Logger.info("Oracle-Verbindung geschlossen");
                        } catch (SQLException e) {
                            Logger.fehler("Fehler beim Schließen der Oracle-Verbindung", e);
                        }
                    }
                    
                    // Temporäre Datei löschen
                    if (tempFile.exists()) {
                        tempFile.delete();
                    }
                }
            }).start();
        } catch (IOException e) {
            JOptionPane.showMessageDialog(this, 
                    "Fehler beim Erstellen der temporären Datei: " + e.getMessage(), 
                    "Fehler", JOptionPane.ERROR_MESSAGE);
        }
    }
    
    /**
     * Startet die Migration.
     */
    private void startMigration() {
        // Konfiguration speichern
        File tempConfig = null;
        try {
            tempConfig = File.createTempFile("migration_", ".properties");
            tempConfig.deleteOnExit();
            
            File finalTempConfig = tempConfig; // Für Lambda-Nutzung
            
            try (FileWriter writer = new FileWriter(tempConfig)) {
                // Datenbankverbindungen
                writer.write("oracle.url=" + oracleUrlField.getText() + "\n");
                writer.write("oracle.benutzer=" + oracleUserField.getText() + "\n");
                writer.write("oracle.passwort=" + new String(oraclePasswordField.getPassword()) + "\n");
                
                writer.write("postgres.url=" + postgresUrlField.getText() + "\n");
                writer.write("postgres.benutzer=" + postgresUserField.getText() + "\n");
                writer.write("postgres.passwort=" + new String(postgresPasswordField.getPassword()) + "\n");
                
                // Ausgabepfad
                writer.write("ausgabe.pfad=" + outputPathField.getText() + "\n");
                
                // Tabellen-Whitelist und Blacklist
                writer.write("tabellen.whitelist=" + whitelistTextArea.getText().replace("\n", ",") + "\n");
                writer.write("tabellen.blacklist=" + blacklistTextArea.getText().replace("\n", ",") + "\n");
                
                // Ignorierte Spalten
                for (int i = 0; i < ignoredColumnsModel.getRowCount(); i++) {
                    String tableName = (String) ignoredColumnsModel.getValueAt(i, 0);
                    String ignoredColumns = (String) ignoredColumnsModel.getValueAt(i, 1);
                    if (tableName != null && !tableName.isEmpty() && ignoredColumns != null && !ignoredColumns.isEmpty()) {
                        writer.write("tabelle." + tableName + ".ignorierte_spalten=" + ignoredColumns + "\n");
                    }
                }
                
                // Datentyp-Mappings
                StringBuilder mappings = new StringBuilder();
                for (int i = 0; i < datatypeMappingsModel.getRowCount(); i++) {
                    String oracleType = (String) datatypeMappingsModel.getValueAt(i, 0);
                    String pgType = (String) datatypeMappingsModel.getValueAt(i, 1);
                    if (oracleType != null && !oracleType.isEmpty() && pgType != null && !pgType.isEmpty()) {
                        if (mappings.length() > 0) {
                            mappings.append(";");
                        }
                        mappings.append(oracleType).append("->").append(pgType);
                    }
                }
                writer.write("datentyp.mapping=" + mappings.toString() + "\n");
                
                // Wertetransformationen
                Map<String, StringBuilder> transformationen = new HashMap<>();
                
                for (int i = 0; i < transformationsModel.getRowCount(); i++) {
                    String type = (String) transformationsModel.getValueAt(i, 0);
                    String fromValue = (String) transformationsModel.getValueAt(i, 1);
                    String toValue = (String) transformationsModel.getValueAt(i, 2);
                    
                    if (type != null && !type.isEmpty() && fromValue != null && !fromValue.isEmpty() 
                            && toValue != null && !toValue.isEmpty()) {
                        String key = type.replaceAll("[^a-zA-Z0-9]", "_");
                        
                        if (!transformationen.containsKey(key)) {
                            transformationen.put(key, new StringBuilder());
                        }
                        
                        StringBuilder sb = transformationen.get(key);
                        if (sb.length() > 0) {
                            sb.append(";");
                        }
                        sb.append(fromValue).append("->").append(toValue);
                    }
                }
                
                for (String key : transformationen.keySet()) {
                    writer.write("transform." + key + "=" + transformationen.get(key).toString() + "\n");
                }
                
                // Boolean-Erkennung
                writer.write("spalte.praefixe.boolean=" + booleanPrefixesArea.getText().replace("\n", ",") + "\n");
                writer.write("spalte.suffixe.boolean=" + booleanSuffixesArea.getText().replace("\n", ",") + "\n");
                writer.write("spalte.namen.boolean=" + booleanNamesArea.getText().replace("\n", ",") + "\n");
            }
            
            // Validiere Konfiguration und Verbindungseinstellungen
            if (oracleUrlField.getText().trim().isEmpty() || 
                    oracleUserField.getText().trim().isEmpty() ||
                    oraclePasswordField.getPassword().length == 0) {
                JOptionPane.showMessageDialog(this, 
                        "Bitte alle Oracle-Verbindungsinformationen angeben.", 
                        "Validierungsfehler", JOptionPane.WARNING_MESSAGE);
                return;
            }
            
            if (whitelistTextArea.getText().trim().isEmpty()) {
                JOptionPane.showMessageDialog(this, 
                        "Bitte mindestens eine Tabelle für die Migration angeben.", 
                        "Validierungsfehler", JOptionPane.WARNING_MESSAGE);
                return;
            }
            
            // Ausgabeordner erstellen, falls nicht vorhanden
            File ausgabeDir = new File(outputPathField.getText());
            if (!ausgabeDir.exists()) {
                if (!ausgabeDir.mkdirs()) {
                    JOptionPane.showMessageDialog(this, 
                            "Ausgabeordner konnte nicht erstellt werden: " + outputPathField.getText(), 
                            "Fehler", JOptionPane.ERROR_MESSAGE);
                    return;
                }
            }
            
            // Benutzer bestätigen lassen
            String[] tabellen = whitelistTextArea.getText().split("[,\\s]+");
            int anzahlTabellen = 0;
            for (String tabelle : tabellen) {
                if (!tabelle.trim().isEmpty()) {
                    anzahlTabellen++;
                }
            }
            
            int confirm = JOptionPane.showConfirmDialog(this,
                    "Migration starten mit " + anzahlTabellen + 
                    " Tabellen?\nDies kann einige Zeit dauern.", 
                    "Migration bestätigen", 
                    JOptionPane.YES_NO_OPTION);
            if (confirm != JOptionPane.YES_OPTION) {
                return;
            }
            
            // UI-Elemente deaktivieren während Migration läuft
            startMigrationButton.setEnabled(false);
            progressBar.setValue(0);
            progressLabel.setText("Migration wird vorbereitet...");
            
            // Migration in einem separaten Thread starten
            new Thread(() -> {
                try {
                    // Logger-Handler für GUI setzen
                    Logger.setLogHandler(message -> {
                        SwingUtilities.invokeLater(() -> {
                            logArea.append(message + "\n");
                            logArea.setCaretPosition(logArea.getDocument().getLength());
                        });
                    });
                    
                    // Konfiguration laden
                    Konfiguration konfiguration = new Konfiguration(finalTempConfig.getAbsolutePath());
                    
                    // Datenbankverbindungen herstellen
                    DbVerbindung dbVerbindung = new DbVerbindung();
                    
                    try {
                        // Oracle-Verbindung herstellen
                        Connection oracleConnection = dbVerbindung.verbindeZuOracle(
                            konfiguration.getOracleUrl(), 
                            konfiguration.getOracleBenutzer(), 
                            konfiguration.getOraclePasswort()
                        );
                        
                        // Liste der zu migrierenden Tabellen ermitteln
                        final List<String> tabellenliste = konfiguration.getWhitelist();
                        final List<String> blacklist = konfiguration.getBlacklist();
                        
                        // Tabellen filtern, die nicht in der Blacklist sind
                        final List<String> effektiveTabellen = new ArrayList<>();
                        for (String tabelle : tabellenliste) {
                            if (!blacklist.contains(tabelle)) {
                                effektiveTabellen.add(tabelle);
                            }
                        }
                        
                        final int gesamtanzahlTabellen = effektiveTabellen.size();
                        final int[] bearbeitetTabellen = {0};
                        
                        // Eigene OracleToPgMigrator-Klasse mit Fortschrittsmeldungen
                        OracleToPgMigrator migrator = new OracleToPgMigrator(konfiguration, oracleConnection) {
                            @Override
                            protected void tableMigrated(String tabelle) {
                                // Überschriebene Methode zum Aktualisieren des Fortschritts
                                bearbeitetTabellen[0]++;
                                final int fortschritt = (bearbeitetTabellen[0] * 100) / gesamtanzahlTabellen;
                                
                                SwingUtilities.invokeLater(() -> {
                                    progressBar.setValue(fortschritt);
                                    progressLabel.setText("Tabellen migriert: " + bearbeitetTabellen[0] + "/" + gesamtanzahlTabellen);
                                });
                            }
                        };
                        
                        // Migration starten
                        migrator.migriere();
                        
                        // Erfolgreich abgeschlossen
                        SwingUtilities.invokeLater(() -> {
                            progressBar.setValue(100);
                            progressLabel.setText("Migration abgeschlossen");
                            JOptionPane.showMessageDialog(this, 
                                    "Migration erfolgreich abgeschlossen.\n" + 
                                    "SQL-Dateien wurden gespeichert in:\n" + 
                                    outputPathField.getText(), 
                                    "Migration abgeschlossen", 
                                    JOptionPane.INFORMATION_MESSAGE);
                            startMigrationButton.setEnabled(true);
                        });
                        
                    } catch (SQLException e) {
                        Logger.fehler("Datenbankfehler während der Migration", e);
                        
                        SwingUtilities.invokeLater(() -> {
                            progressBar.setValue(0);
                            progressLabel.setText("Fehler bei Migration");
                            JOptionPane.showMessageDialog(this, 
                                    "Fehler während der Migration: " + e.getMessage(), 
                                    "Migrationsfehler", 
                                    JOptionPane.ERROR_MESSAGE);
                            startMigrationButton.setEnabled(true);
                        });
                    } finally {
                        // Datenbankverbindungen schließen
                        dbVerbindung.schliesseVerbindungen();
                    }
                    
                } catch (Exception e) {
                    Logger.fehler("Allgemeiner Fehler während der Migration", e);
                    
                    SwingUtilities.invokeLater(() -> {
                        progressBar.setValue(0);
                        progressLabel.setText("Fehler bei Migration");
                        JOptionPane.showMessageDialog(this, 
                                "Fehler während der Migration: " + e.getMessage(), 
                                "Migrationsfehler", 
                                JOptionPane.ERROR_MESSAGE);
                        startMigrationButton.setEnabled(true);
                    });
                } finally {
                    // Temp-Datei löschen
                    if (finalTempConfig != null && finalTempConfig.exists()) {
                        finalTempConfig.delete();
                    }
                }
            }).start();
        } catch (IOException e) {
            JOptionPane.showMessageDialog(this, 
                    "Fehler beim Erstellen der temporären Konfigurationsdatei: " + e.getMessage(), 
                    "Fehler", JOptionPane.ERROR_MESSAGE);
        }
    }
}
