package de.dbmigration.core;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Interface für die Durchführung einer Datenbankmigration.
 */
public interface Migrator {
    /**
     * Führt die Migration durch.
     * 
     * @throws SQLException Bei Datenbankfehlern
     * @throws IOException Bei Dateisystemfehlern
     */
    void migriere() throws SQLException, IOException;
}
