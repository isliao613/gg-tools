package com.example.kafka.connect.iidr.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Factory to create the appropriate Dialect based on the JDBC connection.
 */
public class DialectFactory {

    private static final Logger log = Logger.getLogger(DialectFactory.class.getName());

    /**
     * Create a Dialect instance based on the database product name.
     */
    public static Dialect create(Connection connection) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        String dbProductName = meta.getDatabaseProductName();
        log.info("Resolved database product name: " + dbProductName);

        if ("MySQL".equalsIgnoreCase(dbProductName)) {
            return new MySqlDialect();
        }
        if ("PostgreSQL".equalsIgnoreCase(dbProductName)) {
            return new PostgreSqlDialect();
        }
        // Add more dialects here for other databases
        // e.g., Oracle, SQL Server

        log.warning("No specific dialect found for '" + dbProductName + "'. " +
                    "Using generic dialect with limited functionality.");
        return new GenericDialect();
    }
}
