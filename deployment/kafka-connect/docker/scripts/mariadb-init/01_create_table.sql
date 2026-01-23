-- Create customers table to receive data from Kafka sink
CREATE TABLE IF NOT EXISTS customers (
    ID VARCHAR(50) PRIMARY KEY,
    NAME VARCHAR(100),
    ADDRESS VARCHAR(200),
    DESCRIPTION VARCHAR(500)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
