package com.example.kafka.connect.iidr.operation;

/**
 * CDC operation types derived from DB2 A_ENTTYP header codes.
 */
public enum CdcOperation {
    INSERT,
    UPDATE,
    DELETE,
    UPSERT  // For codes like UR that can be either INSERT or UPDATE
}
