package com.example.kafka.connect.iidr.operation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Maps IBM Journal Entry Type codes (A_ENTTYP) to CDC operations.
 *
 * Based on the DB2 CDC Event Specification:
 * - INSERT: PT, RR, PX
 * - UPDATE: UP, FI, FP
 * - UPSERT: UR (appears in both INSERT and UPDATE per spec)
 * - DELETE: DL, DR
 */
public class EntryTypeMapper {

    private static final Map<String, CdcOperation> ENTRY_TYPE_MAP = new HashMap<>();

    static {
        // INSERT codes: PT, RR, PX
        ENTRY_TYPE_MAP.put("PT", CdcOperation.INSERT);
        ENTRY_TYPE_MAP.put("RR", CdcOperation.INSERT);
        ENTRY_TYPE_MAP.put("PX", CdcOperation.INSERT);

        // UPDATE codes: UP, FI, FP
        ENTRY_TYPE_MAP.put("UP", CdcOperation.UPDATE);
        ENTRY_TYPE_MAP.put("FI", CdcOperation.UPDATE);
        ENTRY_TYPE_MAP.put("FP", CdcOperation.UPDATE);

        // UR appears in both INSERT and UPDATE in spec - treat as UPSERT
        ENTRY_TYPE_MAP.put("UR", CdcOperation.UPSERT);

        // DELETE codes: DL, DR
        ENTRY_TYPE_MAP.put("DL", CdcOperation.DELETE);
        ENTRY_TYPE_MAP.put("DR", CdcOperation.DELETE);
    }

    /**
     * Map an A_ENTTYP code to a CDC operation.
     *
     * @param entryType The IBM Journal Entry Type code
     * @return The corresponding CdcOperation, or null if unrecognized
     */
    public static CdcOperation mapEntryType(String entryType) {
        if (entryType == null || entryType.trim().isEmpty()) {
            return null;
        }
        return ENTRY_TYPE_MAP.get(entryType.trim().toUpperCase());
    }

    /**
     * Check if an entry type code is valid/recognized.
     */
    public static boolean isValidEntryType(String entryType) {
        return mapEntryType(entryType) != null;
    }

    /**
     * Get all valid entry type codes.
     */
    public static Set<String> getValidEntryTypes() {
        return ENTRY_TYPE_MAP.keySet();
    }
}
