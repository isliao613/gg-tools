package com.example.kafka.connect.iidr.util;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.logging.Logger;

/**
 * Converts A_TIMSTAMP format (yyyy-MM-dd HH:mm:ss.SSSSSSSSSSSS) to ISO8601.
 *
 * The A_TIMSTAMP is not ISO8601 compliant. This class interprets it using
 * a configured timezone and converts to standard OffsetDateTime.
 *
 * Example:
 * - Input: "2025-01-22 11:17:14.000000000000"
 * - Timezone: "Asia/Taipei" (+08:00)
 * - Output: 2025-01-22T11:17:14.000000+08:00
 */
public class TimestampConverter {

    private static final Logger log = Logger.getLogger(TimestampConverter.class.getName());

    // Format: yyyy-MM-dd HH:mm:ss.SSSSSSSSS (9 decimal places - nanoseconds)
    private static final DateTimeFormatter INPUT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    private final ZoneId sourceZone;

    public TimestampConverter(String timezone) {
        this.sourceZone = parseTimezone(timezone);
        log.info("TimestampConverter initialized with timezone: " + timezone + " (" + sourceZone + ")");
    }

    private ZoneId parseTimezone(String timezone) {
        if (timezone == null || timezone.trim().isEmpty()) {
            return ZoneId.of("UTC");
        }

        String tz = timezone.trim();

        // Handle offset format like "+08:00" or "-05:00"
        if (tz.startsWith("+") || tz.startsWith("-")) {
            try {
                ZoneOffset offset = ZoneOffset.of(tz);
                return ZoneId.from(offset);
            } catch (Exception e) {
                log.warning("Failed to parse timezone offset '" + tz + "', defaulting to UTC");
                return ZoneId.of("UTC");
            }
        }

        // Handle named timezone like "Asia/Taipei" or "UTC"
        try {
            return ZoneId.of(tz);
        } catch (Exception e) {
            log.warning("Failed to parse timezone '" + tz + "', defaulting to UTC");
            return ZoneId.of("UTC");
        }
    }

    /**
     * Convert A_TIMSTAMP string to OffsetDateTime.
     *
     * @param timestamp The A_TIMSTAMP value (format: yyyy-MM-dd HH:mm:ss.SSSSSSSSSSSS)
     * @return OffsetDateTime with the configured timezone, or null if parsing fails
     */
    public OffsetDateTime convert(String timestamp) {
        if (timestamp == null || timestamp.trim().isEmpty()) {
            return null;
        }

        try {
            // Truncate to 9 decimal places (nanoseconds) if longer
            String truncated = truncateNanos(timestamp.trim());

            // Parse as LocalDateTime (no timezone info in input)
            LocalDateTime localDateTime = LocalDateTime.parse(truncated, INPUT_FORMATTER);

            // Apply configured timezone
            return localDateTime.atZone(sourceZone).toOffsetDateTime();

        } catch (DateTimeParseException e) {
            log.warning("Failed to parse A_TIMSTAMP '" + timestamp + "': " + e.getMessage());
            return null;
        }
    }

    /**
     * Truncate nanosecond precision from 12 digits to 9 digits.
     */
    private String truncateNanos(String timestamp) {
        int dotIndex = timestamp.lastIndexOf('.');
        if (dotIndex == -1) {
            return timestamp + ".000000000";
        }

        String fraction = timestamp.substring(dotIndex + 1);
        if (fraction.length() > 9) {
            return timestamp.substring(0, dotIndex + 1) + fraction.substring(0, 9);
        } else if (fraction.length() < 9) {
            // Pad with zeros
            StringBuilder padded = new StringBuilder(timestamp);
            for (int i = fraction.length(); i < 9; i++) {
                padded.append('0');
            }
            return padded.toString();
        }
        return timestamp;
    }

    /**
     * Convert and format to ISO8601 string.
     */
    public String convertToIso8601(String timestamp) {
        OffsetDateTime odt = convert(timestamp);
        return odt != null ? odt.toString() : null;
    }
}
