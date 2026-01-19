#!/bin/bash
# Compare Debezium 2.x vs 3.x schemas in MariaDB
# This script generates a side-by-side comparison of data type mappings

set -e

NAMESPACE=${NAMESPACE:-dev}
MARIADB_POD=${MARIADB_POD:-dbrep-mariadb-0}
MARIADB_HOST=${MARIADB_HOST:-dbrep-mariadb.dev.svc.cluster.local}
MARIADB_USER=${MARIADB_USER:-root}
MARIADB_PASS=${MARIADB_PASS:-root_password}

TABLE_2X=${TABLE_2X:-datatype_test_v2}
TABLE_3X=${TABLE_3X:-datatype_test_v3}

# Get schema info for both tables
get_schema() {
    local table=$1
    kubectl exec $MARIADB_POD -n $NAMESPACE -- mysql -h $MARIADB_HOST -u$MARIADB_USER -p$MARIADB_PASS -N -e \
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='target_database' AND TABLE_NAME='$table' ORDER BY ORDINAL_POSITION;" 2>/dev/null
}

echo "| Oracle Column | Debezium 2.x (MariaDB) | Debezium 3.x (MariaDB) | Different? |"
echo "|---------------|------------------------|------------------------|------------|"

# Get schemas
SCHEMA_2X=$(get_schema "$TABLE_2X")
SCHEMA_3X=$(get_schema "$TABLE_3X")

if [ -z "$SCHEMA_2X" ] || [ -z "$SCHEMA_3X" ]; then
    echo "Error: Could not retrieve schema from one or both tables"
    echo "Table 2.x ($TABLE_2X): $([ -z "$SCHEMA_2X" ] && echo 'NOT FOUND' || echo 'OK')"
    echo "Table 3.x ($TABLE_3X): $([ -z "$SCHEMA_3X" ] && echo 'NOT FOUND' || echo 'OK')"
    exit 1
fi

# Create temp files
TMP_2X=$(mktemp)
TMP_3X=$(mktemp)
echo "$SCHEMA_2X" > "$TMP_2X"
echo "$SCHEMA_3X" > "$TMP_3X"

# Compare line by line
while IFS=$'\t' read -r col_name col_type_v2; do
    # Get corresponding type from 3.x
    col_type_v3=$(grep "^$col_name	" "$TMP_3X" | cut -f2)

    if [ -z "$col_type_v3" ]; then
        col_type_v3="(missing)"
    fi

    # Check if different
    if [ "$col_type_v2" = "$col_type_v3" ]; then
        diff_marker="No"
    else
        diff_marker="**Yes**"
    fi

    echo "| $col_name | \`$col_type_v2\` | \`$col_type_v3\` | $diff_marker |"
done < "$TMP_2X"

# Clean up
rm -f "$TMP_2X" "$TMP_3X"

echo ""
echo "## Summary"
echo ""

# Count differences
DIFF_COUNT=$(kubectl exec $MARIADB_POD -n $NAMESPACE -- mysql -h $MARIADB_HOST -u$MARIADB_USER -p$MARIADB_PASS -N -e "
SELECT COUNT(*) FROM (
    SELECT c1.COLUMN_NAME, c1.COLUMN_TYPE as type_v2, c2.COLUMN_TYPE as type_v3
    FROM information_schema.COLUMNS c1
    JOIN information_schema.COLUMNS c2
        ON c1.COLUMN_NAME = c2.COLUMN_NAME
    WHERE c1.TABLE_SCHEMA='target_database' AND c1.TABLE_NAME='$TABLE_2X'
        AND c2.TABLE_SCHEMA='target_database' AND c2.TABLE_NAME='$TABLE_3X'
        AND c1.COLUMN_TYPE != c2.COLUMN_TYPE
) diff;
" 2>/dev/null)

TOTAL_COLS=$(kubectl exec $MARIADB_POD -n $NAMESPACE -- mysql -h $MARIADB_HOST -u$MARIADB_USER -p$MARIADB_PASS -N -e "
SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='target_database' AND TABLE_NAME='$TABLE_2X';
" 2>/dev/null)

echo "- Total columns: $TOTAL_COLS"
echo "- Columns with type differences: $DIFF_COUNT"
echo ""

# List the actual differences
if [ "$DIFF_COUNT" -gt 0 ]; then
    echo "### Columns with Different Types"
    echo ""
    echo '```'
    kubectl exec $MARIADB_POD -n $NAMESPACE -- mysql -h $MARIADB_HOST -u$MARIADB_USER -p$MARIADB_PASS -e "
    SELECT c1.COLUMN_NAME as 'Column', c1.COLUMN_TYPE as '2.x Type', c2.COLUMN_TYPE as '3.x Type'
    FROM information_schema.COLUMNS c1
    JOIN information_schema.COLUMNS c2
        ON c1.COLUMN_NAME = c2.COLUMN_NAME
    WHERE c1.TABLE_SCHEMA='target_database' AND c1.TABLE_NAME='$TABLE_2X'
        AND c2.TABLE_SCHEMA='target_database' AND c2.TABLE_NAME='$TABLE_3X'
        AND c1.COLUMN_TYPE != c2.COLUMN_TYPE
    ORDER BY c1.ORDINAL_POSITION;
    " 2>/dev/null
    echo '```'
fi
