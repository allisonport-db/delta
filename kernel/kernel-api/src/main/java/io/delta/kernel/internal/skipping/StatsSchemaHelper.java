package io.delta.kernel.internal.skipping;

import java.util.*;
import java.util.stream.Collectors;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import static io.delta.kernel.internal.util.ColumnMapping.getPhysicalName;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Provides information and utilities for statistics columns given a table schema. Specifically,
 * it is used to:
 * <ol>
 *     <li>Get the expected statistics schema given a table schema</li>
 *     <li>Check if a {@link Literal} or {@link Column} is skipping eligible</li>
 *     <li>Get the statistics column for a given stat type and logical column</li>
 * </ol>
 */
public class StatsSchemaHelper {

    //////////////////////////////////////////////////////////////////////////////////
    // Private static fields and methods
    //////////////////////////////////////////////////////////////////////////////////

    private static String NUM_RECORDS = "numRecords";
    private static String MIN = "minValues";
    private static String MAX = "maxValues";
    private static String NULL_COUNT = "nullCount";

    private static Set<String> skippingEligibleTypeNames = new HashSet<String>() {
        {
            add("byte");
            add("short");
            add("integer");
            add("long");
            add("float");
            add("double");
            add("date");
            add("timestamp");
            add("string");
        }
    };

    /**
     * Returns true if the given data type is eligible for MIN/MAX data skipping.
     */
    private static boolean isSkippingEligibleDataType(DataType dataType) {
        return skippingEligibleTypeNames.contains(dataType.toString()) ||
            dataType instanceof DecimalType;
    }

    /**
     * Given a data schema returns the expected schema for a min or max statistics column. This
     * means 1) replace logical names with physical names 2) set nullable=true 3) only keep stats
     * eligible fields (i.e. don't include fields with isSkippingEligibleDataType=false)
     */
    private static StructType getMinMaxStatsSchema(StructType dataSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (isSkippingEligibleDataType(field.getDataType())) {
                fields.add(new StructField(
                    getPhysicalName(field), field.getDataType(), true));
            } else if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(getPhysicalName(field),
                        getMinMaxStatsSchema((StructType) field.getDataType()), true)
                );
            }
        }
        return new StructType(fields);
    }

    /**
     * Given a data schema returns the expected schema for a null_count statistics column. This
     * means 1) replace logical names with physical names 2) set nullable=true 3) use LongType
     * for all fields
     */
    private static StructType getNullCountSchema(StructType dataSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                fields.add(
                    new StructField(getPhysicalName(field),
                        getNullCountSchema((StructType) field.getDataType()), true)
                );
            } else {
                fields.add(new StructField(
                    getPhysicalName(field), LongType.LONG, true));
            }
        }
        return new StructType(fields);
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Public static fields and methods
    //////////////////////////////////////////////////////////////////////////////////

    /**
     * Returns true if the given literal is skipping-eligible. Delta tracks min/max stats for a
     * limited set of data types and only literals of those types are skipping eligible.
     */
    public static boolean isSkippingEligibleLiteral(Literal literal) {
        return isSkippingEligibleDataType(literal.getDataType());
    }

    /**
     * Returns the expected statistics schema given a table schema.
     */
    public static StructType getStatsSchema(StructType dataSchema) {
        StructType statsSchema = new StructType()
            .add(NUM_RECORDS, LongType.LONG, true);
        StructType minMaxStatsSchema = getMinMaxStatsSchema(dataSchema);
        if (minMaxStatsSchema.length() > 0) {
            statsSchema = statsSchema
                .add(MIN, getMinMaxStatsSchema(dataSchema), true)
                .add(MAX, getMinMaxStatsSchema(dataSchema), true);
        }
        StructType nullCountSchema = getNullCountSchema(dataSchema);
        if (nullCountSchema.length() > 0) {
            statsSchema = statsSchema
                .add(NULL_COUNT, getNullCountSchema(dataSchema), true);
        }
        return statsSchema;
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Instance fields and methods
    //////////////////////////////////////////////////////////////////////////////////

    private final StructType dataSchema;
    /* Map of all leaf columns from logical to physical names */
    private final Map<Column, Column> logicalToPhysicalColumn;
    /* Set of logical leaf columns that are min/max eligible */
    private final Set<Column> availableMinMaxColumns;

    public StatsSchemaHelper(StructType dataSchema) {
        this.dataSchema = dataSchema;
        Map<Column, Tuple2<Column, DataType>> logicalToPhysicalColumnAndDataType =
            getLogicalToPhysicalColumnAndDataType(dataSchema);
        this.logicalToPhysicalColumn = logicalToPhysicalColumnAndDataType.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue()._1 // map to just the column
                )
            );
        this.availableMinMaxColumns = logicalToPhysicalColumnAndDataType.entrySet().stream()
            .filter(e -> isSkippingEligibleDataType(e.getValue()._2))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }
    
    /** Given a logical column returns corresponding the MIN column in the statistic schema */
    public Column getMinColumn(Column column) {
        checkArgument(availableMinMaxColumns.contains(column),
            String.format("%s is not a valid min column for data schema %s", column, dataSchema));
        return getStatsColumn(column, MIN);
    }

    /** Given a logical column returns corresponding the MAX column in the statistic schema */
    public Column getMaxColumn(Column column) {
        checkArgument(availableMinMaxColumns.contains(column),
            String.format("%s is not a valid min column for data schema %s", column, dataSchema));
        return getStatsColumn(column, MAX);
    }

    /**
     * Given a logical column and a stats type returns the corresponding column in the statistics
     * schema
     */
    public Column getStatsColumn(Column column, String statType) {
        checkArgument(logicalToPhysicalColumn.containsKey(column),
            String.format("%s is not a valid leaf column for data schema", column, dataSchema));
        return getChildColumn(logicalToPhysicalColumn.get(column), statType);
    }

    /**
     * Returns true if the given column is skipping-eligible using min/max statistics. This means
     * the column exists, is a leaf column, and is of a skipping-eligible data-type.
     */
    public boolean isSkippingEligibleMinMaxColumn(Column column) {
        return availableMinMaxColumns.contains(column);
    }

    /**
     * Returns true if the given column is skipping-eligible using null count statistics. This means
     * the column exists and is a leaf column as we only collect stats for leaf columns.
     */
    public boolean isSkippingEligibleNullCountColumn(Column column) {
        return logicalToPhysicalColumn.containsKey(column);
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Private helpers
    //////////////////////////////////////////////////////////////////////////////////

    /**
     * Given a data schema returns a map of {logical column -> (physical column, data type)} for
     * all leaf columns in the schema.
     */
    private Map<Column, Tuple2<Column, DataType>> getLogicalToPhysicalColumnAndDataType(
            StructType dataSchema) {
        Map<Column, Tuple2<Column, DataType>> result = new HashMap<>();
        for (StructField field : dataSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                Map<Column, Tuple2<Column, DataType>> nestedCols =
                    getLogicalToPhysicalColumnAndDataType((StructType) field.getDataType());
                for (Column childLogicalCol : nestedCols.keySet()) {
                    Column childPhysicalCol = nestedCols.get(childLogicalCol)._1;
                    DataType childColDataType = nestedCols.get(childLogicalCol)._2;
                    result.put(
                        getChildColumn(childLogicalCol, field.getName()),
                        new Tuple2<>(
                            getChildColumn(childPhysicalCol, getPhysicalName(field)),
                            childColDataType
                        ));
                }
            } else {
                result.put(
                    new Column(field.getName()),
                    new Tuple2<>(
                        new Column(getPhysicalName(field)),
                        field.getDataType()
                    )
                );
            }
        }
        return result;
    }

    /** Returns the provided column as a child column nested under {@code parentName} */
    private static Column getChildColumn(Column column, String parentName) {
        return new Column(prependArray(column.getNames(), parentName));
    }

    /**
     * Given an array {@code names} and a string element {@code preElem} return a new array with
     * {@code preElem} inserted at the beginning
     */
    private static String[] prependArray(String[] arr, String preElem) {
        String[] newNames = new String[arr.length + 1];
        newNames[0] = preElem;
        System.arraycopy(arr, 0, newNames, 1, arr.length);
        return newNames;
    }
}
