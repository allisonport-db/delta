package io.delta.kernel.internal.actions;

import java.net.URI;
import java.util.List;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.IcebergCompatV2Utils;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import static io.delta.kernel.internal.data.TransactionStateRow.*;
import static io.delta.kernel.internal.util.PartitionUtils.validateAndSanitizePartitionValues;

public final class GenerateIcebergCompatActionUtils {

    /**
     * Validates that only table features supported by these APIs are enabled in the table in the
     * transaction. Throws an exception if not.
     * (update these docs)
     */
    private static void validateSupportedTableFeatures(Map<String, String> config) {
        // TODO validate that IcebergCompatWriterV1 is enabled, throw an exception if not
        //  (this requires only configuration as it will have a table property set to true)
        return;
    }

    /**
     * Throws an exception if `maxRetries` was not set to 0 in the transaction.
     */
    private static void validateMaxRetriesSetToZero(Row transactionState) {
        // todo
        return;
    }

    // todo docs
    public static Row generateIcebergCompatWriterV1AddAction(
        Row transactionState,
        DataFileStatus fileStatus,
        Map<String, Literal> partitionValues,
        boolean dataChange
    ) {
        Map<String, String> config = getConfiguration(transactionState);

        /* ----- Validate that this is a valid usage of this API ----- */
        validateSupportedTableFeatures(config); // todo rename?
        validateMaxRetriesSetToZero(transactionState);

        /* ----- Validate this is valid write given the table's protocol & configurations ----- */
        if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(config)) {
            // We require field `numRecords` when icebergCompatV2 is enabled
            IcebergCompatV2Utils.validateDataFileStatus(fileStatus);
        }
        if (!getClusteringColumns(transactionState).isEmpty()) {
            // TODO when adding clustering support validate that stats are present for clustering
            //  columns here
            throw new UnsupportedOperationException("Clustering support not yet implemented");
        }

        /* --- todo ---- */
        StructType tableSchema = getLogicalSchema(transactionState);
        List<String> partitionColNames = getPartitionColumnsList(transactionState);
        partitionValues =
            validateAndSanitizePartitionValues(tableSchema, partitionColNames, partitionValues);

        // Convert partitionValues and statistics to use the physical name for column mapping tables
        ColumnMappingMode cmMode = TableConfig.COLUMN_MAPPING_MODE.fromMetadata(config);
        if (cmMode != ColumnMappingMode.NONE) {
            // TODO convert partitionValues and statistics to use the physical name
            throw new UnsupportedOperationException("Column mapping support not yet implemented");
        }
        
        URI tableRoot = new Path(getTablePath(transactionState)).toUri();

        // This takes care of relativizing the file path and serializing the file statistics
        AddFile addFile = AddFile.convertDataFileStatus(
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange
        );
        return SingleAction.createAddFileSingleAction(addFile.toRow());
    }

    public static Row generateIcebergCompatWriterV1RemoveAction(
        Row transactionState,
        DataFileStatus fileStatus,
        Map<String, Literal> partitionValues,
        boolean dataChange
    ) {
        Map<String, String> config = getConfiguration(transactionState);

        /* ----- Validate that this is a valid usage of this API ----- */
        validateSupportedTableFeatures(config); // todo rename?
        validateMaxRetriesSetToZero(transactionState);

        /* ----- Validate this is valid write given the table's protocol & configurations ----- */
        // only allow remove with dataChange=false (no remove with dataChange=true) (do at commit too)


        /* --- todo ---- */
        StructType tableSchema = getLogicalSchema(transactionState);
        List<String> partitionColNames = getPartitionColumnsList(transactionState);
        partitionValues =
            validateAndSanitizePartitionValues(tableSchema, partitionColNames, partitionValues);

        // Convert partitionValues and statistics to use the physical name for column mapping tables
        ColumnMappingMode cmMode = TableConfig.COLUMN_MAPPING_MODE.fromMetadata(config);
        if (cmMode != ColumnMappingMode.NONE) {
            // TODO convert partitionValues and statistics to use the physical name
            throw new UnsupportedOperationException("Column mapping support not yet implemented");
        }

        // todo !!!!CREATE REMOVE INSTEAD!!!
        URI tableRoot = new Path(getTablePath(transactionState)).toUri();

        // This takes care of relativizing the file path and serializing the file statistics
        AddFile addFile = AddFile.convertDataFileStatus(
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange
        );
        return SingleAction.createAddFileSingleAction(addFile.toRow());
    }

    // todo

    // tests
    // only allow remove with dataChange=false during commit when appendOnly is enabled (at commit)
    // alternatively we can do this check during action generation as well
    // validate that stats are non-null at commit as well (for icebergcompatv2 + maybe clustering?)
    // clean up and combine common code

}
