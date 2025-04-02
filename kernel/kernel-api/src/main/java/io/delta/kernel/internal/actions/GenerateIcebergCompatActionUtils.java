/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.data.TransactionStateRow.*;
import static io.delta.kernel.internal.util.InternalUtils.relativizePath;
import static io.delta.kernel.internal.util.PartitionUtils.serializePartitionMap;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** TODO docs (what is okay to say here?) */
public final class GenerateIcebergCompatActionUtils {

  // TODO reorg private vs public methods

  /**
   * Validates that table feature `icebergWriterCompatV1` is enabled. We restrict usage of these
   * APIs to require that this table feature is enabled to prevent any unsafe usage due to the table
   * features that are blocked via `icebergWriterCompatV1` (for example, rowTracking or
   * deletionVectors).
   */
  private static void validateIcebergWriterCompatV1Enabled(Map<String, String> config) {
    if (!TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(config)) {
      throw new UnsupportedOperationException(
          // TODO use the key here
          "APIs within GenerateIcebergCompatActionUtils are only supported on tables with"
              + " 'delta.enableIcebergWriterCompatV1' set to true");
    }
  }

  /**
   * Throws an exception if `maxRetries` was not set to 0 in the transaction. We restrict these APIs
   * to require `maxRetries = 0` since conflict resolution is not supported for operations other
   * than blind appends.
   */
  private static void validateMaxRetriesSetToZero(Row transactionState) {
    if (getMaxRetries(transactionState) > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Usage of GenerateIcebergCompatActionUtils requires maxRetries=0, "
                  + "found maxRetries=%s",
              getMaxRetries(transactionState)));
    }
  }

  // TODO docs
  public static Row generateIcebergCompatWriterV1AddAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Map<String, String> config = getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV1Enabled(config);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // note -- we know this must be enabled since IcebergCompatWriterV1 is enabled
    if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(config)) {
      // We require field `numRecords` when icebergCompatV2 is enabled
      IcebergCompatV2MetadataValidatorAndUpdater.validateDataFileStatus(fileStatus);
    }
    if (!getClusteringColumns(transactionState).isEmpty()) {
      // TODO when adding clustering support validate that stats are present for clustering
      //  columns here
      throw new UnsupportedOperationException("Clustering support not yet implemented");
    }

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    if (!getPartitionColumnsList(transactionState).isEmpty()) {
      throw new UnsupportedOperationException(
          "Currently GenerateIcebergCompatActionUtils "
              + "is not supported for partitioned tables");
    }
    checkArgument(
        partitionValues.isEmpty(), "Non-empty partitionValues provided for an unpartitioned table");

    URI tableRoot = new Path(getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    // (including converting from fieldId -> physical name)
    AddFile addFile =
        AddFile.convertDataFileStatus(
            TransactionStateRow.getPhysicalSchema(transactionState),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange);
    return SingleAction.createAddFileSingleAction(addFile.toRow());
  }

  // TODO docs
  public static Row generateIcebergCompatWriterV1RemoveAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Map<String, String> config = getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV1Enabled(config);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // We only allow removes with dataChange=false when appendOnly=true
    if (dataChange && TableConfig.APPEND_ONLY_ENABLED.fromMetadata(config)) {
      throw DeltaErrors.cannotModifyAppendOnlyTable(getTablePath(transactionState));
    }

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    if (!getPartitionColumnsList(transactionState).isEmpty()) {
      throw new UnsupportedOperationException(
          "Currently GenerateIcebergCompatActionUtils "
              + "is not supported for partitioned tables");
    }
    checkArgument(
        partitionValues.isEmpty(), "Non-empty partitionValues provided for an unpartitioned table");

    URI tableRoot = new Path(getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    // (including converting from fieldId -> physical name)
    Row removeFileRow =
        convertRemoveDataFileStatus(
            TransactionStateRow.getPhysicalSchema(transactionState),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange);
    return SingleAction.createRemoveFileSingleAction(removeFileRow);
  }

  //////////////////////////////////////////////////
  // Private methods for creating RemoveFile rows //
  //////////////////////////////////////////////////
  // I've added these APIs here since they rely on the assumptions validated within
  // GenerateIcebergCompatActionUtils such as icebergWriterCompatV1 is enabled --> rowTracking is
  // disabled. Since these APIs are not valid without these assumptions, holding off on putting them
  // within RemoveFile.java until we add full support for deletes (which will likely involve
  // generating RemoveFiles directly from AddFiles anyway)

  private static Row createRemoveFileRowWithExtendedFileMetadata(
      String path,
      long deletionTimestamp,
      boolean dataChange,
      MapValue partitionValues,
      long size,
      Optional<DataFileStatistics> stats,
      StructType physicalSchema) {
    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("path"), requireNonNull(path));
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("deletionTimestamp"), deletionTimestamp);
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("dataChange"), dataChange);
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("extendedFileMetadata"), true);
    fieldMap.put(
        RemoveFile.FULL_SCHEMA.indexOf("partitionValues"), requireNonNull(partitionValues));
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("size"), size);
    stats.ifPresent(
        stat ->
            fieldMap.put(
                RemoveFile.FULL_SCHEMA.indexOf("stats"), stat.serializeAsJson(physicalSchema)));
    return new GenericRow(RemoveFile.FULL_SCHEMA, fieldMap);
  }

  private static Row convertRemoveDataFileStatus(
      StructType physicalSchema,
      URI tableRoot,
      DataFileStatus dataFileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    return createRemoveFileRowWithExtendedFileMetadata(
        relativizePath(new Path(dataFileStatus.getPath()), tableRoot).toUri().toString(),
        dataFileStatus.getModificationTime(),
        dataChange,
        serializePartitionMap(partitionValues),
        dataFileStatus.getSize(),
        dataFileStatus.getStatistics(),
        physicalSchema);
  }
}
