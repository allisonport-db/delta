/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import java.io.IOException;
import java.util.*;
import static java.util.stream.Collectors.toMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.skipping.DataSkippingUtils;
import io.delta.kernel.internal.util.*;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnScanFileSchema;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Implementation of {@link Scan}
 */
public class ScanImpl implements Scan {
    private static final Logger logger = LoggerFactory.getLogger(ScanImpl.class);

    /**
     * Schema of the snapshot from the Delta log being scanned in this scan. It is a logical schema
     * with metadata properties to derive the physical schema.
     */
    private final StructType snapshotSchema;

    /** Schema that we actually want to read. */
    private final StructType readSchema;
    private final Protocol protocol;
    private final Metadata metadata;
    private final LogReplay logReplay;
    private final Path dataPath;
    private final Optional<Tuple2<Predicate, Predicate>> partitionAndDataFilters;
    // Partition column names in lower case.
    private final Set<String> partitionColumnNames;
    private boolean accessedScanFiles;

    public ScanImpl(
            StructType snapshotSchema,
            StructType readSchema,
            Protocol protocol,
            Metadata metadata,
            LogReplay logReplay,
            Optional<Predicate> filter,
            Path dataPath) {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.protocol = protocol;
        this.metadata = metadata;
        this.logReplay = logReplay;
        this.partitionColumnNames = loadPartitionColNames(); // must be called before `splitFilters`
        this.partitionAndDataFilters = splitFilters(filter);
        this.dataPath = dataPath;
    }

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    @Override
    public CloseableIterator<FilteredColumnarBatch> getScanFiles(TableClient tableClient) {
        if (accessedScanFiles) {
            throw new IllegalStateException("Scan files are already fetched from this instance");
        }
        accessedScanFiles = true;

        // Generate data skipping filter and decide if we should read the stats column
        Optional<Predicate> dataFilter = getDataSkippingPredicate();
        boolean shouldReadStats = dataFilter.isPresent();

        // Get active AddFiles via log replay
        CloseableIterator<FilteredColumnarBatch> scanFileIter = logReplay
            .getAddFilesAsColumnarBatches(shouldReadStats);

        // Apply partition pruning
        scanFileIter = applyPartitionPruning(tableClient, scanFileIter);

        // Apply data skipping
        if (shouldReadStats) {
            // there was a usable data skipping filter --> apply data skipping
            // TODO drop stats column before returning
            return applyDataSkipping(tableClient, scanFileIter, dataFilter.get());
        } else {
            return scanFileIter;
        }
    }

    @Override
    public Row getScanState(TableClient tableClient) {
        return ScanStateRow.of(
            metadata,
            protocol,
            readSchema.toJson(),
            ColumnMapping.convertToPhysicalSchema(
                readSchema,
                snapshotSchema,
                ColumnMapping.getColumnMappingMode(metadata.getConfiguration())
            ).toJson(),
            dataPath.toUri().toString());
    }

    @Override
    public Optional<Predicate> getRemainingFilter() {
        return getDataFilters();
    }

    private Optional<Tuple2<Predicate, Predicate>> splitFilters(Optional<Predicate> filter) {
        return filter.map(predicate ->
            PartitionUtils.splitMetadataAndDataPredicates(predicate, partitionColumnNames));
    }

    private Optional<Predicate> getDataFilters() {
        return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._2));
    }

    private Optional<Predicate> getPartitionsFilters() {
        return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._1));
    }

    /**
     * Consider `ALWAYS_TRUE` as no predicate.
     */
    private Optional<Predicate> removeAlwaysTrue(Optional<Predicate> predicate) {
        return predicate
            .filter(filter -> !filter.getName().equalsIgnoreCase("ALWAYS_TRUE"));
    }

    private CloseableIterator<FilteredColumnarBatch> applyPartitionPruning(
        TableClient tableClient,
        CloseableIterator<FilteredColumnarBatch> scanFileIter) {
        Optional<Predicate> partitionPredicate = getPartitionsFilters();
        if (!partitionPredicate.isPresent()) {
            // There is no partition filter, return the scan file iterator as is.
            return scanFileIter;
        }

        Set<String> partitionColNames = partitionColumnNames;
        Map<String, DataType> partitionColNameToTypeMap = metadata.getSchema().fields().stream()
            .filter(field -> partitionColNames.contains(field.getName()))
            .collect(toMap(
                field -> field.getName().toLowerCase(Locale.ENGLISH),
                field -> field.getDataType()));

        Predicate predicateOnScanFileBatch = rewritePartitionPredicateOnScanFileSchema(
            partitionPredicate.get(),
            partitionColNameToTypeMap);

        return new CloseableIterator<FilteredColumnarBatch>() {
            PredicateEvaluator predicateEvaluator = null;

            @Override
            public boolean hasNext() {
                return scanFileIter.hasNext();
            }

            @Override
            public FilteredColumnarBatch next() {
                FilteredColumnarBatch next = scanFileIter.next();
                if (predicateEvaluator == null) {
                    predicateEvaluator =
                        tableClient.getExpressionHandler().getPredicateEvaluator(
                            next.getData().getSchema(),
                            predicateOnScanFileBatch);
                }
                ColumnVector newSelectionVector = predicateEvaluator.eval(
                    next.getData(),
                    next.getSelectionVector());
                return new FilteredColumnarBatch(
                    next.getData(),
                    Optional.of(newSelectionVector));
            }

            @Override
            public void close() throws IOException {
                scanFileIter.close();
            }
        };
    }

    private Optional<Predicate> getDataSkippingPredicate() {
        Optional<Predicate> dataPredicate = getDataFilters();
        if (!dataPredicate.isPresent()) { // no data filter
            return Optional.empty();
        }
        return DataSkippingUtils.constructDataFilters(dataPredicate.get(), metadata.getSchema());
    }

    private CloseableIterator<FilteredColumnarBatch> applyDataSkipping(
        TableClient tableClient,
        CloseableIterator<FilteredColumnarBatch> scanFileIter,
        Predicate dataFilter) {
        // Get the stats schema
        // TODO prune stats schema according to the data filter
        StructType statsSchema = DataSkippingUtils.getStatsSchema(metadata.getSchema());

        // Skipping happens in two steps:
        // 1. The predicate produces false for any file whose stats prove we can safely skip it. A
        //    value of true means the stats say we must keep the file, and null means we could not
        //    determine whether the file is safe to skip, because its stats were missing/null.
        // 2. The coalesce(skip, true) converts null (= keep) to true
        Predicate filterToEval = new Predicate(
            "=",
            new ScalarExpression(
                "COALESCE",
                Arrays.asList(dataFilter, Literal.ofBoolean(true))),
            AlwaysTrue.ALWAYS_TRUE);
        logger.info(String.format("totalFilter=%s", filterToEval));

        PredicateEvaluator predicateEvaluator = tableClient
            .getExpressionHandler()
            .getPredicateEvaluator(statsSchema, filterToEval);

        return scanFileIter.map(filteredScanFileBatch -> {

            ColumnVector newSelectionVector = predicateEvaluator.eval(
                DataSkippingUtils.parseJsonStats(
                    tableClient,
                    filteredScanFileBatch,
                    statsSchema
                ),
                filteredScanFileBatch.getSelectionVector());

            return new FilteredColumnarBatch(
                filteredScanFileBatch.getData(),
                Optional.of(newSelectionVector));
            }
        );
    }

    /**
     * Helper method to load the partition column names from the metadata.
     */
    private Set<String> loadPartitionColNames() {
        ArrayValue partitionColValue = metadata.getPartitionColumns();
        ColumnVector partitionColNameVector = partitionColValue.getElements();
        Set<String> partitionColumnNames = new HashSet<>();
        for (int i = 0; i < partitionColValue.getSize(); i++) {
            checkArgument(!partitionColNameVector.isNullAt(i),
                "Expected a non-null partition column name");
            String partitionColName = partitionColNameVector.getString(i);
            checkArgument(partitionColName != null && !partitionColName.isEmpty(),
                "Expected non-null and non-empty partition column name");
            partitionColumnNames.add(partitionColName.toLowerCase(Locale.ENGLISH));
        }
        return Collections.unmodifiableSet(partitionColumnNames);
    }
}
