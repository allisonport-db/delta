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

package io.delta.kernel.internal.replay;

import java.io.IOException;
import java.util.List;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Tuple2;

/**
 * Replays a history of actions, resolving them to produce the current state of the table. The
 * protocol for resolution is as follows:
 *  - The most recent {@code AddFile} and accompanying metadata for any `(path, dv id)` tuple wins.
 *  - {@code RemoveFile} deletes a corresponding AddFile. A {@code RemoveFile} "corresponds" to
 *    the AddFile that matches both the parquet file URI *and* the deletion vector's URI (if any).
 *  - The most recent {@code Metadata} wins.
 *  - The most recent {@code Protocol} version wins.
 *  - For each `(path, dv id)` tuple, this class should always output only one {@code FileAction}
 *    (either {@code AddFile} or {@code RemoveFile})
 *
 * This class exposes the following public APIs
 * - {@link #getProtocol()}: latest non-null Protocol
 * - {@link #getMetadata()}: latest non-null Metadata
 * - {@link #getAddFilesAsColumnarBatches}: return all active (not tombstoned) AddFiles as
 *                                          {@link ColumnarBatch}s
 */
public class LogReplay {

    /** Read schema when searching for the latest Protocol and Metadata. */
    public static final StructType PROTOCOL_METADATA_READ_SCHEMA = new StructType()
        .add("protocol", Protocol.READ_SCHEMA)
        .add("metaData", Metadata.READ_SCHEMA);

    private static StructType getAddSchema(boolean shouldReadStats) {
        return shouldReadStats ? AddFile.SCHEMA_WITH_STATS :
            AddFile.SCHEMA_WITHOUT_STATS;
    }

    public static StructType getAddRemoveReadSchema(boolean shouldReadStats) {
        return new StructType()
            .add("add", getAddSchema(shouldReadStats))
            .add("remove", new StructType()
                .add("path", StringType.STRING, false /* nullable */)
                .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
            );
    }

    public static int ADD_FILE_ORDINAL = 0;
    public static int ADD_FILE_PATH_ORDINAL = 0;
    public static int ADD_FILE_DV_ORDINAL = 5;

    public static int REMOVE_FILE_ORDINAL = 1;
    public static int REMOVE_FILE_PATH_ORDINAL = 0;
    public static int REMOVE_FILE_DV_ORDINAL = 1;

    private final Path dataPath;
    private final LogSegment logSegment;
    private final TableClient tableClient;

    private final Tuple2<Protocol, Metadata> protocolAndMetadata;

    public LogReplay(
            Path logPath,
            Path dataPath,
            TableClient tableClient,
            LogSegment logSegment) {
        assertLogFilesBelongToTable(logPath, logSegment.allLogFilesUnsorted());

        this.dataPath = dataPath;
        this.logSegment = logSegment;
        this.tableClient = tableClient;
        this.protocolAndMetadata = loadTableProtocolAndMetadata();
    }

    /////////////////
    // Public APIs //
    /////////////////

    public Protocol getProtocol() {
        return this.protocolAndMetadata._1;
    }

    public Metadata getMetadata() {
        return this.protocolAndMetadata._2;
    }

    /**
     * Returns an iterator of {@link FilteredColumnarBatch} with schema
     * {@link #ADD_ONLY_DATA_SCHEMA} representing all the active AddFiles in the table
     */
    public CloseableIterator<FilteredColumnarBatch> getAddFilesAsColumnarBatches(
        boolean shouldReadStats) {

        final CloseableIterator<Tuple2<FileDataReadResult, Boolean>> addRemoveIter =
            new ActionsIterator(
                tableClient,
                logSegment.allLogFilesReversed(),
                getAddRemoveReadSchema(shouldReadStats));
        return new ActiveAddFilesIterator(tableClient, addRemoveIter, dataPath);
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    private Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata() {
        Protocol protocol = null;
        Metadata metadata = null;

        try (CloseableIterator<Tuple2<FileDataReadResult, Boolean>> reverseIter =
                 new ActionsIterator(
                     tableClient,
                     logSegment.allLogFilesReversed(),
                     PROTOCOL_METADATA_READ_SCHEMA)) {
            while (reverseIter.hasNext()) {
                final ColumnarBatch columnarBatch = reverseIter.next()._1.getData();

                assert(columnarBatch.getSchema().equals(PROTOCOL_METADATA_READ_SCHEMA));

                if (protocol == null) {
                    final ColumnVector protocolVector = columnarBatch.getColumnVector(0);

                    for (int i = 0; i < protocolVector.getSize(); i++) {
                        if (!protocolVector.isNullAt(i)) {
                            protocol = Protocol.fromColumnVector(protocolVector, i);

                            if (metadata != null) {
                                // Stop since we have found the latest Protocol and Metadata.
                                validateSupportedTable(protocol, metadata);
                                return new Tuple2<>(protocol, metadata);
                            }

                            break; // We already found the protocol, exit this for-loop
                        }
                    }
                }
                if (metadata == null) {
                    final ColumnVector metadataVector = columnarBatch.getColumnVector(1);

                    for (int i = 0; i < metadataVector.getSize(); i++) {
                        if (!metadataVector.isNullAt(i)) {
                            metadata = Metadata.fromColumnVector(metadataVector, i, tableClient);

                            if (protocol != null) {
                                // Stop since we have found the latest Protocol and Metadata.
                                validateSupportedTable(protocol, metadata);
                                return new Tuple2<>(protocol, metadata);
                            }

                            break; // We already found the metadata, exit this for-loop
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        if (protocol == null) {
            throw new IllegalStateException(
                String.format("No protocol found at version %s", logSegment.version)
            );
        }

        throw new IllegalStateException(
            String.format("No metadata found at version %s", logSegment.version)
        );
    }

    private void validateSupportedTable(Protocol protocol, Metadata metadata) {
        switch (protocol.getMinReaderVersion()) {
            case 1:
                break;
            case 2:
                verifySupportedColumnMappingMode(metadata);
                break;
            case 3:
                List<String> readerFeatures = protocol.getReaderFeatures();
                for (String readerFeature : readerFeatures) {
                    switch (readerFeature) {
                        case "deletionVectors":
                            break;
                        case "columnMapping":
                            verifySupportedColumnMappingMode(metadata);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                "Unsupported table feature: " + readerFeature);
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reader protocol version: " + protocol.getMinReaderVersion());
        }
    }

    private void verifySupportedColumnMappingMode(Metadata metadata) {
        // Check if the mode is name. Id mode is not yet supported
        String cmMode = metadata.getConfiguration()
                .getOrDefault("delta.columnMapping.mode", "none");
        if (!"none".equalsIgnoreCase(cmMode) &&
            !"name".equalsIgnoreCase(cmMode)) {
            throw new UnsupportedOperationException(
                "Unsupported column mapping mode: " + cmMode);
        }
    }

    /**
     * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
     * Visible only for testing.
     */
    protected static void assertLogFilesBelongToTable(Path logPath, List<FileStatus> allFiles) {
        String logPathStr = logPath.toString(); // fully qualified path
        for (FileStatus fileStatus : allFiles) {
            String filePath = fileStatus.getPath();
            if (!filePath.startsWith(logPathStr)) {
                throw new RuntimeException("File (" + filePath + ") doesn't belong in the " +
                    "transaction log at " + logPathStr + ".");
            }
        }
    }
}
