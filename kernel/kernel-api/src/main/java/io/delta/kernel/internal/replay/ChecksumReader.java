/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import java.util.Optional;
import java.util.regex.Pattern;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.fs.Path;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

/**
 * TODO
 */
public class ChecksumReader {

    // private static final Pattern DELTA_FILE_PATTERN = Pattern.compile("\\d+\\.json");
    private static final Pattern CHECKSUM_FILE_PATTERN = Pattern.compile("\\d+\\.crc");

    private static Path checksumFile(Path path, long version) {
        return new Path(path, String.format("%020d.crc", version));
    }

    public static Optional<VersionStats> getVersionStats(
            Path logPath, long version, Engine engine) { // checksumFileReconciliationWindow

        // For now, read just the requested version CRC
        return readChecksumFile(checksumFile(logPath, version), engine);

        // TODO read in the checksumFileReconciliationWindow and reconcile if found
        //   use SnapshotHint as a lowerbound such that it becomes (availableCRC, snapshotHint)
    }

    private static Optional<VersionStats> readChecksumFile(Path filePath, Engine engine) {
        try {
            FileStatus fs = FileStatus.of(filePath.toString(), 0 /* size */, 0 /* modTime */);
            try (CloseableIterator<ColumnarBatch> iter = engine.getJsonHandler().readJsonFiles(
                singletonCloseableIterator(fs),
                VersionStats.FULL_SCHEMA,
                Optional.empty())
            ) {
                // We do this instead of iterating through the rows or using getSingularRow so we
                // can use the existing fromColumnVector methods in Protocol, Metadata, Format etc
                if (!iter.hasNext()) {
                    throw new RuntimeException("Expected at least one batch returned");
                }
                ColumnarBatch batch = iter.next();

                if (batch.getSize() != 1) {
                    throw new RuntimeException("Expected a batch of size 1");
                }
                return Optional.of(VersionStats.fromColumnarBatch(batch, 0, engine));
            }
        } catch (Exception e) {
            // This can happen when the version does not have a checksum file
            // TODO log that we saw an exception
            return Optional.empty();
        }
    }
}
