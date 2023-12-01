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

import java.util.HashMap;
import java.util.Map;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.VectorUtils;

/**
 * Utilities to extract information out of the scan file rows returned by
 * {@link Scan#getScanFiles(TableClient)}.
 */
public class InternalScanFileUtils {
    private InternalScanFileUtils() {}

    private static final String TABLE_ROOT_COL_NAME = "tableRoot";
    private static final DataType TABLE_ROOT_DATA_TYPE = StringType.STRING;
    /**
     * {@link Column} expression referring to the `partitionValues` in scan `add` file.
     */
    public static final Column ADD_FILE_PARTITION_COL_REF =
        new Column(new String[] {"add", "partitionValues"});

    public static StructField TABLE_ROOT_STRUCT_FIELD = new StructField(
        TABLE_ROOT_COL_NAME,
        TABLE_ROOT_DATA_TYPE,
        false /* nullable */);

    public static StructType scanFileSchema(StructType dataSchema) {
        return new StructType()
            .add("add", AddFile.getAddFileSchema(dataSchema))
            // NOTE: table root is temporary, until the path in `add.path` is converted to
            // an absolute path. https://github.com/delta-io/delta/issues/2089
            .add(TABLE_ROOT_COL_NAME, TABLE_ROOT_DATA_TYPE);
    }

    private static final int ADD_FILE_ORDINAL = 0;

    private static final int ADD_FILE_PATH_ORDINAL = 0;

    private static final int ADD_FILE_PARTITION_VALUES_ORDINAL = 1;

    private static final int ADD_FILE_SIZE_ORDINAL = 2;

    private static final int ADD_FILE_MOD_TIME_ORDINAL = 4;

    private static final int ADD_FILE_DATA_CHANGE_ORDINAL = 5;

    private static final int ADD_FILE_DV_ORDINAL = 6;

    private static final int TABLE_ROOT_ORDINAL = 1;

    /**
     * Get the {@link FileStatus} of {@code AddFile} from given scan file {@link Row}. The
     * {@link FileStatus} contains file metadata about the file.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getAddFileStatus(Row scanFileInfo) {
        Row addFile = getAddFileEntry(scanFileInfo);
        String path = addFile.getString(ADD_FILE_PATH_ORDINAL);
        Long size = addFile.getLong(ADD_FILE_SIZE_ORDINAL);
        Long modificationTime = addFile.getLong(ADD_FILE_MOD_TIME_ORDINAL);

        // TODO: this is hack until the path in `add.path` is converted to an absolute path
        String tableRoot = scanFileInfo.getString(TABLE_ROOT_ORDINAL);
        String absolutePath = new Path(tableRoot, path).toString();

        return FileStatus.of(absolutePath, size, modificationTime);
    }

    /**
     * Get the partition columns and values belonging to the {@code AddFile} from given scan file
     * row.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return Map of partition column name to partition column value.
     */
    public static Map<String, String> getPartitionValues(Row scanFileInfo) {
        Row addFile = getAddFileEntry(scanFileInfo);
        return VectorUtils.toJavaMap(addFile.getMap(ADD_FILE_PARTITION_VALUES_ORDINAL));
    }

    /**
     * Helper method to get the {@code AddFile} struct from the scan file row.
     *
     * @param scanFileInfo
     * @return {@link Row} representing the {@code AddFile}
     * @throws IllegalArgumentException If the scan file row doesn't contain {@code add} file entry.
     */
    protected static Row getAddFileEntry(Row scanFileInfo) {
        if (scanFileInfo.isNullAt(ADD_FILE_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add` entry in the scan file row");
        }
        return scanFileInfo.getStruct(ADD_FILE_ORDINAL);
    }

    /**
     * Create a scan file row conforming to the schema {@link #SCAN_FILE_SCHEMA} for
     * given file status. This is used when creating the ScanFile row for reading commit or
     * checkpoint files.
     *
     * @param fileStatus
     * @return
     */
    public static Row generateScanFileRow(FileStatus fileStatus) {
        Row addFile = new GenericRow(
            AddFile.getAddFileSchema(new StructType()),
            new HashMap<Integer, Object>() {
                {
                    put(ADD_FILE_PATH_ORDINAL, fileStatus.getPath());
                    put(ADD_FILE_PARTITION_VALUES_ORDINAL, null); // partitionValues
                    put(ADD_FILE_SIZE_ORDINAL, fileStatus.getSize());
                    put(ADD_FILE_MOD_TIME_ORDINAL, fileStatus.getModificationTime());
                    put(ADD_FILE_DATA_CHANGE_ORDINAL, null); // dataChange
                    put(ADD_FILE_DV_ORDINAL, null); // deletionVector
                }
            });

        return new GenericRow(
            scanFileSchema(new StructType()),
            new HashMap<Integer, Object>() {
                {
                    put(ADD_FILE_ORDINAL, addFile);
                    put(TABLE_ROOT_ORDINAL, "/");
                }
            });
    }

    /**
     * Create a {@link DeletionVectorDescriptor} from {@code add} entry in the given scan file row.
     *
     * @param scanFile {@link Row} representing one scan file.
     * @return
     */
    public static DeletionVectorDescriptor getDeletionVectorDescriptorFromRow(Row scanFile) {
        Row addFile = getAddFileEntry(scanFile);
        return DeletionVectorDescriptor.fromRow(addFile.getStruct(ADD_FILE_DV_ORDINAL));
    }
}
