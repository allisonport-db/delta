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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

public class ReplaceTableTransactionBuilderImpl extends TransactionBuilderImpl {

  public ReplaceTableTransactionBuilderImpl(
      TableImpl table, String engineInfo, Operation operation) {
    super(table, engineInfo, operation);
  }

  // TODO validate schema and preserve maxFieldId
  //  We should allow the same fieldId reuse only if type and nullability is the same
  //  Otherwise throw an error
  //  And retain the old maxFieldId (update should happen automatically for DBI case where
  //  fieldId is provided in schema but also consider the case for no fieldId provided for schema)

  @Override
  public Transaction build(Engine engine) {
    try {
      withMaxRetries(0);
      SnapshotImpl snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
      // TODO do we want to preserve any table properties?
      return new ReplaceTableTransactionWrapper(
          buildTransactionInternal(engine, true, Optional.of(snapshot)), snapshot);
    } catch (TableNotFoundException tblf) {
      throw new RuntimeException("cannot replace table that does not exist");
    }
  }

  // All this does in (1) add all the remove files before commit and (2) remove domain metadatas
  // that are otherwise still going to persist in the txn
  class ReplaceTableTransactionWrapper implements Transaction {
    // TODO we should be able to have better abstractions for TransactionImpl but for now this
    //  wrapper is the simplest way to do this

    TransactionImpl internalTxn;
    SnapshotImpl originalSnapshot;
    Set<String> domainsAdded = new HashSet();

    ReplaceTableTransactionWrapper(TransactionImpl internalTxn, SnapshotImpl originalSnapshot) {
      this.internalTxn = internalTxn;
      this.originalSnapshot = originalSnapshot;
    }

    @Override
    public StructType getSchema(Engine engine) {
      return internalTxn.getSchema(engine);
    }

    @Override
    public List<String> getPartitionColumns(Engine engine) {
      return internalTxn.getPartitionColumns(engine);
    }

    @Override
    public long getReadTableVersion() {
      return internalTxn.getReadTableVersion();
    }

    @Override
    public Row getTransactionState(Engine engine) {
      return internalTxn.getTransactionState(engine);
    }

    @Override
    public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
        throws ConcurrentWriteException {
      // TODO enforce all dataActions are adds

      // Remove all files from table
      CloseableIterable<Row> removes = getRemoveActions(engine);

      // Remove domain metadatas as needed
      originalSnapshot
          .getDomainMetadataMap()
          .forEach(
              (k, v) -> {
                if (!v.isRemoved()) { // active domain in prev snapshot
                  // Special case for clustering since whether it's added or not in current txn
                  // depends on if clusteringColumns were set
                  if (Objects.equals(k, ClusteringMetadataDomain.DOMAIN_NAME)) {
                    // if it's clustering, only remove if no clustered columns (since cannot
                    // remove + add clustering domain in 1 txn)
                    if (!internalTxn.getClusteringColumnsSetInTxn().isPresent()) {
                      internalTxn.removeDomainMetadataInternal(k);
                    }
                    // Otherwise we will overwrite it so we should not remove
                  } else if (!domainsAdded.contains(k)) {
                    // We don't add this domain in this commit
                    internalTxn.removeDomainMetadataInternal(k);
                  }
                }
              });

      return internalTxn.commit(engine, removes.combine(dataActions));
    }

    @Override
    public void addDomainMetadata(String domain, String config) {
      domainsAdded.add(domain);
      internalTxn.addDomainMetadata(domain, config);
    }

    @Override
    public void removeDomainMetadata(String domain) {
      internalTxn.removeDomainMetadata(domain);
    }

    private CloseableIterable<Row> getRemoveActions(Engine engine) {
      final ArrayList<Row> removeRows = new ArrayList<>();

      Scan scan = originalSnapshot.getScanBuilder().build();
      try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
        scanFiles.forEachRemaining(
            batch -> {
              try (CloseableIterator<Row> scanRows = batch.getRows()) {
                scanRows.forEachRemaining(
                    scanRow -> {
                      Row removeRow = createRemoveRowFromScanRow(scanRow);
                      removeRows.add(SingleAction.createRemoveFileSingleAction(removeRow));
                    });
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return new CloseableIterable<Row>() {
        @Override
        public void close() {
          // nothing to close
        }

        @Override
        public CloseableIterator<Row> iterator() {
          return toCloseableIterator(removeRows.iterator());
        }
      };
    }

    private Row createRemoveRowFromScanRow(Row scanRow) {
      AddFile add = new AddFile(scanRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
      return add.toRemoveFileRow();
    }
  }
}
