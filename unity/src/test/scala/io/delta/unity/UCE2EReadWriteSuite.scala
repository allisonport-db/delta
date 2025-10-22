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

package io.delta.unity

import java.net.URI
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.{CommitRange, Operation, Snapshot, Transaction}
import io.delta.kernel.TransactionSuite.longVector
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.{TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.model.{GenerateTemporaryTableCredential, TableOperation, TemporaryCredentials}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off
class UCE2EReadWriteSuite extends AnyFunSuite {
  val baseUri = "https://e2-dogfood.staging.cloud.databricks.com/"
  val token = "XXX"

  /** Creates a new Engine instance with credentials configured for the given storage location. */
  private def createEngineWithCredentials(credentials: TemporaryCredentials): Engine = {
    val conf = new Configuration()
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.access.key", credentials.getAwsTempCredentials.getAccessKeyId)
    conf.set("fs.s3a.secret.key", credentials.getAwsTempCredentials.getSecretAccessKey)
    conf.set("fs.s3a.session.token", credentials.getAwsTempCredentials.getSessionToken)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3.impl.disable.cache", "true")
    conf.set("fs.s3a.impl.disable.cache", "true")
    DefaultEngine.create(conf)
  }

  private def getUcApiClient(): ApiClient = {
    val parsedUri = new URI(baseUri)
    new ApiClient()
      .setScheme(parsedUri.getScheme())
      .setHost(parsedUri.getHost())
      .setPort(parsedUri.getPort())
      .setRequestInterceptor(request => request.header("Authorization", "Bearer " + token))
  }

  private def scanAllRows(engine: Engine, snapshot: Snapshot): Seq[Row] = {
    val scan = snapshot.getScanBuilder().build()
    val scanFiles = scan.getScanFiles(engine)
    val outputRows = scala.collection.mutable.ArrayBuffer[Row]()

    try {
      scanFiles.asScala.foreach { fileColumnarBatch =>
        fileColumnarBatch.getRows.asScala.foreach { scanFileRow =>
          val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)
          val physicalDataReadSchema =
            ScanStateRow.getPhysicalDataReadSchema(scan.getScanState(engine))
          val physicalDataIter = engine.getParquetHandler.readParquetFiles(
            io.delta.kernel.internal.util.Utils.singletonCloseableIterator(fileStatus),
            physicalDataReadSchema,
            Optional.empty()).map(_.getData)

          val dataBatches = io.delta.kernel.Scan.transformPhysicalData(
            engine,
            scan.getScanState(engine),
            scanFileRow,
            physicalDataIter)

          try {
            dataBatches.asScala.foreach { batch =>
              val data = batch.getData
              val selectionVector = batch.getSelectionVector
              val rowIter = data.getRows
              try {
                var i = 0
                while (rowIter.hasNext) {
                  val row = rowIter.next()
                  if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) {
                    outputRows += row
                  }
                  i += 1
                }
              } finally {
                rowIter.close()
              }
            }
          } finally {
            dataBatches.close()
          }
        }
      }
    } finally {
      scanFiles.close()
    }

    outputRows.toSeq
  }

  private def loadLatestSnapshotAndPrint(
      engine: Engine,
      ucCatalogManagedClient: UCCatalogManagedClient,
      ucTableId: String,
      tablePath: String): Snapshot = {
    val latestSnapshot = ucCatalogManagedClient
      .loadSnapshot(engine, ucTableId, tablePath, Optional.empty(), Optional.empty())
      .asInstanceOf[SnapshotImpl]
    println(s"version: ${latestSnapshot.getVersion}")
    println(s"schema: ${latestSnapshot.getSchema}")
    println(s"protocol: ${latestSnapshot.getProtocol}")
    scanAllRows(engine, latestSnapshot).foreach { row => println(TestRow(row)) }
    latestSnapshot
  }

  private def loadSnapshotAtTimestampAndPrint(
      engine: Engine,
      ucCatalogManagedClient: UCCatalogManagedClient,
      ucTableId: String,
      tablePath: String,
      timestamp: Long): Snapshot = {
    val latestSnapshot = ucCatalogManagedClient
      .loadSnapshot(engine, ucTableId, tablePath, Optional.empty(), Optional.of(timestamp))
      .asInstanceOf[SnapshotImpl]
    println(s"version: ${latestSnapshot.getVersion}")
    println(s"schema: ${latestSnapshot.getSchema}")
    println(s"protocol: ${latestSnapshot.getProtocol}")
    scanAllRows(engine, latestSnapshot).foreach { row => println(TestRow(row)) }
    latestSnapshot
  }

  private def loadCommitRangeAndPrint(
      engine: Engine,
      ucCatalogManagedClient: UCCatalogManagedClient,
      ucTableId: String,
      tablePath: String,
      startVersion: Optional[java.lang.Long] = Optional.empty(),
      startTimestamp: Optional[java.lang.Long] = Optional.empty(),
      endVersion: Optional[java.lang.Long] = Optional.empty(),
      endTimestamp: Optional[java.lang.Long] = Optional.empty()): CommitRange = {
    val commitRange = ucCatalogManagedClient
      .loadCommitRange(
        engine,
        ucTableId,
        tablePath,
        startVersion,
        startTimestamp,
        endVersion,
        endTimestamp)
    println(s"startVersion: ${commitRange.getStartVersion}")
    println(s"endVersion: ${commitRange.getEndVersion}")
    val startSnapshot = ucCatalogManagedClient
      .loadSnapshot(
        engine,
        ucTableId,
        tablePath,
        Optional.of(commitRange.getStartVersion),
        Optional.empty())
    val actions = commitRange
      .getActions(
        engine,
        startSnapshot,
        Set(
          DeltaAction.ADD,
          DeltaAction.COMMITINFO,
          DeltaAction.METADATA,
          DeltaAction.PROTOCOL).asJava)
    val outputRows = scala.collection.mutable.ArrayBuffer[Row]()
    actions.forEachRemaining(batch => {
      batch.getRows.forEachRemaining(outputRows.append(_))
    })
    println(s"actions (version, timestamp, add, commitInfo, metadata, protocol):")
    outputRows.foreach { row => println(TestRow(row)) }
    commitRange
  }

  private def writeDataAndCommit(
      engine: Engine,
      snapshot: Snapshot,
      lowInc: Int,
      highInc: Int): Unit = {
    println(
      s"Attempting to write data + commit: snapshotVersion = s${snapshot.getVersion}, data = [$lowInc, $highInc]")

    val schema = snapshot.getSchema
    val txn = snapshot.buildUpdateTableTransaction("custom", Operation.WRITE).build(engine)
    val txnStateRow = txn.getTransactionState(engine)
    val col1Vector = longVector((lowInc.toLong to highInc.toLong).map(java.lang.Long.valueOf).toSeq)
    val columnarBatchData =
      new DefaultColumnarBatch(highInc - lowInc + 1, schema, Array(col1Vector))
    val filteredColumnarBatchData = new FilteredColumnarBatch(columnarBatchData, Optional.empty())
    val partitionValues = Collections.emptyMap[String, Literal]()

    val physicalDataIter = Transaction.transformLogicalData(
      engine,
      txnStateRow,
      toCloseableIterator(Seq(filteredColumnarBatchData).toIterator.asJava),
      partitionValues)

    println("Created physicalDataIter")

    val writeContext = Transaction.getWriteContext(engine, txnStateRow, partitionValues)

    val writeResultIter = engine
      .getParquetHandler
      .writeParquetFiles(
        writeContext.getTargetDirectory,
        physicalDataIter,
        writeContext.getStatisticsColumns)

    println("Created writeResultIter")

    val addRowsIter =
      Transaction.generateAppendActions(engine, txnStateRow, writeResultIter, writeContext)

    println("Created addRowsIter")

    val result = txn.commit(engine, CloseableIterable.inMemoryIterable(addRowsIter))

    println(s"committed version: ${result.getVersion}")
    println("Commit SUCCESS!!!!")
  }

  test("basic write") {
    // ========== Credential and Client Setup ==========
    val ucApiClient = getUcApiClient()
    val tablesApi = new TablesApi(ucApiClient)
    val tableInfo = tablesApi.getTable("scott.main.ccv2_test_kkk")
    val ucTableId = tableInfo.getTableId
    val tablePath = tableInfo.getStorageLocation
    val temporaryCredentialsApi = new TemporaryCredentialsApi(ucApiClient)
    val temporaryCredentials = temporaryCredentialsApi
      .generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
          .tableId(ucTableId).operation(TableOperation.READ_WRITE))
    val ucDeltaStorageClient = new UCTokenBasedRestClient(baseUri, token)
    val engine = createEngineWithCredentials(temporaryCredentials)
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucDeltaStorageClient)

    // ========== READ ==========
    var latestSnapshot =
      loadLatestSnapshotAndPrint(engine, ucCatalogManagedClient, ucTableId, tablePath)

    // ========== WRITE ==========
    writeDataAndCommit(engine, latestSnapshot, 100, 109)

    // ========== READ ==========
    latestSnapshot =
      loadLatestSnapshotAndPrint(engine, ucCatalogManagedClient, ucTableId, tablePath)

    // ========== WRITE ==========
    writeDataAndCommit(engine, latestSnapshot, 110, 119)

    // ========== READ ==========
    latestSnapshot =
      loadLatestSnapshotAndPrint(engine, ucCatalogManagedClient, ucTableId, tablePath)
  }

  test("basic time-travel by-ts") {
    val ucApiClient = getUcApiClient()
    val tablesApi = new TablesApi(ucApiClient)
    val tableInfo = tablesApi.getTable("alli.main.ccv2_test_tt_by_ts")
    val ucTableId = tableInfo.getTableId
    val tablePath = tableInfo.getStorageLocation
    val temporaryCredentialsApi = new TemporaryCredentialsApi(ucApiClient)
    val temporaryCredentials = temporaryCredentialsApi
      .generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
          .tableId(ucTableId).operation(TableOperation.READ_WRITE))
    val ucDeltaStorageClient = new UCTokenBasedRestClient(baseUri, token)
    val engine = createEngineWithCredentials(temporaryCredentials)
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucDeltaStorageClient)

    // Table should be empty!
    val tsBetweenV0AndV1 = 1758584629000L
    loadSnapshotAtTimestampAndPrint(
      engine,
      ucCatalogManagedClient,
      ucTableId,
      tablePath,
      tsBetweenV0AndV1)

    // Table should have 2 rows col1 = [1,2]
    val tsBetweenV1AndV2 = 1758584951000L
    loadSnapshotAtTimestampAndPrint(
      engine,
      ucCatalogManagedClient,
      ucTableId,
      tablePath,
      tsBetweenV1AndV2)
  }

  test("basic commitRange") {
    val ucApiClient = getUcApiClient()
    val tablesApi = new TablesApi(ucApiClient)
    val tableInfo = tablesApi.getTable("alli.main.ccv2_test_tt_by_ts")
    val ucTableId = tableInfo.getTableId
    val tablePath = tableInfo.getStorageLocation
    val temporaryCredentialsApi = new TemporaryCredentialsApi(ucApiClient)
    val temporaryCredentials = temporaryCredentialsApi
      .generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
          .tableId(ucTableId).operation(TableOperation.READ_WRITE))
    val ucDeltaStorageClient = new UCTokenBasedRestClient(baseUri, token)
    val engine = createEngineWithCredentials(temporaryCredentials)
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucDeltaStorageClient)

    val tsBetweenV0AndV1 = 1758584629000L
    val tsBetweenV1AndV2 = 1758584951000L

    // Default start and end boundaries (should be 0, 2 with 2 add files)
    loadCommitRangeAndPrint(engine, ucCatalogManagedClient, ucTableId, tablePath)

    // Version-based travel  (should be 0, 1 with 1 add file)
    loadCommitRangeAndPrint(
      engine,
      ucCatalogManagedClient,
      ucTableId,
      tablePath,
      startVersion = Optional.of(0),
      endVersion = Optional.of(1))

    // Timestamp-based travel (should be 1, 1 with 1 add file)
    loadCommitRangeAndPrint(
      engine,
      ucCatalogManagedClient,
      ucTableId,
      tablePath,
      startTimestamp = Optional.of(tsBetweenV0AndV1),
      endTimestamp = Optional.of(tsBetweenV1AndV2))

    // Mixed version + timestamp based travel (should be 1, 2 with 2 add file)
    loadCommitRangeAndPrint(
      engine,
      ucCatalogManagedClient,
      ucTableId,
      tablePath,
      startTimestamp = Optional.of(tsBetweenV0AndV1),
      endVersion = Optional.of(2))

    // Version doesn't exist (error case)
    val e = intercept[Exception] {
      loadCommitRangeAndPrint(
        engine,
        ucCatalogManagedClient,
        ucTableId,
        tablePath,
        endVersion = Optional.of(5))
    }
    println(e)
  }
}
