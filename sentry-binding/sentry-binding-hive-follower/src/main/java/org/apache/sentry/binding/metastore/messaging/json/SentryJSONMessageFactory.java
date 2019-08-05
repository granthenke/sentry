/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.binding.metastore.messaging.json;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.*;

import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropCatalogMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONInsertMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONOpenTxnMessage;

public class SentryJSONMessageFactory extends MessageFactory {
  private static final Log LOG = LogFactory.getLog(SentryJSONMessageFactory.class.getName());
  private static SentryJSONMessageDeserializer deserializer = new SentryJSONMessageDeserializer();

  // This class has basic information from a Partition. It is used to get a list of Partition
  // information from an iterator instead of having a list of Partition objects. Partition objects
  // may hold more information that can cause memory issues if we get a large list.
  private class PartitionBasicInfo {
    private List<Map<String, String>> partitionList = Lists.newLinkedList();
    private List<String> locations = Lists.newArrayList();

    public List<Map<String, String>> getPartitionList() {
      return partitionList;
    }

    public List<String> getLocations() {
      return locations;
    }
  }

  public SentryJSONMessageFactory() {
    LOG.info("Using SentryJSONMessageFactory for building Notification log messages ");
  }

  public MessageDeserializer getDeserializer() {
    return deserializer;
  }

  public String getVersion() {
    return "0.1";
  }

  public String getMessageFormat() {
    return "json";
  }

  @Override
  public SentryJSONCreateDatabaseMessage buildCreateDatabaseMessage(Database db) {
    return new SentryJSONCreateDatabaseMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, db,
        now(), db.getLocationUri());
  }

  @Override
  public AlterDatabaseMessage buildAlterDatabaseMessage(Database beforeDb, Database afterDb) {
    return new SentryJSONAlterDatabaseMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, beforeDb, afterDb,
        now(), beforeDb.getLocationUri(), afterDb.getLocationUri());
  }

  @Override
  public SentryJSONDropDatabaseMessage buildDropDatabaseMessage(Database db) {
    return new SentryJSONDropDatabaseMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, db.getName(),
        now(), db.getLocationUri());
  }

  @Override
  public SentryJSONCreateTableMessage buildCreateTableMessage(Table table, Iterator<String> fileIter) {
    // fileIter is used to iterate through a full list of files that partition have. This
    // may be too verbose and it is overloading the Sentry store. Sentry does not use these files
    // so it is safe to ignore them.
    return new SentryJSONCreateTableMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table, Collections.emptyIterator(), now());
  }

  @Override
  public SentryJSONAlterTableMessage buildAlterTableMessage(Table before, Table after, boolean isTruncateOp) {
    return new SentryJSONAlterTableMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, before, after, isTruncateOp, now());
  }

  @Override
  public DropTableMessage buildDropTableMessage(Table table) {
    return new SentryJSONDropTableMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), now(), table.getSd().getLocation());
  }

  @Override
  public DropPartitionMessage buildDropPartitionMessage(Table table, Iterator<Partition> partitions) {
    PartitionBasicInfo partitionBasicInfo = getPartitionBasicInfo(table, partitions);

    return new SentryJSONDropPartitionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL,
        table, partitionBasicInfo.getPartitionList(), now(), partitionBasicInfo.getLocations());
  }

  @Override
  public CreateFunctionMessage buildCreateFunctionMessage(Function function) {
    // Sentry would be not be interested in CreateFunctionMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for CreateFunctionMessage
    return new JSONCreateFunctionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, function, now());
  }

  @Override
  public DropFunctionMessage buildDropFunctionMessage(Function function) {
    // Sentry would be not be interested in DropFunctionMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for DropFunctionMessage
    return new JSONDropFunctionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, function, now());

  }

  @Override
  public InsertMessage buildInsertMessage(Table table, Partition partition, boolean replace,
                                          Iterator<String> fileIter) {
    // Sentry would be not be interested in InsertMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for InsertMessage.
    return new JSONInsertMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table, partition, replace, fileIter, now());
  }

  @Override
  public OpenTxnMessage buildOpenTxnMessage(Long fromTxnId, Long toTxnId) {
    // Sentry would be not be interested in OpenTxnMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for OpenTxnMessage.
    return new JSONOpenTxnMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, fromTxnId, toTxnId, now());
  }

  @Override
  public CommitTxnMessage buildCommitTxnMessage(Long txnId) {
    // Sentry would be not be interested in CommitTxnMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for CommitTxnMessage.
    return new JSONCommitTxnMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, txnId, now());
  }

  @Override
  public AbortTxnMessage buildAbortTxnMessage(Long txnId) {
    // Sentry would be not be interested in AbortTxnMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for AbortTxnMessage.
    return new JSONAbortTxnMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, txnId, now());
  }

  @Override
  public AllocWriteIdMessage buildAllocWriteIdMessage(List<TxnToWriteId> txnToWriteIdList,
                                                      String dbName, String tableName) {
    // Sentry would be not be interested in AllocWriteIdMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for AllocWriteIdMessage.
    return new JSONAllocWriteIdMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, txnToWriteIdList,
        dbName, tableName, now());
  }

  @Override
  public AddPrimaryKeyMessage buildAddPrimaryKeyMessage(List<SQLPrimaryKey> pks) {
    return new JSONAddPrimaryKeyMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, pks, now());
  }

  @Override
  public AddForeignKeyMessage buildAddForeignKeyMessage(List<SQLForeignKey> fks) {
    return new JSONAddForeignKeyMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, fks, now());
  }

  @Override
  public AddUniqueConstraintMessage buildAddUniqueConstraintMessage(List<SQLUniqueConstraint> uks) {
    return new JSONAddUniqueConstraintMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, uks, now());
  }

  @Override
  public AddNotNullConstraintMessage buildAddNotNullConstraintMessage(List<SQLNotNullConstraint> nns) {
    return new JSONAddNotNullConstraintMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, nns, now());
  }

  @Override
  public DropConstraintMessage buildDropConstraintMessage(String dbName, String tableName,
                                                          String constraintName) {
    return new JSONDropConstraintMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, dbName, tableName,
        constraintName, now());
  }

  @Override
  public CreateCatalogMessage buildCreateCatalogMessage(Catalog catalog) {
    return new JSONCreateCatalogMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, catalog.getName(), now());
  }

  @Override
  public DropCatalogMessage buildDropCatalogMessage(Catalog catalog) {
    return new JSONDropCatalogMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, catalog.getName(), now());
  }

  @Override
  public AlterCatalogMessage buildAlterCatalogMessage(Catalog oldCat, Catalog newCat) {
    return new JSONAlterCatalogMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, oldCat, newCat, now());
  }

  @Override
  public AddPartitionMessage buildAddPartitionMessage(Table table,
      Iterator<Partition> partitionsIterator, Iterator<PartitionFiles> partitionFileIter) {
    PartitionBasicInfo partitionBasicInfo = getPartitionBasicInfo(table, partitionsIterator);

    // partitionFileIter is used to iterate through a full list of files that partition have. This
    // may be too verbose and it is overloading the Sentry store. Sentry does not use these files
    // so it is safe to ignore them.
    return new SentryJSONAddPartitionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table,
        partitionsIterator, Collections.emptyIterator(), now(), partitionBasicInfo.getLocations());
  }

  @Override
  public AlterPartitionMessage buildAlterPartitionMessage(Table table, Partition before, Partition after,
                                                          boolean isTruncateOp) {
    return new SentryJSONAlterPartitionMessage(MS_SERVER_URL, MS_SERVICE_PRINCIPAL, table,
        before, after, isTruncateOp, now());
  }

  private PartitionBasicInfo getPartitionBasicInfo(Table table, Iterator<Partition> iterator) {
    PartitionBasicInfo partitionBasicInfo = new PartitionBasicInfo();
    while(iterator.hasNext()) {
      Partition partition = iterator.next();

      partitionBasicInfo.getPartitionList().add(getPartitionKeyValues(table, partition));
      partitionBasicInfo.getLocations().add(partition.getSd().getLocation());
    }

    return partitionBasicInfo;
  }

  private static Map<String, String> getPartitionKeyValues(Table table, Partition partition) {
    LinkedHashMap partitionKeys = new LinkedHashMap();

    for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
      partitionKeys.put((table.getPartitionKeys().get(i)).getName(), partition.getValues().get(i));
    }

    return partitionKeys;
  }

  //This is private in parent class
  private long now() {
    return System.currentTimeMillis() / 1000L;
  }
}
