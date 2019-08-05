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

import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONInsertMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONOpenTxnMessage;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

public class SentryJSONMessageDeserializer extends MessageDeserializer {
  private static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public SentryJSONMessageDeserializer() {
  }

  /**
   * Method to de-serialize CreateDatabaseMessage instance.
   */
  @Override
  public SentryJSONCreateDatabaseMessage getCreateDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONCreateDatabaseMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONCreateDatabaseMessage: ", e);
    }
  }

  @Override
  public SentryJSONAlterDatabaseMessage getAlterDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAlterDatabaseMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAlterDatabaseMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropDatabaseMessage instance.
   */
  @Override
  public SentryJSONDropDatabaseMessage getDropDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONDropDatabaseMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONDropDatabaseMessage: ", e);
    }
  }

  /**
   * Method to de-serialize CreateTableMessage instance.
   */
  @Override
  public SentryJSONCreateTableMessage getCreateTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONCreateTableMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONCreateTableMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AlterTableMessage instance.
   */
  @Override
  public SentryJSONAlterTableMessage getAlterTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAlterTableMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAlterTableMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropTableMessage instance.
   */
  @Override
  public SentryJSONDropTableMessage getDropTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONDropTableMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONDropTableMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AddPartitionMessage instance.
   */
  @Override
  public SentryJSONAddPartitionMessage getAddPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAddPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAddPartitionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AlterPartitionMessage instance.
   */
  @Override
  public SentryJSONAlterPartitionMessage getAlterPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAlterPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAlterPartitionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropPartitionMessage instance.
   */
  @Override
  public SentryJSONDropPartitionMessage getDropPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONDropPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONDropPartitionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize CreateFunctionMessage instance.
   */
  @Override
  public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONCreateFunctionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONCreateFunctionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropFunctionMessage instance.
   */
  @Override
  public DropFunctionMessage getDropFunctionMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONDropFunctionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONDropDatabaseMessage: ", e);
    }
  }

  /**
   * Method to de-serialize JSONInsertMessage instance.
   */
  @Override
  public InsertMessage getInsertMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONInsertMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AddPrimaryKeyMessage getAddPrimaryKeyMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAddPrimaryKeyMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AddForeignKeyMessage getAddForeignKeyMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAddForeignKeyMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AddUniqueConstraintMessage getAddUniqueConstraintMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAddUniqueConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AddNotNullConstraintMessage getAddNotNullConstraintMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAddNotNullConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public DropConstraintMessage getDropConstraintMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONDropConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public OpenTxnMessage getOpenTxnMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONOpenTxnMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public CommitTxnMessage getCommitTxnMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONCommitTxnMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AbortTxnMessage getAbortTxnMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAbortTxnMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AllocWriteIdMessage getAllocWriteIdMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAllocWriteIdMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  public static String serialize(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }
}
