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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterDatabaseMessage;
import org.codehaus.jackson.annotate.JsonProperty;

public class SentryJSONAlterDatabaseMessage extends JSONAlterDatabaseMessage {
  @JsonProperty
  private String newLocation;
  @JsonProperty
  private String oldLocation;

  public SentryJSONAlterDatabaseMessage() {
  }

  public SentryJSONAlterDatabaseMessage(String server, String servicePrincipal, Database dbObjBefore,
                                        Database dbObjAfter, Long timestamp) {
    this(server, servicePrincipal, dbObjBefore, dbObjAfter, timestamp,
        dbObjBefore.getLocationUri(), dbObjAfter.getLocationUri());
  }

  public SentryJSONAlterDatabaseMessage(String server, String servicePrincipal,
                                        Database dbObjBefore, Database dbObjAfter, Long timestamp,
                                        String oldLocation, String newLocation) {
    super(server, servicePrincipal, dbObjBefore, dbObjAfter, timestamp);
    this.newLocation = newLocation;
    this.oldLocation = oldLocation;
  }

  public String getNewLocation() {
    return newLocation;
  }

  public String getOldLocation() {
    return oldLocation;
  }

  @Override
  public String toString() {
    return SentryJSONMessageDeserializer.serialize(this);
  }
}
