/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.binding.metastore;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter.ObjectExtractor;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/***
 * This class is the wrapper of ObjectStore which is the interface between the
 * application logic and the database store. Do the authorization or filter the
 * result when processing the metastore request.
 * eg:
 * Callers will only receive the objects back which they have privileges to
 * access.
 * If there is a request for the object list(like getAllTables()), the result
 * will be filtered to exclude object the requestor doesn't have privilege to
 * access.
 */
public class AuthorizingObjectStore extends ObjectStore {
  private static ImmutableSet<String> serviceUsers;
  private static HiveConf hiveConf;
  private static HiveAuthzConf authzConf;
  private static HiveAuthzBinding hiveAuthzBinding;
  private static final String NO_ACCESS_MESSAGE_TABLE = "Table does not exist or insufficient privileges to access: ";
  private static final String NO_ACCESS_MESSAGE_DATABASE = "Database does not exist or insufficient privileges to access: ";

  /**
   * Invoke Hive database filtering that removes the entries which use has no
   * privileges to access
   * @param dbList
   * @return
   * @throws MetaException
   */
  private List<String> filterDatabases(String catName, List<String> dbList)
      throws MetaException {
    if (needsAuthorization(getUserName())) {
      try {
        MetastoreAuthzObjectFilter<String> filter = new MetastoreAuthzObjectFilter<>(
          getHiveAuthzBinding(), new ObjectExtractor<String>() {
          @Override
          public String getCatalogName(String o) {
            return catName;
          }

          @Override
          public String getDatabaseName(String o) {
            return o;
          }

          @Override
          public String getTableName(String o) {
            return null;
          }
        });

        // TODO: pass catName
        return filter.filterDatabases(getUserName(), dbList);
      } catch (Exception e) {
        throw new MetaException("Error getting DB list " + e.getMessage());
      }
    } else {
      return dbList;
    }
  }

  /**
   * Invoke Hive table filtering that removes the entries which use has no
   * privileges to access
   * @param tabList
   * @return
   * @throws MetaException
   */
  protected List<String> filterTables(String catName, String dbName, List<String> tabList)
      throws MetaException {
    if (needsAuthorization(getUserName())) {
      try {
        MetastoreAuthzObjectFilter<String> filter = new MetastoreAuthzObjectFilter<>(
          getHiveAuthzBinding(), new ObjectExtractor<String>() {
          @Override
          public String getCatalogName(String o) {
            return catName;
          }

          @Override
          public String getDatabaseName(String o) {
            return dbName;
          }

          @Override
          public String getTableName(String o) {
            return o;
          }
        });

        // TODO: pass catName
        return filter.filterTables(getUserName(), tabList);
      } catch (Exception e) {
        throw new MetaException("Error getting Table list " + e.getMessage());
      }
    } else {
      return tabList;
    }
  }

  /**
   * load Hive auth provider
   * 
   * @return
   * @throws MetaException
   */
  private HiveAuthzBinding getHiveAuthzBinding() throws MetaException {
    if (hiveAuthzBinding == null) {
      try {
        hiveAuthzBinding = new HiveAuthzBinding(HiveAuthzBinding.HiveHook.HiveMetaStore,
            getHiveConf(), getAuthzConf());
      } catch (Exception e) {
        throw new MetaException("Failed to load Hive binding " + e.getMessage());
      }
    }
    return hiveAuthzBinding;
  }

  private ImmutableSet<String> getServiceUsers() throws MetaException {
    if (serviceUsers == null) {
      serviceUsers = ImmutableSet.copyOf(toTrimed(Sets.newHashSet(getAuthzConf().getStrings(
          AuthzConfVars.AUTHZ_METASTORE_SERVICE_USERS.getVar(), new String[] { "" }))));
    }
    return serviceUsers;
  }

  private HiveConf getHiveConf() {
    if (hiveConf == null) {
      hiveConf = new HiveConf(getConf(), this.getClass());
    }
    return hiveConf;
  }

  private HiveAuthzConf getAuthzConf() throws MetaException {
    if (authzConf == null) {
      String hiveAuthzConf = getConf().get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
      if (hiveAuthzConf == null
          || (hiveAuthzConf = hiveAuthzConf.trim()).isEmpty()) {
        throw new MetaException("Configuration key "
            + HiveAuthzConf.HIVE_SENTRY_CONF_URL + " value '" + hiveAuthzConf
            + "' is invalid.");
      }
      try {
        authzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
      } catch (MalformedURLException e) {
        throw new MetaException("Configuration key "
            + HiveAuthzConf.HIVE_SENTRY_CONF_URL
            + " specifies a malformed URL '" + hiveAuthzConf + "' "
            + e.getMessage());
      }
    }
    return authzConf;
  }

  /**
   * Extract the user from underlying auth subsystem
   * @return
   * @throws MetaException
   */
  private String getUserName() throws MetaException {
    try {
      return Utils.getUGI().getShortUserName();
    } catch (LoginException e) {
      throw new MetaException("Failed to get username " + e.getMessage());
    } catch (IOException e) {
      throw new MetaException("Failed to get username " + e.getMessage());
    }
  }

  /**
   * Check if the give user needs to be validated.
   * @param userName
   * @return
   */
  private boolean needsAuthorization(String userName) throws MetaException {
    // Username should be case sensitive
    return !getServiceUsers().contains(userName);
  }

  private static Set<String> toTrimed(Set<String> s) {
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim());
    }
    return result;
  }

  protected String getNoAccessMessageForTable(String dbName, String tableName) {
    return NO_ACCESS_MESSAGE_TABLE + "<" + dbName + ">.<" + tableName + ">";
  }

  private String getNoAccessMessageForDB(String dbName) {
    return NO_ACCESS_MESSAGE_DATABASE + "<" + dbName + ">";
  }
}
