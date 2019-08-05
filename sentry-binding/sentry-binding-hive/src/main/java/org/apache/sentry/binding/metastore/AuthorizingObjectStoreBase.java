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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter;
import org.apache.sentry.binding.hive.authz.MetastoreAuthzObjectFilter.ObjectExtractor;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

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
public class AuthorizingObjectStoreBase extends ObjectStore {
  private static ImmutableSet<String> serviceUsers;
  private static HiveConf hiveConf;
  private static HiveAuthzConf authzConf;
  private static HiveAuthzBinding hiveAuthzBinding;
  private static String NO_ACCESS_MESSAGE_TABLE = "Table does not exist or insufficient privileges to access: ";
  private static String NO_ACCESS_MESSAGE_DATABASE = "Database does not exist or insufficient privileges to access: ";

  @Override
  public List<String> getDatabases(String catName, String pattern) throws MetaException {
    return filterDatabases(catName, super.getDatabases(catName, pattern));
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException {
    return filterDatabases(catName, super.getAllDatabases(catName));
  }

  @Override
  public Database getDatabase(String catName, String name) throws NoSuchObjectException {
    Database db = super.getDatabase(catName, name);
    try {
      if (filterDatabases(catName, Lists.newArrayList(name)).isEmpty()) {
        throw new NoSuchObjectException(getNoAccessMessageForDB(name));
      }
    } catch (MetaException e) {
      throw new NoSuchObjectException("Failed to authorized access to " + name
          + " : " + e.getMessage());
    }
    return db;
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws MetaException {
    Table table = super.getTable(catName, dbName, tableName);
    if (table == null
        || filterTables(catName, dbName, Lists.newArrayList(tableName)).isEmpty()) {
      return null;
    }
    return table;
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new NoSuchObjectException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.getPartition(catName, dbName, tableName, part_vals);
  }

  @Override
  public List<Partition> getPartitions(String catName, String dbName, String tableName,
      int maxParts) throws MetaException, NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.getPartitions(catName, dbName, tableName, maxParts);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern)
      throws MetaException {
    return filterTables(catName, dbName, super.getTables(catName, dbName, pattern));
  }
 
  @Override
  public List<Table> getTableObjectsByName(String catName, String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {
    return super.getTableObjectsByName(catName, dbname, filterTables(catName, dbname, tableNames));
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws MetaException {
    return filterTables(catName, dbName, super.getAllTables(catName, dbName));
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
      short maxTables) throws MetaException {
    return filterTables(catName, dbName,
        super.listTableNamesByFilter(catName, dbName, filter, maxTables));
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName,
      short max_parts) throws MetaException {
    if (filterTables(catName, dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.listPartitionNames(catName, dbName, tableName, max_parts);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String catName, String dbName,
      String tblName, String filter, short maxParts) throws MetaException,
      NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionsByFilter(catName, dbName, tblName, filter, maxParts);
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionsByNames(catName, dbName, tblName, partNames);
  }

  @Override
  public Partition getPartitionWithAuth(String catName, String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionWithAuth(catName, dbName, tblName, partVals, user_name,
        group_names);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName,
        groupNames);
  }

  @Override
  public List<String> listPartitionNamesPs(String catName, String dbName, String tblName,
      List<String> part_vals, short max_parts) throws MetaException,
      NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.listPartitionNamesPs(catName, dbName, tblName, part_vals, max_parts);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String catName, String dbName,
      String tblName, List<String> part_vals, short max_parts, String userName,
      List<String> groupNames) throws MetaException, InvalidObjectException,
      NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.listPartitionsPsWithAuth(catName, dbName, tblName, part_vals,
        max_parts, userName, groupNames);
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames) throws MetaException,
      NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tableName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
    }
    return super.getTableColumnStatistics(catName, dbName, tableName, colNames);
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(
      String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    if (filterTables(catName, dbName, Lists.newArrayList(tblName)).isEmpty()) {
      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
    }
    return super.getPartitionColumnStatistics(catName, dbName, tblName, partNames,
        colNames);
  }

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
    // Username is case sensitive
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
