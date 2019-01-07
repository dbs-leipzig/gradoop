/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.dataintegration.importer.rdbmsimporter.metadata;

import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.RdbmsType;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.FkTuple;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.NameTypeTuple;

import java.util.ArrayList;

/**
 * Provides sql queries
 */
class SQLQuery {

  /**
   * Creates a sql query for vertex conversion.
   *
   * @param tableName name of database table
   * @param primaryKeys list of primary keys
   * @param foreignKeys list of foreign keys
   * @param furtherAttributes list of further attributes
   * @param rdbmsType management system type of connected rdbms
   * @return valid sql string for querying needed data for tuple-to-vertex conversion
   */
  static String getNodeTableQuery(
    String tableName, ArrayList<NameTypeTuple> primaryKeys,
    ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes,
    RdbmsType rdbmsType) {
    StringBuilder sqlQuery = new StringBuilder("SELECT ");

    ArrayList[] attributeList = {primaryKeys, foreignKeys, furtherAttributes};

    for (int i = 0; i < 3; i++) {
      for (Object attribute : attributeList[i]) {

        if (attribute instanceof NameTypeTuple) {
          if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
            sqlQuery.append("[").append(((NameTypeTuple) attribute).f0).append("],");
          } else {
            sqlQuery.append(((NameTypeTuple) attribute).f0).append(",");
          }
        }

        if (attribute instanceof FkTuple) {
          if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
            sqlQuery.append("[").append(((FkTuple) attribute).f0).append("],");
          } else {
            sqlQuery.append(((FkTuple) attribute).f0).append(",");
          }
        }
      }
    }

    return sqlQuery.toString().substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
  }

  /**
   * Creates a sql query for whole tuple to edge conversation.
   *
   * @param tableName name of database table
   * @param startAttribute name of first foreign key attribute
   * @param endAttribute name of second foreign key attribute
   * @param furtherAttributes list of further attributes
   * @param rdbmsType management system type of connected rdbms
   * @return valid sql string for querying needed data for tuple-to-edge conversation
   */
  static String getNtoMEdgeTableQuery(
    String tableName, String startAttribute,
    String endAttribute, ArrayList<NameTypeTuple> furtherAttributes, RdbmsType rdbmsType) {

    if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
      startAttribute = "[" + startAttribute + "]";
      endAttribute = "[" + endAttribute + "]";
    }

    StringBuilder sqlQuery = new StringBuilder(
      "SELECT " + startAttribute + "," + endAttribute + ",");

    if (!furtherAttributes.isEmpty()) {
      for (NameTypeTuple att : furtherAttributes) {
        if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
          sqlQuery.append("[").append(att.f0).append("],");
        } else {
          sqlQuery.append(att.f0).append(",");
        }
      }
    }

    return sqlQuery.toString().substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
  }
}
