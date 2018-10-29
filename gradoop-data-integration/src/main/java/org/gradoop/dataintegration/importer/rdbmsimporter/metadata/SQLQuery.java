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
public class SQLQuery {

  /**
   * Creates a sql query for vertex conversion
   *
   * @param tableName Name of database table
   * @param primaryKeys List of primary keys
   * @param foreignKeys List of foreign keys
   * @param furtherAttributes List of further attributes
   * @param rdbmsType Management type of connected rdbms
   * @return Valid sql string for querying needed data for tuple-to-vertex
   *         conversation
   */
  public static String getNodeTableQuery(String tableName, ArrayList<NameTypeTuple> primaryKeys,
      ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes,
      RdbmsType rdbmsType) {

    StringBuilder sqlQuery = new StringBuilder("SELECT ");

    if (!primaryKeys.isEmpty()) {
      for (NameTypeTuple pk : primaryKeys) {
        if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
          sqlQuery = sqlQuery.append("[" + pk.f0 + "]" + ",");
        } else {
          sqlQuery = sqlQuery.append(pk.f0 + ",");
        }
      }
    }

    if (!foreignKeys.isEmpty()) {
      for (FkTuple fk : foreignKeys) {
        if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
          sqlQuery = sqlQuery.append("[" + fk.f0 + "]" + ",");
        } else {
          sqlQuery = sqlQuery.append(fk.f0 + ",");
        }
      }
    }

    if (!furtherAttributes.isEmpty()) {
      for (NameTypeTuple att : furtherAttributes) {
        if (rdbmsType == RdbmsType.SQLSERVER_TYPE) {
          sqlQuery = sqlQuery.append("[" + att.f0 + "]" + ",");
        } else {
          sqlQuery = sqlQuery.append(att.f0 + ",");
        }
      }
    }

    return sqlQuery.toString().substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
  }

  /**
   * Creates a sql query for tuple to edge conversation
   *
   * @param tableName Name of database table
   * @param startAttribute Name of first foreign key attribute
   * @param endAttribute Name of second foreign key attribute
   * @param furtherAttributes List of further attributes
   * @param rdbmsType Management type of connected rdbms
   * @return Valid sql string for querying needed data for tuple-to-edge
   *         conversation
   */
  public static String getNtoMEdgeTableQuery(String tableName, String startAttribute,
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
          sqlQuery = sqlQuery.append("[" + att.f0 + "]" + ",");
        } else {
          sqlQuery = sqlQuery.append(att.f0 + ",");
        }
      }
    }

    return sqlQuery.toString().substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
  }
}
