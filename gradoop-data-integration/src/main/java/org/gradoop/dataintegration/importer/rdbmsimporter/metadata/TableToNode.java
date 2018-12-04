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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.dataintegration.importer.rdbmsimporter.connection.SQLToBasicTypeMapper;
import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.RdbmsType;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.FkTuple;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.NameTypeTuple;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.RowHeaderTuple;

import java.io.Serializable;
import java.util.ArrayList;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.ATTRIBUTE_FIELD;
import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.FK_FIELD;
import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.PK_FIELD;


/**
 * Stores metadata for tuple-to-vertex conversation.
 */
public class TableToNode implements Serializable {

  /**
   * Serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Management type of connected rdbms.
   */
  private RdbmsType rdbmsType;

  /**
   * Name of database table.
   */
  private String tableName;

  /**
   * List of primary key names and belonging datatypes.
   */
  private ArrayList<NameTypeTuple> primaryKeys;

  /**
   * List of foreign key names and belonging datatypes.
   */
  private ArrayList<FkTuple> foreignKeys;

  /**
   * List of further attribute names and belonging datatypes.
   */
  private ArrayList<NameTypeTuple> furtherAttributes;

  /**
   * Numbe of rows of database table.
   */
  private int rowCount;

  /**
   * Valid sql query for querying needed relational data.
   */
  private String sqlQuery;

  /**
   * Rowheader for row data representation of relational data.
   */
  private RowHeader rowheader;

  /**
   * Flink type information for belonging database value.
   */
  private RowTypeInfo rowTypeInfo;

  /**
   * Instance of class {@link TableToNode} representing the schema of a database relation going to
   * convert to a {@link org.gradoop.common.model.api.entities.EPGMVertex}.
   *
   * @param rdbmsType Management type of connected rdbms
   * @param tableName Name of database table
   * @param primaryKeys List of primary key names and datatypes
   * @param foreignKeys List of foreign key names and datatypes
   * @param furtherAttributes List of further attribute names and datatypes
   * @param rowCount Number of database rows
   */
  public TableToNode(
    RdbmsType rdbmsType, String tableName, ArrayList<NameTypeTuple> primaryKeys,
    ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes, int rowCount) {
    this.rdbmsType = rdbmsType;
    this.tableName = tableName;
    this.primaryKeys = primaryKeys;
    this.foreignKeys = foreignKeys;
    this.furtherAttributes = furtherAttributes;
    this.rowCount = rowCount;
    this.init();
  }

  /**
   * Creates sql query for querying database, belonging rowheader and belonging flink type
   * information.
   */
  private void init() {
    this.sqlQuery = SQLQuery.getNodeTableQuery(tableName, primaryKeys, foreignKeys,
      furtherAttributes, rdbmsType);

    rowheader = new RowHeader();

    TypeInformation<?>[] fieldTypes = new TypeInformation[primaryKeys.size() + foreignKeys.size() +
      furtherAttributes.size()];

    SQLToBasicTypeMapper typeMapper = SQLToBasicTypeMapper.create();

    int i = 0;
    if (!primaryKeys.isEmpty()) {
      for (NameTypeTuple pk : primaryKeys) {
        fieldTypes[i] = typeMapper.getTypeInfo(pk.f1, rdbmsType);
        rowheader.getRowHeader()
          .add(new RowHeaderTuple(pk.f0, PK_FIELD, i));
        i++;
      }
    }

    if (!foreignKeys.isEmpty()) {
      for (FkTuple fk : foreignKeys) {
        fieldTypes[i] = typeMapper.getTypeInfo(fk.f1, rdbmsType);
        rowheader.getRowHeader()
          .add(new RowHeaderTuple(fk.f0, FK_FIELD, i));
        i++;
      }
    }

    if (!furtherAttributes.isEmpty()) {
      for (NameTypeTuple att : furtherAttributes) {
        fieldTypes[i] = typeMapper.getTypeInfo(att.f1, rdbmsType);
        rowheader.getRowHeader()
          .add(new RowHeaderTuple(att.f0, ATTRIBUTE_FIELD, i));
        i++;
      }
    }

    this.rowTypeInfo = new RowTypeInfo(fieldTypes);
  }

  public String getTableName() {
    return tableName;
  }

  public int getRowCount() {
    return rowCount;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public RowHeader getRowheader() {
    return rowheader;
  }

  public RowTypeInfo getRowTypeInfo() {
    return rowTypeInfo;
  }
}
