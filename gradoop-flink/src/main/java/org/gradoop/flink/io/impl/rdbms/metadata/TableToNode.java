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

package org.gradoop.flink.io.impl.rdbms.metadata;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.connection.SQLToBasicTypeMapper;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants.RdbmsType;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Stores metadata for tuple-to-vertex conversation
 */
public class TableToNode implements Serializable {

  /**
   * Serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Management type of connected rdbms
   */
  private RdbmsType rdbmsType;

  /**
   * Name of database table
   */
  private String tableName;

  /**
   * List of primary key names and belonging datatypes
   */
  private ArrayList<NameTypeTuple> primaryKeys;

  /**
   * List of foreign key names and belonging datatypes
   */
  private ArrayList<FkTuple> foreignKeys;

  /**
   * List of further attribute names and belonging datatypes
   */
  private ArrayList<NameTypeTuple> furtherAttributes;

  /**
   * Numbe of rows of database table
   */
  private int rowCount;

  /**
   * Valid sql query for querying needed relational data
   */
  private String sqlQuery;

  /**
   * Rowheader for row data representation of relational data
   */
  private RowHeader rowheader;

  /**
   * Flink type information for belonging database value
   */
  private RowTypeInfo rowTypeInfo;

  /**
   * Constructor
   *
   * @param rdbmsType Management type of connected rdbms
   * @param tableName Name of database table
   * @param primaryKeys List of primary key names and datatypes
   * @param foreignKeys List of foreign key names and datatypes
   * @param furtherAttributes List of further attribute names and datatypes
   * @param rowCount Number of database rows
   */
  public TableToNode(RdbmsType rdbmsType, String tableName, ArrayList<NameTypeTuple> primaryKeys,
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
   * Creates sql query for querying database, belonging rowheader and belonging
   * flink type information
   */
  public void init() {
    this.sqlQuery = SQLQuery.getNodeTableQuery(tableName, primaryKeys, foreignKeys,
        furtherAttributes, rdbmsType);

    rowheader = new RowHeader();

    TypeInformation<?>[] fieldTypes = new TypeInformation[primaryKeys.size() + foreignKeys.size() +
        furtherAttributes.size()];

    int i = 0;
    try {
      for (NameTypeTuple pk : primaryKeys) {
        fieldTypes[i] = SQLToBasicTypeMapper.getTypeInfo(pk.f1, rdbmsType);
        rowheader.getRowHeader().add(new RowHeaderTuple(pk.f0, RdbmsConstants.PK_FIELD, i));
        i++;
      }
      for (FkTuple fk : foreignKeys) {
        fieldTypes[i] = SQLToBasicTypeMapper.getTypeInfo(fk.f1, rdbmsType);
        rowheader.getRowHeader().add(new RowHeaderTuple(fk.f0, RdbmsConstants.FK_FIELD, i));
        i++;
      }
      for (NameTypeTuple att : furtherAttributes) {
        fieldTypes[i] = SQLToBasicTypeMapper.getTypeInfo(att.f1, rdbmsType);
        rowheader.getRowHeader()
            .add(new RowHeaderTuple(att.f0, RdbmsConstants.ATTRIBUTE_FIELD, i));
        i++;
      }
    } catch (IllegalArgumentException e) {
      System.err.println("Empty attribute set. Error Message : " + e.getMessage());
    }
    this.rowTypeInfo = new RowTypeInfo(fieldTypes);
  }

  public RdbmsType getRdbmsType() {
    return rdbmsType;
  }

  public void setRdbmsType(RdbmsType rdbmsType) {
    this.rdbmsType = rdbmsType;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public ArrayList<NameTypeTuple> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(ArrayList<NameTypeTuple> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public ArrayList<FkTuple> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(ArrayList<FkTuple> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  public ArrayList<NameTypeTuple> getFurtherAttributes() {
    return furtherAttributes;
  }

  public void setFurtherAttributes(ArrayList<NameTypeTuple> furtherAttributes) {
    this.furtherAttributes = furtherAttributes;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public void setSqlQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
  }

  public RowHeader getRowheader() {
    return rowheader;
  }

  public void setRowheader(RowHeader rowheader) {
    this.rowheader = rowheader;
  }

  public RowTypeInfo getRowTypeInfo() {
    return rowTypeInfo;
  }

  public void setRowTypeInfo(RowTypeInfo rowTypeInfo) {
    this.rowTypeInfo = rowTypeInfo;
  }
}
