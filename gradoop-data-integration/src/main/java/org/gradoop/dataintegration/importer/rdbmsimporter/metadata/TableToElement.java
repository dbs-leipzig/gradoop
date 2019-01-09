/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.FkTuple;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.NameTypeTuple;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Base class for metadata representation of database tables.
 */
public abstract class TableToElement {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Name of database table.
   */
  private String tableName;

  /**
   * Management type of connected rdbms.
   */
  private RdbmsConstants.RdbmsType rdbmsType;

  /**
   * List of primary key names and belonging datatypes.
   */
  private ArrayList<NameTypeTuple> primaryKeys;

  /**
   * List of foreign key names and belonging datatypes.
   */
  private ArrayList<FkTuple> foreignKeys;

  /**
   * List of further attribute names and belonging data types.
   */
  private ArrayList<NameTypeTuple> furtherAttributes;

  /**
   * Number of rows.
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
   * Flink type information for database table columns.
   */
  private RowTypeInfo rowTypeInfo;

  /**
   * Void constructor
   */
  public TableToElement() { }

  /**
   * Creates an instance of {@link TableToElement} to store database metadata.
   *
   * @param rdbmsType type of connected database management system
   * @param tableName name of table
   * @param primaryKeys table's primary keys
   * @param foreignKeys table's foreign keys
   * @param furtherAttributes tables's further attributes
   * @param rowCount count of rows of table
   */
  TableToElement(
    RdbmsConstants.RdbmsType rdbmsType, String tableName, ArrayList<NameTypeTuple> primaryKeys,
    ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes, int rowCount) {
    Objects.requireNonNull(rdbmsType);
    Objects.requireNonNull(tableName);
    this.rdbmsType = rdbmsType;
    this.tableName = tableName;
    this.primaryKeys = primaryKeys;
    this.foreignKeys = foreignKeys;
    this.furtherAttributes = furtherAttributes;
    this.rowCount = rowCount;
    this.rowheader = new RowHeader();
    this.rowTypeInfo = new RowTypeInfo();
  }

  /**
   * Get serial version id.
   *
   * @return serial version id
   */
  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  /**
   * Get name of database Table.
   *
   * @return name of table
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get type of management system type.
   *
   * @return type of database management system
   */
  RdbmsConstants.RdbmsType getRdbmsType() {
    return rdbmsType;
  }

  /**
   * Get primary keys of database table.
   *
   * @return primary keys
   */
  ArrayList<NameTypeTuple> getPrimaryKeys() {
    return primaryKeys;
  }

  /**
   * Get foreign keys of database table.
   *
   * @return foreign keys
   */
  ArrayList<FkTuple> getForeignKeys() {
    return foreignKeys;
  }

  /**
   * Get further Attributes of database table.
   *
   * @return further attributes
   */
  ArrayList<NameTypeTuple> getFurtherAttributes() {
    return furtherAttributes;
  }

  /**
   * Get count of rows of database table.
   *
   * @return count of rows of database table
   */
  public int getRowCount() {
    return rowCount;
  }

  /**
   * Get SQL query string to receive needed data of database.
   *
   * @return SQL query string
   */
  public String getSqlQuery() {
    return sqlQuery;
  }

  /**
   * Get row header to locate fields in row representation.
   *
   * @return row header
   */
  public RowHeader getRowheader() {
    return rowheader;
  }

  /**
   * Get type information of every row field.
   *
   * @return type information of row fields
   */
  public RowTypeInfo getRowTypeInfo() {
    return rowTypeInfo;
  }

  /**
   * Set name of database table.
   *
   * @param tableName name of database table
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Set type of database management system.
   *
   * @param rdbmsType database management system type
   */
  public void setRdbmsType(
    RdbmsConstants.RdbmsType rdbmsType) {
    this.rdbmsType = rdbmsType;
  }

  /**
   * Set primary keys of database.
   *
   * @param primaryKeys primary keys
   */
  public void setPrimaryKeys(
    ArrayList<NameTypeTuple> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  /**
   * Set foreign keys of database table.
   *
   * @param foreignKeys foreign keys
   */
  public void setForeignKeys(
    ArrayList<FkTuple> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  /**
   * Set further attributes of database table.
   *
   * @param furtherAttributes further attributes
   */
  public void setFurtherAttributes(
    ArrayList<NameTypeTuple> furtherAttributes) {
    this.furtherAttributes = furtherAttributes;
  }

  /**
   * Set count of rows of database table.
   *
   * @param rowCount count of rows of database table
   */
  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  /**
   * Set SQL query to receive needed data from database.
   *
   * @param sqlQuery SQL query
   */
  void setSqlQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
  }

  /**
   * Set row header of database table to locate fields of row representation.
   *
   * @param rowheader row header
   */
  void setRowheader(RowHeader rowheader) {
    this.rowheader = rowheader;
  }

  /**
   * Set type information of every field of a row.
   *
   * @param rowTypeInfo type information of field in row
   */
  void setRowTypeInfo(RowTypeInfo rowTypeInfo) {
    this.rowTypeInfo = rowTypeInfo;
  }
}
