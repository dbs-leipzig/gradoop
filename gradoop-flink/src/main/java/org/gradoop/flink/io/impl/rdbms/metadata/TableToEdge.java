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

import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.connection.SQLToBasicTypeMapper;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Stores metadata for tuple-to-edge conversation
 */
public class TableToEdge {

  /**
   * Management type of connecte rdbms
   */
  private int rdbmsType;

  /**
   * Relationship type
   */
  private String relationshipType;

  /**
   * Name of relation start table
   */
  private String startTableName;

  /**
   * Name of relation end table
   */
  private String endTableName;

  /**
   * Name and datatype of relation start attribute
   */
  private NameTypeTuple startAttribute;

  /**
   * Name and datatype of relation end attribute
   */
  private NameTypeTuple endAttribute;

  /**
   * List of primary key names and belonging datatypes
   */
  private ArrayList<NameTypeTuple> primaryKeys;

  /**
   * List of further attribute names and belonging datatypes
   */
  private ArrayList<NameTypeTuple> furtherAttributes;

  /**
   * Direction indicator
   */
  private boolean directionIndicator;

  /**
   * Number of rows
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
   * Conctructor
   *
   * @param rdbmsType
   *        Management type of connected rdbms
   * @param relationshipType
   *          Relationship type
   * @param startTableName
   *          Name of relation start table
   * @param endTableName
   *          Name of relation end table
   * @param startAttribute
   *          Name and type of relation start attribute
   * @param endAttribute
   *          Name and datatype of relation end attribute
   * @param primaryKeys
   *          List of primary key names and datatypes
   * @param furtherAttributes
   *          List of further attribute names and datatypes
   * @param directionIndicator
   *          Direction indicator
   * @param rowCount
   *          Number of rows
   */
  public TableToEdge(int rdbmsType, String relationshipType, String startTableName,
      String endTableName, NameTypeTuple startAttribute, NameTypeTuple endAttribute,
      ArrayList<NameTypeTuple> primaryKeys, ArrayList<NameTypeTuple> furtherAttributes,
      boolean directionIndicator, int rowCount) {
    this.rdbmsType = rdbmsType;
    this.relationshipType = relationshipType;
    this.startTableName = startTableName;
    this.endTableName = endTableName;
    this.startAttribute = startAttribute;
    this.endAttribute = endAttribute;
    this.primaryKeys = primaryKeys;
    this.furtherAttributes = furtherAttributes;
    this.directionIndicator = directionIndicator;
    this.rowCount = rowCount;
    this.rowheader = new RowHeader();
    if (!directionIndicator) {
      this.sqlQuery = SQLQuery.getNtoMEdgeTableQuery(relationshipType, startAttribute.f0,
          endAttribute.f0, furtherAttributes, rdbmsType);
    }
  }

  /**
   * Creates a valid type information for belonging sql query
   *
   * @return Row type information for belonging sql query
   */
  public RowTypeInfo getRowTypeInfo() {
    TypeInformation<?>[] fieldTypes = null;

    if (!directionIndicator) {
      fieldTypes = new TypeInformation<?>[furtherAttributes.size() + 2];
      fieldTypes[0] = SQLToBasicTypeMapper.getTypeInfo(startAttribute.f1, rdbmsType);
      rowheader.getRowHeader()
          .add(new RowHeaderTuple(startAttribute.f0, RdbmsConstants.FK_FIELD, 0));
      fieldTypes[1] = SQLToBasicTypeMapper.getTypeInfo(endAttribute.f1, rdbmsType);
      rowheader.getRowHeader().add(new RowHeaderTuple(endAttribute.f0, RdbmsConstants.FK_FIELD, 1));

      int i = 2;
      for (NameTypeTuple att : furtherAttributes) {
        fieldTypes[i] = SQLToBasicTypeMapper.getTypeInfo(att.f1, rdbmsType);
        rowheader.getRowHeader().add(new RowHeaderTuple(att.f0, RdbmsConstants.ATTRIBUTE_FIELD, i));
        i++;
      }
    }
    return new RowTypeInfo(fieldTypes);
  }

  public String getRelationshipType() {
    return relationshipType;
  }

  public void setRelationshipType(String relationshipType) {
    this.relationshipType = relationshipType;
  }

  public String getStartTableName() {
    return startTableName;
  }

  public void setStartTableName(String startTableName) {
    this.startTableName = startTableName;
  }

  public String getEndTableName() {
    return endTableName;
  }

  public void setEndTableName(String endTableName) {
    this.endTableName = endTableName;
  }

  public NameTypeTuple getStartAttribute() {
    return startAttribute;
  }

  public void setStartAttribute(NameTypeTuple startAttribute) {
    this.startAttribute = startAttribute;
  }

  public NameTypeTuple getEndAttribute() {
    return endAttribute;
  }

  public void setEndAttribute(NameTypeTuple endAttribute) {
    this.endAttribute = endAttribute;
  }

  public ArrayList<NameTypeTuple> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(ArrayList<NameTypeTuple> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public ArrayList<NameTypeTuple> getFurtherAttributes() {
    return furtherAttributes;
  }

  public void setFurtherAttributes(ArrayList<NameTypeTuple> furtherAttributes) {
    this.furtherAttributes = furtherAttributes;
  }

  public boolean isDirectionIndicator() {
    return directionIndicator;
  }

  public void setDirectionIndicator(boolean directionIndicator) {
    this.directionIndicator = directionIndicator;
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
}
