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
package org.gradoop.dataintegration.importer.rdbms.metadata;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.dataintegration.importer.rdbms.connection.Helper;
import org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.RdbmsType;
import org.gradoop.dataintegration.importer.rdbms.tuples.NameTypeTuple;
import org.gradoop.dataintegration.importer.rdbms.tuples.RowHeaderTuple;

import java.io.Serializable;
import java.util.ArrayList;

import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.ATTRIBUTE_FIELD;
import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.FK_FIELD;


/**
 * Stores metadata for tuple-to-edge conversation.
 */
public class TableToEdge extends TableToElement implements Serializable {

  /**
   * Name of relation start table.
   */
  private String startTableName;

  /**
   * Name of relation end table.
   */
  private String endTableName;

  /**
   * Name and data type of relation start attribute.
   */
  private NameTypeTuple startAttribute;

  /**
   * Name and data type of relation end attribute.
   */
  private NameTypeTuple endAttribute;

  /**
   * Direction indicator.
   */
  private boolean directionIndicator;

  /**
   * Creates an instance of {@link TableToEdge} to store relational table's metadata.
   *
   * @param tableName name of database table
   * @param rdbmsType management type of connected rdbms
   * @param furtherAttributes list of further attribute names and datatypes
   * @param rowCount number of rows
   * @param startTableName name of relation start table
   * @param endTableName name of relation end table
   * @param startAttribute name and type of relation start attribute
   * @param endAttribute name and datatype of relation end attribute
   * @param directionIndicator direction indicator
   */
  TableToEdge(
    String tableName, RdbmsType rdbmsType, ArrayList<NameTypeTuple> furtherAttributes,
    int rowCount, String startTableName, String endTableName, NameTypeTuple startAttribute,
    NameTypeTuple endAttribute, boolean directionIndicator) {
    super(tableName, rdbmsType, null, null, furtherAttributes, rowCount);
    this.startTableName = startTableName;
    this.endTableName = endTableName;
    this.startAttribute = startAttribute;
    this.endAttribute = endAttribute;
    this.directionIndicator = directionIndicator;
    if (!directionIndicator) {
      this.init();
    }
  }

  /**
   * Assigns proper sql query and generates belonging flink row type information and row header.
   */
  private void init() {
    TypeInformation<?>[] fieldTypes;

    setSqlQuery(Helper.getTableToEdgesQuery(getTableName(), this.startAttribute.f0,
      this.endAttribute.f0, getFurtherAttributes(), getRdbmsType()));

    fieldTypes = new TypeInformation<?>[getFurtherAttributes().size() + 2];

    fieldTypes[0] = Helper.getTypeInfo(startAttribute.f1, getRdbmsType());
    getRowheader().add(new RowHeaderTuple(startAttribute.f0, FK_FIELD, 0));

    fieldTypes[1] = Helper.getTypeInfo(endAttribute.f1, getRdbmsType());
    getRowheader().add(new RowHeaderTuple(endAttribute.f0, FK_FIELD, 1));

    int i = 2;
    if (!getFurtherAttributes().isEmpty()) {
      for (NameTypeTuple att : getFurtherAttributes()) {
        fieldTypes[i] = Helper.getTypeInfo(att.f1, getRdbmsType());
        getRowheader()
          .add(new RowHeaderTuple(att.f0, ATTRIBUTE_FIELD, i));
        i++;
      }
    }

    setRowTypeInfo(new RowTypeInfo(fieldTypes));
  }

  /**
   * Get name of start table.
   * @return name of start table
   */
  public String getStartTableName() {
    return startTableName;
  }

  /**
   * Get name of end table.
   * @return name of end table
   */
  public String getEndTableName() {
    return endTableName;
  }

  /**
   * Get name, data type of start attribute.
   * @return name, data type of start attribute
   */
  public NameTypeTuple getStartAttribute() {
    return startAttribute;
  }

  /**
   * Get name, data type of end attribute.
   * @return name, data type of end attribute
   */
  public NameTypeTuple getEndAttribute() {
    return endAttribute;
  }

  /**
   * True, when relation is a directed one.
   * @return true, when relation is a directed one
   */
  public boolean isDirectionIndicator() {
    return directionIndicator;
  }
}
