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
import org.gradoop.dataintegration.importer.rdbms.tuples.FkTuple;
import org.gradoop.dataintegration.importer.rdbms.tuples.NameTypeTuple;
import org.gradoop.dataintegration.importer.rdbms.tuples.RowHeaderTuple;

import java.io.Serializable;
import java.util.ArrayList;

import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.ATTRIBUTE_FIELD;
import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.FK_FIELD;
import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.PK_FIELD;


/**
 * Stores metadata for tuple-to-vertex conversation.
 */
public class TableToVertex extends TableToElement implements Serializable {

  /**
   * Instance of class {@link TableToVertex} representing the schema of a database relation going to
   * convert to a {@link org.gradoop.common.model.api.entities.EPGMVertex}.
   *
   * @param rdbmsType Management type of connected rdbms
   * @param tableName Name of database table
   * @param primaryKeys List of primary key names and datatypes
   * @param foreignKeys List of foreign key names and datatypes
   * @param furtherAttributes List of further attribute names and datatypes
   * @param rowCount Number of database rows
   */
  TableToVertex(
    RdbmsType rdbmsType, String tableName, ArrayList<NameTypeTuple> primaryKeys,
    ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes, int rowCount) {

    super(rdbmsType, tableName, primaryKeys, foreignKeys, furtherAttributes, rowCount);
    this.init();
  }

  /**
   * Creates sql query for querying database, belonging rowheader and belonging flink type
   * information.
   */
  private void init() {
    setSqlQuery(Helper.getTableToVerticesQuery(getTableName(), getPrimaryKeys(), getForeignKeys(),
      getFurtherAttributes(), getRdbmsType()));

    TypeInformation<?>[] fieldTypes =
      new TypeInformation[getPrimaryKeys().size() + getForeignKeys().size() +
        getFurtherAttributes().size()];

    int i = 0;
    if (!getPrimaryKeys().isEmpty()) {
      for (NameTypeTuple pk : getPrimaryKeys()) {
        fieldTypes[i] = Helper.getTypeInfo(pk.f1, getRdbmsType());
        getRowheader().add(new RowHeaderTuple(pk.f0, PK_FIELD, i));
        i++;
      }
    }

    if (!getForeignKeys().isEmpty()) {
      for (FkTuple fk : getForeignKeys()) {
        fieldTypes[i] = Helper.getTypeInfo(fk.f1, getRdbmsType());
        getRowheader().add(new RowHeaderTuple(fk.f0, FK_FIELD, i));
        i++;
      }
    }

    if (!getFurtherAttributes().isEmpty()) {
      for (NameTypeTuple att : getFurtherAttributes()) {
        fieldTypes[i] = Helper.getTypeInfo(att.f1, getRdbmsType());
        getRowheader().add(new RowHeaderTuple(att.f0, ATTRIBUTE_FIELD, i));
        i++;
      }
    }

    setRowTypeInfo(new RowTypeInfo(fieldTypes));
  }
}
