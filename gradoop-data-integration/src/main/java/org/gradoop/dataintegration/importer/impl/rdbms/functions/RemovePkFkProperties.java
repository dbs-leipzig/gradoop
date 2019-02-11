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
package org.gradoop.dataintegration.importer.impl.rdbms.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.dataintegration.importer.impl.rdbms.metadata.TableToVertex;
import org.gradoop.dataintegration.importer.impl.rdbms.tuples.RowHeaderTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.BROADCAST_VARIABLE;
import static org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.FK_FIELD;
import static org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.PK_ID;

/**
 * Cleans Epgm vertices by deleting primary key and foreign key propeties
 */
public class RemovePkFkProperties extends RichMapFunction<Vertex, Vertex> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * List of tables to nodes representation
   */
  private List<TableToVertex> tablesToVertices;

  @Override
  public void open(Configuration parameters) {
    this.tablesToVertices = getRuntimeContext()
      .getBroadcastVariable(BROADCAST_VARIABLE);
  }

  @Override
  public Vertex map(Vertex v) {

    Properties newProps = Properties.create();

    for (TableToVertex table : tablesToVertices) {
      if (table.getTableName().equals(v.getLabel())) {

        ArrayList<String> foreignKeys = Lists.newArrayList();
        for (RowHeaderTuple rowHeaderTuple : table.getRowheader()) {
          if (rowHeaderTuple.getAttributeRole().equals(FK_FIELD)) {
            foreignKeys.add(rowHeaderTuple.f0);
          }
        }

        for (Property oldProperty : Objects.requireNonNull(v.getProperties())) {
          if (!oldProperty.getKey().equals(PK_ID) &&
            !foreignKeys.contains(oldProperty.getKey())) {
            newProps.set(oldProperty);
          }
        }
      }
    }
    v.setProperties(newProps);
    return v;
  }
}
