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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.impl.rdbms.metadata.TableToVertex;
import org.gradoop.dataintegration.importer.impl.rdbms.tuples.RowHeaderTuple;

import java.util.List;

import static org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.BROADCAST_VARIABLE;
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
    v.getProperties().remove(PK_ID);

    for (TableToVertex table : tablesToVertices) {
      if (table.getTableName().equals(v.getLabel())) {
        for (RowHeaderTuple fk : table.getForeignKeyHeader()) {
          v.getProperties().remove(fk.f0);
        }
      }
    }
    return v;
  }
}
