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
package org.gradoop.dataintegration.importer.rdbms.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.dataintegration.importer.rdbms.connection.Helper;
import org.gradoop.dataintegration.importer.rdbms.connection.RdbmsConfig;
import org.gradoop.dataintegration.importer.rdbms.metadata.MetaDataParser;
import org.gradoop.dataintegration.importer.rdbms.metadata.TableToVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

import static org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.BROADCAST_VARIABLE;

/**
 * Creates EPGM vertices from database tables.
 */
public class CreateVertices {

  /**
   * No need to instantiate.
   */
  private CreateVertices() { }

  /**
   * Creates EPGM vertices from database table tuples.
   *
   * @param flinkConfig valid gradoop flink config
   * @param rdbmsConfig valid rdbms config
   * @param metadataParser database metadata
   * @return set of EPGM vertices
   */
  public static DataSet<Vertex> convert(

    GradoopFlinkConfig flinkConfig,
    RdbmsConfig rdbmsConfig,
    MetaDataParser metadataParser) {

    List<TableToVertex> tablesToVertices = metadataParser.getTablesToVertices();

    DataSet<Vertex> vertices = null;
    VertexFactory vertexFactory = flinkConfig.getVertexFactory();

    int counter = 0;
    for (TableToVertex table : tablesToVertices) {
      DataSet<Row> dsSQLResult = Helper.getRdbmsInput(
          flinkConfig.getExecutionEnvironment(), rdbmsConfig, table.getRowCount(),
          table.getSqlQuery(), table.getRowTypeInfo());

      if (vertices == null) {
        vertices = dsSQLResult
          .map(new RowToVertex(vertexFactory, table.getTableName(), counter))
          .withBroadcastSet(
            flinkConfig.getExecutionEnvironment().fromCollection(tablesToVertices),
            BROADCAST_VARIABLE);
      } else {
        vertices = vertices
          .union(
            dsSQLResult
              .map(new RowToVertex(vertexFactory, table.getTableName(), counter))
              .withBroadcastSet(
                flinkConfig.getExecutionEnvironment().fromCollection(tablesToVertices),
                BROADCAST_VARIABLE));
      }

      counter++;
    }
    return vertices;
  }
}
