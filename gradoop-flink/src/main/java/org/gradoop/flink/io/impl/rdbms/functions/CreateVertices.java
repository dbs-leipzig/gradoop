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
package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkDatabaseInputHelper;
import org.gradoop.flink.io.impl.rdbms.connection.RdbmsConfig;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Creates Epgm vertices from database tables
 */
public class CreateVertices {

  /**
   * Creates Epgm vertices from database table tuples
   *
   * @param flinkConfig Valid gradoop flink config
   * @param rdbmsConfig Valid rdbms config
   * @param metadataParser Metadata of connected relational database
   * @return DataSet of Epgm vertices
   */
  public static DataSet<Vertex> create(GradoopFlinkConfig flinkConfig, RdbmsConfig rdbmsConfig,
      MetaDataParser metadataParser) {
    List<TableToNode> tablesToNodes = metadataParser.getTablesToNodes();

    DataSet<Vertex> vertices = null;
    VertexFactory vertexFactory = flinkConfig.getVertexFactory();

    int counter = 0;

    for (TableToNode table : tablesToNodes) {
      DataSet<Row> dsSQLResult = FlinkDatabaseInputHelper.getInput(
          flinkConfig.getExecutionEnvironment(), rdbmsConfig, table.getRowCount(),
          table.getSqlQuery(), table.getRowTypeInfo());

      if (vertices == null) {
        vertices = dsSQLResult.map(new RowToVertices(vertexFactory, table.getTableName(), counter))
            .withBroadcastSet(flinkConfig.getExecutionEnvironment().fromCollection(tablesToNodes),
                RdbmsConstants.BROADCAST_VARIABLE);
      } else {
        vertices = vertices.union(dsSQLResult
            .map(new RowToVertices(vertexFactory, table.getTableName(), counter))
            .withBroadcastSet(flinkConfig.getExecutionEnvironment().fromCollection(tablesToNodes),
                RdbmsConstants.BROADCAST_VARIABLE));
      }

      counter++;
    }
    return vertices;
  }
}
