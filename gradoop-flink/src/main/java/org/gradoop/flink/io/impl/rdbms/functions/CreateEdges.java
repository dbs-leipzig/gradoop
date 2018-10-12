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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkDatabaseInputHelper;
import org.gradoop.flink.io.impl.rdbms.connection.RdbmsConfig;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.Fk1Fk2Props;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.LabelIdKeyTuple;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Creates Epgm edges from foreign key respectively n:m relations
 *
 */
public class CreateEdges {

  /**
   * Creates EPGM edges from foreign key respectively n:m relations
   *
   * @param flinkConfig Gradoop Flink configuration.
   * @param rdbmsConfig Relational database configuration.
   * @param metadataParser Metadata of connected relational database.
   * @param vertices Dataset of already created vertices.
   * @return Directed and undirected EPGM edges.
   * @throws Exception
   */

  public static DataSet<Edge> create(GradoopFlinkConfig flinkConfig, RdbmsConfig rdbmsConfig,
      MetaDataParser metadataParser, DataSet<Vertex> vertices) {
    List<TableToEdge> tablesToEdges = metadataParser.getTablesToEdges();

    DataSet<Edge> edges = null;
    EdgeFactory edgeFactory = flinkConfig.getEdgeFactory();
    ExecutionEnvironment env = flinkConfig.getExecutionEnvironment();

    /*
     * Foreign key relations to edges
     */
    if (!tablesToEdges.isEmpty()) {
      DataSet<TableToEdge> dsTablesToEdges = env.fromCollection(tablesToEdges);

      // Primary key table representation of foreign key relation
      DataSet<LabelIdKeyTuple> pkEdges = dsTablesToEdges.filter(new IsDirected())
          .flatMap(new VerticesToPkTable())
          .withBroadcastSet(vertices, RdbmsConstants.BROADCAST_VARIABLE).distinct("*");

      // Foreign key table representation of foreign key relation
      DataSet<LabelIdKeyTuple> fkEdges = dsTablesToEdges.filter(new IsDirected())
          .flatMap(new VerticesToFkTable())
          .withBroadcastSet(vertices, RdbmsConstants.BROADCAST_VARIABLE);

      edges = pkEdges.join(fkEdges).where(0, 2).equalTo(0, 2).map(new JoinSetToEdges(edgeFactory));

      /*
       * N:M relations to edges
       */
      int counter = 0;
      for (TableToEdge table : tablesToEdges) {
        if (!table.isDirectionIndicator()) {

          DataSet<Row> dsSQLResult = FlinkDatabaseInputHelper.getInput(env, rdbmsConfig,
              table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());

          // Represents the two foreign key attributes and belonging
          // properties
          DataSet<Fk1Fk2Props> fkPropsTable = dsSQLResult.map(new FKandProps(counter))
              .withBroadcastSet(env.fromCollection(tablesToEdges),
                  RdbmsConstants.BROADCAST_VARIABLE);

          // Represents vertices in relation with foreign key one
          DataSet<IdKeyTuple> idPkTableOne = vertices
              .filter(new ByLabel<Vertex>(table.getStartTableName())).map(new VertexToIdPkTuple());

          // Represents vertices in relation with foreign key two
          DataSet<IdKeyTuple> idPkTableTwo = vertices
              .filter(new ByLabel<Vertex>(table.getEndTableName())).map(new VertexToIdPkTuple());

          DataSet<Edge> dsTupleEdges = fkPropsTable.join(idPkTableOne).where(0).equalTo(1)
              .map(new Tuple2ToIdFkWithProps()).join(idPkTableTwo).where(1).equalTo(1)
              .map(new Tuple3ToEdge(edgeFactory, table.getRelationshipType()));

          if (edges == null) {
            edges = dsTupleEdges;
          } else {
            edges = edges.union(dsTupleEdges);
          }

          // Creates other direction edges
          edges = edges.union(dsTupleEdges.map(new EdgeToEdgeComplement(edgeFactory)));
        }
        counter++;
      }
    }
    return edges;
  }
}
