/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.functions.JSONToEdge;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.impl.json.functions.JSONToGraphHead;
import org.gradoop.flink.io.impl.json.functions.JSONToVertex;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Creates an EPGM instance from JSON files. The exact format is documented in
 * {@link JSONToGraphHead}, {@link JSONToVertex}, {@link JSONToEdge}.
 */
public class JSONDataSource extends JSONBase implements DataSource {

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param graphHeadPath graph data file
   * @param vertexPath    vertex data file
   * @param edgePath      edge data file
   * @param config        Gradoop Flink configuration
   */
  public JSONDataSource(String graphHeadPath, String vertexPath,
    String edgePath, GradoopFlinkConfig config) {
    super(graphHeadPath, vertexPath, edgePath, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // used for type hinting when loading vertex data
    TypeInformation vertexTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation edgeTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getEdgeFactory().getType());
    // used for type hinting when loading graph data
    TypeInformation graphTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getGraphHeadFactory().getType());

    // read vertex, edge and graph data
    DataSet<Vertex> vertices = env.readTextFile(getVertexPath())
      .map(new JSONToVertex(getConfig().getVertexFactory()))
      .returns(vertexTypeInfo);
    DataSet<Edge> edges = env.readTextFile(getEdgePath())
      .map(new JSONToEdge(getConfig().getEdgeFactory()))
      .returns(edgeTypeInfo);
    DataSet<GraphHead> graphHeads;
    if (getGraphHeadPath() != null) {
      graphHeads = env.readTextFile(getGraphHeadPath())
        .map(new JSONToGraphHead(getConfig().getGraphHeadFactory()))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromElements(
        getConfig().getGraphHeadFactory().createGraphHead());
    }

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, getConfig());
  }

  @Override
  public GraphTransactions getGraphTransactions() {
    return getGraphCollection().toTransactions();
  }
}
