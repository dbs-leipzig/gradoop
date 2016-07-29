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

package org.gradoop.io.impl.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.json.functions.JSONToEdge;
import org.gradoop.io.impl.json.functions.JSONToGraphHead;
import org.gradoop.io.impl.json.functions.JSONToVertex;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.combination.ReduceCombination;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Creates an EPGM instance from JSON files. The exact format is documented in
 * {@link JSONToGraphHead}, {@link JSONToVertex}, {@link JSONToEdge}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class JSONDataSource
  <G extends GraphHead, V extends Vertex, E extends Edge>
  extends JSONBase<G, V, E>
  implements DataSource<G, V, E> {

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param graphHeadPath graph data file
   * @param vertexPath    vertex data file
   * @param edgePath      edge data file
   * @param config        Gradoop Flink configuration
   */
  public JSONDataSource(String graphHeadPath, String vertexPath,
    String edgePath, GradoopFlinkConfig<G, V, E> config) {
    super(graphHeadPath, vertexPath, edgePath, config);
  }

  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination<G, V, E>());
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // used for type hinting when loading vertex data
    TypeInformation<V> vertexTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<E> edgeTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getEdgeFactory().getType());
    // used for type hinting when loading graph data
    TypeInformation<G> graphTypeInfo = TypeExtractor
      .createTypeInfo(getConfig().getGraphHeadFactory().getType());

    // read vertex, edge and graph data
    DataSet<V> vertices = env.readTextFile(getVertexPath())
      .map(new JSONToVertex<>(getConfig().getVertexFactory()))
      .returns(vertexTypeInfo);
    DataSet<E> edges = env.readTextFile(getEdgePath())
      .map(new JSONToEdge<>(getConfig().getEdgeFactory()))
      .returns(edgeTypeInfo);
    DataSet<G> graphHeads;
    if (getGraphHeadPath() != null) {
      graphHeads = env.readTextFile(getGraphHeadPath())
        .map(new JSONToGraphHead<>(getConfig().getGraphHeadFactory()))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromElements(
        getConfig().getGraphHeadFactory().createGraphHead());
    }

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, getConfig());
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() {
    return getGraphCollection().toTransactions();
  }
}
