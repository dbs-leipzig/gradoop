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

import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.json.functions.EdgeToJSON;
import org.gradoop.io.impl.json.functions.GraphHeadToJSON;
import org.gradoop.io.impl.json.functions.VertexToJSON;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Write an EPGM representation into three separate JSON files. The format
 * is documented at {@link GraphHeadToJSON}, {@link VertexToJSON} and
 * {@link EdgeToJSON}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class JSONDataSink
  <G extends GraphHead, V extends Vertex, E extends Edge>
  extends JSONBase<G, V, E>
  implements DataSink<G, V, E> {

  /**
   * Creates a new data sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param graphHeadPath graph data file
   * @param vertexPath    vertex data path
   * @param edgePath      edge data file
   * @param config        Gradoop Flink configuration
   */
  public JSONDataSink(String graphHeadPath, String vertexPath, String edgePath,
    GradoopFlinkConfig<G, V, E> config) {
    super(graphHeadPath, vertexPath, edgePath, config);
  }

  @Override
  public void write(LogicalGraph<G, V, E> logicalGraph) {
    write(GraphCollection.fromGraph(logicalGraph));
  }

  @Override
  public void write(GraphCollection<G, V, E> graphCollection) {
    graphCollection.getGraphHeads()
      .writeAsFormattedText(getGraphHeadPath(), new GraphHeadToJSON<G>());
    graphCollection.getVertices()
      .writeAsFormattedText(getVertexPath(), new VertexToJSON<V>());
    graphCollection.getEdges()
      .writeAsFormattedText(getEdgePath(), new EdgeToJSON<E>());
  }

  @Override
  public void write(GraphTransactions<G, V, E> graphTransactions) {
    write(GraphCollection.fromTransactions(graphTransactions));
  }
}
