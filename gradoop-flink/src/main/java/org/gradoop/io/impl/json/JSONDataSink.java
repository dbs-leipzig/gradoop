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
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Write an EPGM representation into three separate JSON files. The format
 * is documented at {@link GraphHeadToJSON}, {@link VertexToJSON} and
 * {@link EdgeToJSON}.
 */
public class JSONDataSink extends JSONBase implements DataSink {

  /**
   * Creates a new data sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param graphHeadPath graph data file
   * @param vertexPath    vertex data path
   * @param edgePath      edge data file
   * @param config        Gradoop Flink configuration
   */
  public JSONDataSink(String graphHeadPath, String vertexPath, String edgePath,
    GradoopFlinkConfig config) {
    super(graphHeadPath, vertexPath, edgePath, config);
  }

  @Override
  public void write(LogicalGraph logicalGraph) {
    write(GraphCollection.fromGraph(logicalGraph));
  }

  @Override
  public void write(GraphCollection graphCollection) {
    graphCollection.getGraphHeads().writeAsFormattedText(getGraphHeadPath(),
      new GraphHeadToJSON<>());
    graphCollection.getVertices().writeAsFormattedText(getVertexPath(),
      new VertexToJSON<>());
    graphCollection.getEdges().writeAsFormattedText(getEdgePath(),
      new EdgeToJSON<>());
  }

  @Override
  public void write(GraphTransactions graphTransactions) {
    write(GraphCollection.fromTransactions(graphTransactions));
  }
}
