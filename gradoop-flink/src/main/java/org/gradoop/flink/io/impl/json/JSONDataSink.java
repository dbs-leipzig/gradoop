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

import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.functions.EdgeToJSON;
import org.gradoop.flink.io.impl.json.functions.VertexToJSON;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.impl.json.functions.GraphHeadToJSON;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;

/**
 * Write an EPGM representation into three separate JSON files. The format
 * is documented at {@link GraphHeadToJSON}, {@link VertexToJSON} and
 * {@link EdgeToJSON}.
 */
public class JSONDataSink extends JSONBase implements DataSink {

  /**
   * Creates a new data sink. The graph is written into the specified directory. Paths can be local
   * (file://) or HDFS (hdfs://).
   *
   * @param outputPath directory to write the graph to
   * @param config     Gradoop Flink configuration
   */
  public JSONDataSink(String outputPath, GradoopFlinkConfig config) {
    this(outputPath + DEFAULT_GRAPHS_FILE,
      outputPath + DEFAULT_VERTEX_FILE,
      outputPath + DEFAULT_EDGE_FILE,
      config);
  }

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
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(GraphTransactions graphTransactions) throws IOException {
    write(graphTransactions, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overWrite) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph), overWrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {

    FileSystem.WriteMode writeMode =
      overWrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;

    graphCollection.getGraphHeads().writeAsFormattedText(getGraphHeadPath(), writeMode,
      new GraphHeadToJSON<>());
    graphCollection.getVertices().writeAsFormattedText(getVertexPath(), writeMode,
      new VertexToJSON<>());
    graphCollection.getEdges().writeAsFormattedText(getEdgePath(), writeMode,
      new EdgeToJSON<>());
  }

  @Override
  public void write(GraphTransactions graphTransactions, boolean overWrite) throws IOException {
    write(GraphCollection.fromTransactions(graphTransactions), overWrite);
  }
}
