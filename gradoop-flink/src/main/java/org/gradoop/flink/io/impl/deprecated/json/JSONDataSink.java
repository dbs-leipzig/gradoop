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
package org.gradoop.flink.io.impl.deprecated.json;

import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.deprecated.json.functions.EdgeToJSON;
import org.gradoop.flink.io.impl.deprecated.json.functions.GraphHeadToJSON;
import org.gradoop.flink.io.impl.deprecated.json.functions.VertexToJSON;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Write an EPGM representation into three separate JSON files. The format
 * is documented at {@link GraphHeadToJSON}, {@link VertexToJSON} and
 * {@link EdgeToJSON}.
 *
 * @deprecated This class is deprecated. For example use
 * {@link org.gradoop.flink.io.impl.csv.CSVDataSink}
 */
@Deprecated
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
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
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
}
