/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.parquet.protobuf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.EdgeToProtobufObject;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.GraphHeadToProtobufObject;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.VertexToProtobufObject;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * A graph data sink for parquet-protobuf files.
 */
public class ParquetProtobufDataSink extends ParquetProtobufBase implements DataSink {

  /**
   * Creates a new parquet-protobuf data sink.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public ParquetProtobufDataSink(String basePath, GradoopFlinkConfig config) {
    super(basePath, config);
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
    write(logicalGraph.getCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    DataSet<EPGMProto.GraphHead.Builder> graphHeads = graphCollection.getGraphHeads()
      .map(new GraphHeadToProtobufObject());

    DataSet<EPGMProto.Vertex.Builder> vertices = graphCollection.getVertices()
      .map(new VertexToProtobufObject());

    DataSet<EPGMProto.Edge.Builder> edges = graphCollection.getEdges()
      .map(new EdgeToProtobufObject());

    write(graphHeads, EPGMProto.GraphHead.class, getGraphHeadPath(), overwrite);
    write(vertices, EPGMProto.Vertex.class, getVertexPath(), overwrite);
    write(edges, EPGMProto.Edge.class, getEdgePath(), overwrite);
  }
}
