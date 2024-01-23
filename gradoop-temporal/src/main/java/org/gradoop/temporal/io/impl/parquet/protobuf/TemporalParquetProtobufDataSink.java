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
package org.gradoop.temporal.io.impl.parquet.protobuf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.impl.parquet.protobuf.ParquetProtobufDataSink;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.impl.parquet.protobuf.functions.TemporalEdgeToProtobufObject;
import org.gradoop.temporal.io.impl.parquet.protobuf.functions.TemporalGraphHeadToProtobufObject;
import org.gradoop.temporal.io.impl.parquet.protobuf.functions.TemporalVertexToProtobufObject;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;

import java.io.IOException;

/**
 * A temporal graph data sink for parquet-protobuf files.
 */
public class TemporalParquetProtobufDataSink extends ParquetProtobufDataSink implements TemporalDataSink {

  /**
   * Creates a new temporal parquet-protobuf data sink.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public TemporalParquetProtobufDataSink(String basePath, GradoopFlinkConfig config) {
    super(basePath, config);
  }

  @Override
  public void write(TemporalGraph temporalGraph) throws IOException {
    write(temporalGraph, false);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection) throws IOException {
    write(temporalGraphCollection, false);
  }

  @Override
  public void write(TemporalGraph temporalGraph, boolean overwrite) throws IOException {
    write(temporalGraph.getCollectionFactory().fromGraph(temporalGraph), overwrite);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection, boolean overwrite) throws IOException {
    DataSet<TPGMProto.TemporalGraphHead.Builder> graphHeads =
      temporalGraphCollection.getGraphHeads().map(new TemporalGraphHeadToProtobufObject());

    DataSet<TPGMProto.TemporalVertex.Builder> vertices =
      temporalGraphCollection.getVertices().map(new TemporalVertexToProtobufObject());

    DataSet<TPGMProto.TemporalEdge.Builder> edges =
      temporalGraphCollection.getEdges().map(new TemporalEdgeToProtobufObject());

    write(graphHeads, TPGMProto.TemporalGraphHead.class, getGraphHeadPath(), overwrite);
    write(vertices, TPGMProto.TemporalVertex.class, getVertexPath(), overwrite);
    write(edges, TPGMProto.TemporalEdge.class, getEdgePath(), overwrite);
  }
}
