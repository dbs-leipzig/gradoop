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
import org.gradoop.flink.io.impl.parquet.protobuf.ParquetProtobufDataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.parquet.protobuf.functions.ProtobufObjectToTemporalEdge;
import org.gradoop.temporal.io.impl.parquet.protobuf.functions.ProtobufObjectToTemporalGraphHead;
import org.gradoop.temporal.io.impl.parquet.protobuf.functions.ProtobufObjectToTemporalVertex;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.TemporalGraphCollectionFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;

/**
 * A temporal graph data source for parquet-protobuf files.
 * <p>
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 * <p>
 * protoParquetRoot
 * |- vertices.proto.parquet # all vertex data
 * |- edges.proto.parquet    # all edge data
 * |- graphs.proto.parquet   # all graph head data
 */
public class TemporalParquetProtobufDataSource extends ParquetProtobufDataSource implements
  TemporalDataSource {

  /**
   * Creates a new temporal parquet-protobuf data source.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public TemporalParquetProtobufDataSource(String basePath, GradoopFlinkConfig config) {
    super(basePath, config);
  }

  @Override
  public TemporalGraph getTemporalGraph() throws IOException {
    TemporalGraphCollection collection = getTemporalGraphCollection();
    return getConfig().getTemporalGraphFactory().fromDataSets(
      collection.getGraphHeads().first(1),
      collection.getVertices(),
      collection.getEdges());
  }

  @Override
  public TemporalGraphCollection getTemporalGraphCollection() throws IOException {
    TemporalGraphCollectionFactory collectionFactory = getConfig().getTemporalGraphCollectionFactory();

    DataSet<TemporalGraphHead> graphHeads =
      read(TPGMProto.TemporalGraphHead.Builder.class, getGraphHeadPath())
        .map(new ProtobufObjectToTemporalGraphHead(collectionFactory.getGraphHeadFactory()));

    DataSet<TemporalVertex> vertices =
      read(TPGMProto.TemporalVertex.Builder.class, getVertexPath())
        .map(new ProtobufObjectToTemporalVertex(collectionFactory.getVertexFactory()));

    DataSet<TemporalEdge> edges =
      read(TPGMProto.TemporalEdge.Builder.class, getEdgePath())
        .map(new ProtobufObjectToTemporalEdge(collectionFactory.getEdgeFactory()));

    return collectionFactory.fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  protected TemporalGradoopConfig getConfig() {
    return (TemporalGradoopConfig) super.getConfig();
  }
}
