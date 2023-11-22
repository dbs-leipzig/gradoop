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
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.ProtobufObjectToEdge;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.ProtobufObjectToGraphHead;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.ProtobufObjectToVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * A graph data source for parquet-protobuf files.
 * <p>
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 * <p>
 * protoParquetRoot
 * |- vertices.proto.parquet # all vertex data
 * |- edges.proto.parquet    # all edge data
 * |- graphs.proto.parquet   # all graph head data
 */
public class ParquetProtobufDataSource extends ParquetProtobufBase implements DataSource {

  /**
   * Creates a new parquet-protobuf data source.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public ParquetProtobufDataSource(String basePath, GradoopFlinkConfig config) {
    super(basePath, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    GraphCollection collection = getGraphCollection();
    return collection.getGraphFactory().fromDataSets(
      collection.getGraphHeads().first(1),
      collection.getVertices(),
      collection.getEdges());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    GraphCollectionFactory collectionFactory = getConfig().getGraphCollectionFactory();

    DataSet<EPGMGraphHead> graphHeads =
      read(EPGMProto.GraphHead.Builder.class, getGraphHeadPath())
        .map(new ProtobufObjectToGraphHead(collectionFactory.getGraphHeadFactory()));

    DataSet<EPGMVertex> vertices =
      read(EPGMProto.Vertex.Builder.class, getVertexPath())
        .map(new ProtobufObjectToVertex(collectionFactory.getVertexFactory()));

    DataSet<EPGMEdge> edges =
      read(EPGMProto.Edge.Builder.class, getEdgePath())
        .map(new ProtobufObjectToEdge(collectionFactory.getEdgeFactory()));

    return collectionFactory.fromDataSets(graphHeads, vertices, edges);
  }
}
