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
package org.gradoop.flink.io.impl.parquet.plain;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.parquet.plain.read.EdgeRootConverter;
import org.gradoop.flink.io.impl.parquet.plain.read.GraphHeadRootConverter;
import org.gradoop.flink.io.impl.parquet.plain.read.VertexRootConverter;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * A graph data source for parquet files.
 * <p>
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 * <p>
 * parquetRoot
 * |- vertices.parquet # all vertex data
 * |- edges.parquet    # all edge data
 * |- graphs.parquet   # all graph head data
 */
public class ParquetDataSource extends ParquetBase implements DataSource {

  /**
   * Creates a new parquet data source.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public ParquetDataSource(String basePath, GradoopFlinkConfig config) {
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

    DataSet<EPGMGraphHead> graphHeads = read(
      EPGMGraphHead.class,
      getGraphHeadPath(),
      GraphHeadRootConverter.class);
    DataSet<EPGMVertex> vertices = read(
      EPGMVertex.class,
      getVertexPath(),
      VertexRootConverter.class);
    DataSet<EPGMEdge> edges = read(
      EPGMEdge.class,
      getEdgePath(),
      EdgeRootConverter.class);

    return collectionFactory.fromDataSets(graphHeads, vertices, edges);
  }
}
