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
package org.gradoop.temporal.io.impl.parquet.plain;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.impl.parquet.plain.ParquetDataSource;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.parquet.plain.read.TemporalEdgeRootConverter;
import org.gradoop.temporal.io.impl.parquet.plain.read.TemporalGraphHeadRootConverter;
import org.gradoop.temporal.io.impl.parquet.plain.read.TemporalVertexRootConverter;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.TemporalGraphCollectionFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;

/**
 * A graph data source for parquet files storing temporal graphs.
 * <p>
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 * <p>
 * parquetRoot
 * |- vertices.parquet # all vertex data
 * |- edges.parquet    # all edge data
 * |- graphs.parquet   # all graph head data
 */
public class TemporalParquetDataSource extends ParquetDataSource implements TemporalDataSource {

  /**
   * Creates a new temporal parquet data source.
   *
   * @param basePath path to the directory containing the CSV files
   * @param config   Gradoop Flink configuration
   */
  public TemporalParquetDataSource(String basePath, TemporalGradoopConfig config) {
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

    DataSet<TemporalGraphHead> graphHeads = read(
      TemporalGraphHead.class,
      getGraphHeadPath(),
      TemporalGraphHeadRootConverter.class);

    DataSet<TemporalVertex> vertices = read(
      TemporalVertex.class,
      getVertexPath(),
      TemporalVertexRootConverter.class);

    DataSet<TemporalEdge> edges = read(
      TemporalEdge.class,
      getEdgePath(),
      TemporalEdgeRootConverter.class);

    return collectionFactory.fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  protected TemporalGradoopConfig getConfig() {
    return (TemporalGradoopConfig) super.getConfig();
  }
}
